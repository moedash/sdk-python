"""Generate the stream service's Python bindings from its Nexus contract.

The contract is ``temporalio/contrib/streams/nexus/temporal_streams.nexusrpc.yaml``
and the bindings land in ``temporalio/contrib/streams/nexus/_generated``. With
``--check`` the script generates into a scratch directory and fails when the
result differs from what is checked in, which is how CI keeps the two in step.

The contract uses the ``x-nexus-long-poll``, ``x-nexus-cursor``,
``x-nexus-stream-ref`` and ``x-nexus-handle`` annotations, which no nexgen
release carries yet, so the generator is a pinned revision of the nexgen fork
until they merge upstream. It is installed under its own root, so the
``nexgen`` release that ``gen_nexus_system_api.py`` uses is left alone.
``NEX_GEN_STREAMS_BIN`` points the script at a binary built some other way.
"""

import argparse
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

base_dir = Path(__file__).parent.parent
package_dir = base_dir / "temporalio" / "contrib" / "streams" / "nexus"
contract_path = package_dir / "temporal_streams.nexusrpc.yaml"
output_dir = package_dir / "_generated"

NEX_GEN_REPOSITORY = "https://github.com/moetemp/nexgen"
NEX_GEN_REVISION = "e50fd263044f7dc1066818f9c1068c08617fb39e"
nex_gen_root = base_dir / ".nexgen-streams" / NEX_GEN_REVISION


def nex_gen_command() -> list[str]:
    if bin_path := os.environ.get("NEX_GEN_STREAMS_BIN"):
        return [bin_path]
    binary = nex_gen_root / "bin" / "nexgen"
    if not binary.exists():
        subprocess.check_call(
            [
                "cargo",
                "install",
                "--locked",
                "nexgen",
                "--git",
                NEX_GEN_REPOSITORY,
                "--rev",
                NEX_GEN_REVISION,
                "--features",
                "advanced",
                "--root",
                str(nex_gen_root),
            ],
            # The git CLI honours the user's URL rewrites and credentials,
            # which cargo's built-in client does not.
            env={**os.environ, "CARGO_NET_GIT_FETCH_WITH_CLI": "true"},
        )
    return [str(binary)]


def generate(target: Path) -> None:
    """Generate the bindings into ``target``, formatted as the repository formats."""
    subprocess.check_call(
        [
            *nex_gen_command(),
            "python",
            "--client",
            "--output",
            str(target),
            str(contract_path),
        ]
    )
    for fix in (["check", "--select", "I", "--fix"], ["format"]):
        subprocess.check_call(
            [sys.executable, "-m", "ruff", *fix, "--quiet", str(target)]
        )


def differences(left: Path, right: Path) -> list[str]:
    """The relative paths whose content differs between two trees."""
    comparison = filecmp.dircmp(left, right, ignore=["__pycache__"])
    found = [
        *comparison.left_only,
        *comparison.right_only,
        *comparison.diff_files,
        *comparison.funny_files,
    ]
    for name, sub in comparison.subdirs.items():
        found.extend(
            f"{name}/{path}" for path in differences(Path(sub.left), Path(sub.right))
        )
    _, mismatch, errors = filecmp.cmpfiles(
        left, right, comparison.same_files, shallow=False
    )
    return sorted({*found, *mismatch, *errors})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail when regenerating would change the checked-in bindings",
    )
    args = parser.parse_args()
    if not contract_path.exists():
        raise RuntimeError(f"missing contract: {contract_path}")

    with tempfile.TemporaryDirectory(dir=base_dir) as temp_dir:
        scratch = Path(temp_dir) / "_generated"
        generate(scratch)
        if args.check:
            changed = differences(scratch, output_dir) if output_dir.exists() else ["."]
            if changed:
                print(
                    "The stream service bindings are stale; run "
                    "`uv run scripts/gen_nexus_streams_api.py`. Changed: "
                    + ", ".join(changed),
                    file=sys.stderr,
                )
                sys.exit(1)
            return
        shutil.rmtree(output_dir, ignore_errors=True)
        shutil.copytree(scratch, output_dir)


if __name__ == "__main__":
    main()
