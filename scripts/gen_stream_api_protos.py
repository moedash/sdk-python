"""Regenerate the stream additions to the vendored public API protos.

The stream protos live on a branch of the api repo that the Core submodule
does not pin yet. Regenerating everything from that branch would also pull in
whatever else moved on the api main line since the pin, so this stages the
API protos the submodule pins, applies the branch's own diff on top, and
regenerates only the files that diff touches. Every other module under
``temporalio/api`` stays byte-identical.

    uv run --python 3.10 --no-project --with "grpcio-tools==1.48.2" \\
        --with "mypy-protobuf==3.3.0" --with "protobuf<4" \\
        scripts/gen_stream_api_protos.py /path/to/api origin/main..origin/<branch>

The toolchain pins match ``scripts/_proto/Dockerfile``. Generated code refuses
to load on a protobuf runtime older than the one it was built against, and
these modules end up in applications that pin protobuf themselves. Run
``uv run poe format`` afterwards, as the ``gen-protos`` task does.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(BASE / "scripts"))

import gen_protos  # noqa: E402

API_OUT = BASE / "temporalio" / "api"


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: gen_stream_api_protos.py /path/to/api-checkout <base>..<ref>")
    api = Path(sys.argv[1]).resolve()
    revisions = sys.argv[2]
    diff = subprocess.run(
        ["git", "-C", str(api), "diff", revisions, "--", "temporal/"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    touched = subprocess.run(
        ["git", "-C", str(api), "diff", "--name-only", revisions, "--", "temporal/"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    if not touched:
        sys.exit(f"{revisions} touches no proto under temporal/")

    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp) / "stage"
        shutil.copytree(gen_protos.api_proto_dir / "temporal", stage / "temporal")
        subprocess.run(
            ["patch", "-p1", "--silent"],
            check=True,
            cwd=stage,
            input=diff,
            text=True,
        )
        out = Path(tmp) / "out"
        out.mkdir()
        subprocess.check_call(
            [
                sys.executable,
                "-mgrpc_tools.protoc",
                f"--proto_path={stage}",
                f"--python_out={out}",
                f"--mypy_out={out}",
                *touched,
            ]
        )
        packages: set[Path] = set()
        for proto in touched:
            relative = Path(proto).relative_to("temporal/api").with_suffix("")
            for suffix in ("_pb2.py", "_pb2.pyi"):
                generated = (
                    out
                    / "temporal"
                    / "api"
                    / relative.with_name(relative.name + suffix)
                )
                target = API_OUT / relative.with_name(relative.name + suffix)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(generated, target)
                print(f"wrote {target.relative_to(BASE)}")
            packages.add(target.parent)

    for package in sorted(packages):
        (package.parent / "__init__.py").touch()
        gen_protos.fix_generated_output(package)
        print(f"rewrote {(package / '__init__.py').relative_to(BASE)}")


if __name__ == "__main__":
    main()
