import os
import shutil
import subprocess
import sys
from pathlib import Path

base_dir = Path(__file__).parent.parent
providers_dir = base_dir / "temporalio" / "streams" / "providers"
contract_path = providers_dir / "temporal_streams.nexusrpc.yaml"
output_dir = providers_dir / "_nexus_generated"
# Pinned to the version CI installs, because nexgen's output changes between
# releases and check-protos compares against what is committed here.
NEX_GEN_VERSION = "0.2.4"


def nex_gen_command() -> list[str]:
    if bin_path := os.environ.get("NEX_GEN_BIN"):
        return [bin_path]

    if shutil.which("nexgen") is None:
        subprocess.check_call(
            [
                "cargo",
                "install",
                "--locked",
                "nexgen",
                "--version",
                NEX_GEN_VERSION,
                # Same build as the system API script installs, so one binary
                # serves both and neither can overwrite the other's output.
                "--features",
                "advanced",
                "--force",
            ]
        )
    return ["nexgen"]


def check_version(command: list[str]) -> None:
    # A different release on PATH would regenerate different code locally
    # and the drift would only show in CI, so refuse before writing anything.
    reported = subprocess.check_output([*command, "--version"], text=True).strip()
    found = reported.split()[-1] if reported else ""
    if found != NEX_GEN_VERSION:
        raise SystemExit(
            f"found nexgen {found or '?'} at {command[0]}, but the stream contract is "
            f"generated with {NEX_GEN_VERSION}. Install it with `cargo install --locked "
            f"nexgen --version {NEX_GEN_VERSION} --features advanced` or point "
            "NEX_GEN_BIN at that binary."
        )


def generate_streams_nexus_api() -> None:
    if not contract_path.exists():
        raise RuntimeError(f"missing stream contract: {contract_path}")

    command = nex_gen_command()
    check_version(command)
    shutil.rmtree(output_dir, ignore_errors=True)
    subprocess.check_call(
        [
            *command,
            "python",
            str(contract_path),
            "--output",
            str(output_dir),
        ]
    )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--select",
            "I",
            "--fix",
            str(output_dir),
        ]
    )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            str(output_dir),
        ]
    )


if __name__ == "__main__":
    print("Generating stream endpoint Nexus API...", file=sys.stderr)
    generate_streams_nexus_api()
    print("Done", file=sys.stderr)
