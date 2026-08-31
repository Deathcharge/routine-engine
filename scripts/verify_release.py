"""Verify both artifacts and run tests from an extracted sdist, outside the checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile
from pathlib import Path


def run(command: list[str], cwd: Path) -> str:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=180,
        )
    except subprocess.CalledProcessError as exc:
        print(exc.stdout, file=sys.stderr)
        print(exc.stderr, file=sys.stderr)
        raise
    return completed.stdout


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dist", type=Path, required=True, help="directory containing exactly one wheel and sdist"
    )
    args = parser.parse_args()
    wheels = list(args.dist.resolve().glob("*.whl"))
    sdists = list(args.dist.resolve().glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        parser.error("use a fresh artifact directory containing exactly one wheel and one sdist")
    wheel, sdist = wheels[0], sdists[0]
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        required = {
            "routine_engine/engine.py",
            "routine_engine/py.typed",
            "routine_engine/schemas/workflow-v1.schema.json",
        }
        if not required.issubset(names) or any(
            "helix_flow/" in name or "workflow_engine/" in name for name in names
        ):
            raise RuntimeError("wheel is missing the supported API/schema or includes archived source")

    with tempfile.TemporaryDirectory(prefix="routine-release-") as temporary:
        work = Path(temporary)
        unpacked = work / "source"
        unpacked.mkdir()
        with tarfile.open(sdist) as archive:
            for member in archive.getmembers():
                destination = (unpacked / member.name).resolve()
                if not destination.is_relative_to(unpacked.resolve()) or not (
                    member.isfile() or member.isdir()
                ):
                    raise RuntimeError("unsafe source archive member")
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    source = archive.extractfile(member)
                    if source is None:
                        raise RuntimeError("source archive file is missing")
                    with source, destination.open("wb") as target:
                        shutil.copyfileobj(source, target)
        roots = list(unpacked.iterdir())
        if len(roots) != 1:
            raise RuntimeError("source archive must have one top-level directory")
        source_root = roots[0]
        for relative in (
            "tests/conftest.py",
            "examples/release_actions.py",
            "schemas/workflow-v1.schema.json",
            "CHANGELOG.md",
        ):
            if not (source_root / relative).is_file():
                raise RuntimeError(f"source archive missing {relative}")
        if (source_root / "routine_engine").exists():
            raise RuntimeError("source archive includes unsupported legacy source")
        print(run([sys.executable, "-m", "pytest", "-q"], source_root), end="")

        environment = work / "wheel-env"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        print(run([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel)], work), end="")
        print(run([str(python), "-m", "routine_engine", "--version"], work), end="")
        schema = json.loads(run([str(python), "-m", "routine_engine", "schema"], work))
        if schema["properties"]["schema_version"]["const"] != 1:
            raise RuntimeError("installed workflow schema version mismatch")
        result = json.loads(run([str(python), "-m", "routine_engine", "demo", "--name", "Release"], work))
        if result["status"] != "success":
            raise RuntimeError("installed CLI demo failed")
        # Only copy the consumer fixture, not src/, into a separate working directory.
        consumer = work / "consumer"
        consumer.mkdir()
        shutil.copytree(source_root / "examples", consumer / "examples")
        for name in ("pyproject.toml", "README.md", "CHANGELOG.md", "LICENSE"):
            shutil.copyfile(source_root / name, consumer / name)
        result = json.loads(
            run(
                [
                    str(python),
                    "-m",
                    "routine_engine",
                    "run",
                    "examples/release-readiness.json",
                    "--plugin",
                    "examples.release_actions",
                    "--state",
                    "state.json",
                ],
                consumer,
            )
        )
        if (
            result["status"] != "success"
            or len(result["steps"]["evidence"]["output"]["readme"]["sha256"]) != 64
        ):
            raise RuntimeError("installed consumer journey failed")
    print(
        json.dumps(
            {artifact.name: hashlib.sha256(artifact.read_bytes()).hexdigest() for artifact in (wheel, sdist)},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
