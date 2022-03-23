# !/usr/bin/env python
import argparse
import sys
import venv
import tempfile
import json
from subprocess import run
from pathlib import Path
from typing import TypedDict, List

class LibType(TypedDict):
    name: str
    version: str

def main():
    parser = argparse.ArgumentParser(
        description="Update requirement versions in specified file."
    )
    parser.add_argument(
        "in_file",
        type=argparse.FileType("r", encoding="UTF-8"),
        help="The requirements file to read.",
    )
    args = parser.parse_args()

    print(
        "\n".join(
            f"{lib['name']}=={lib['version']}"
            for lib in freeze_req(args.in_file.read())
        )
    )


def freeze_req(requirements: str) -> List[LibType]:
    with tempfile.TemporaryDirectory() as td:
        builder = venv.EnvBuilder(with_pip=True)
        builder.create(td)

        rf_path = str(Path(td) / "reqs.txt")
        with open(rf_path, "w") as f:
            f.write(requirements)

        python = str(Path(td) / "bin" / "python")
        if not Path(python).exists():
            python = str(Path(td) / "Scripts" / "python.exe")

        run([python, "-m", "pip", "install", "-r", rf_path], stdout=sys.stderr.buffer)

        freeze_json = run(
            [python, "-m", "pip", "list", "--format", "json"],
            capture_output=True,
            text=True,
        )
        freeze = json.loads(freeze_json.stdout)
        return freeze
