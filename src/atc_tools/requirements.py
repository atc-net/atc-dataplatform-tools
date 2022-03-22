# !/usr/bin/env python
import argparse
import sys
import venv
import tempfile
from subprocess import run
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Update requirement versions in specified file.')
    parser.add_argument('in_file', type=argparse.FileType('r', encoding='UTF-8'), help='The requirements file to read.')
    args = parser.parse_args()

    print(freeze_req(args.in_file.read()))

def freeze_req(requirements:str)->str:

    with tempfile.TemporaryDirectory() as td:
        builder = venv.EnvBuilder(with_pip=True)
        builder.create(td)

        rf_path = str(Path(td)/"reqs.txt")
        with open(rf_path,'w') as f:
            f.write(requirements)

        python = str(Path(td)/"bin"/"python")
        if not Path(python).exists():
            python = str(Path(td) / "Scripts" / "python.exe")

        run([python, "-m", "pip", "install", "-r", rf_path], stdout=sys.stderr.buffer)

        freeze = run([python, "-m", "pip", "freeze"], capture_output=True, text=True)
        return freeze.stdout
