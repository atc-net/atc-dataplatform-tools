# !/usr/bin/env python
import json
from subprocess import run
import argparse
import io
import re


def main():
    parser = argparse.ArgumentParser(description='Update requirement versions in specified file.')
    parser.add_argument('file', type=argparse.FileType('r+', encoding='UTF-8'), help='The requirements file to manipulate.')
    args = parser.parse_args()

    manipulate_file(args.file)

def manipulate_file(req_file:io.TextIOBase):
    # get all dependencies without their version information
    bare_dependencies = []
    for line in req_file:
        line = line.split("#")[0].strip()
        if not line: continue

        library = re.match(r"[\w\d-]+",line)
        if not library:
            raise Exception(f"Line {line} cannot be parsed.")
        library = library.group(0)
        bare_dependencies.append(library)

    run(["pip", "install", "--upgrade"] + bare_dependencies)
    versions = {}
    for item in json.loads(
        run(["pip", "list", "--format", "json"], capture_output=True).stdout
    ):
        versions[item["name"].lower()] = item["version"]

    req_file.seek(0)
    req_file.truncate()
    for lib in bare_dependencies:
        req_file.write(f"{lib}=={versions[lib.lower()]}\n")
