"""
This is the default main file that is pushed to databricks to launch the test task.
Its tasks are
- to unpack the test archive,
- print a sequence of marker characters to identify the start
  of python executing in the output
- run the tests using pytest
This file is not intended to be used directly.
"""
import argparse
import json
import os
import shutil
import sys
from hashlib import sha1

import pytest

# print a marker message to screen to mark the start of python output
# make it invisible using only "\u200E\u200F\u2060" https://invisible-characters.com/
marker = "".join(list("\u200E\u200F\u2060")[i % 3] for i in sha1(b"my marker").digest())


def test_main():
    """Main function to be called inside the test job task. Do not use directly."""
    parser = argparse.ArgumentParser(description="Run Test Cases.")

    # location to use for current run. Usually the cluster logs base folder
    parser.add_argument("--basedir")

    # relative path of test folder in test archive
    parser.add_argument("--folder")

    # archive of test files to use
    parser.add_argument("--archive")

    # archive of test files to use
    parser.add_argument("--pytestargs")

    args = parser.parse_args()

    extra_args = json.loads(args.pytestargs)

    # move to basedir so that simple imports from one test to another work
    os.chdir(args.basedir)
    sys.path = [os.getcwd()] + sys.path

    # unzip test archive to base folder
    shutil.unpack_archive(args.archive)

    # Ensure Spark is initialized before any tests are run
    # the import statement is inside the function so that the outer file
    # can be imported even where pyspark may not be available
    from atc.spark import Spark

    Spark.get()

    print()
    print(marker)
    print()

    retcode = pytest.main(["-x", args.folder, *extra_args])
    if retcode.value:
        raise Exception("Pytest failed")


if __name__ == "__main__":
    test_main()
