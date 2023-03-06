import argparse
import os
import shutil
import sys
from hashlib import sha1

import pytest
from atc.spark import Spark

# print a marker message to screen to mark the start of python output
# make it invisible using only "\u200E\u200F\u2060" https://invisible-characters.com/
marker = "".join(list("\u200E\u200F\u2060")[i % 3] for i in sha1(b"my marker").digest())

def test_main():
    parser = argparse.ArgumentParser(description="Run Test Cases.")

    # location to use for current run. Usually the cluster logs base folder
    parser.add_argument("--basedir")
    # relative path of test folder in test archive
    parser.add_argument("--folder")
    # archive of test files to use
    parser.add_argument("--archive")

    args = parser.parse_args()

    # move to basedir so that simple imports from one test to another work
    os.chdir(args.basedir)
    sys.path = [os.getcwd()] + sys.path

    # unzip test archive to base folder
    shutil.unpack_archive(args.archive)

    # Ensure Spark is initialized before any tests are run
    Spark.get()

    print()
    print(marker)
    print()

    retcode = pytest.main(["-x", args.folder])
    if retcode.value:
        raise Exception("Pytest failed")

if __name__ == "__main__":
    test_main()
