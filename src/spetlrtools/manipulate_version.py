import argparse
import configparser
import sys
from typing import IO

import requests
from packaging.version import Version, parse


def main():
    """Main CLI function, not to be used directly."""
    parser = argparse.ArgumentParser(
        description="Automatically set the version for upload to pypi"
    )
    parser.add_argument(
        "-t",
        dest="prerelease",
        action="store_true",
        help="prepare pre-release version for test.pypi",
    )
    parser.add_argument(
        "--name", help="Package name, if different from name in setup.cfg"
    )
    parser.add_argument(
        "--version-file",
        default="src/VERSION.txt",
        help="location of version to manipulate",
    )

    args = parser.parse_args()
    name = args.name
    if not name:
        config = configparser.ConfigParser()
        config.read("setup.cfg")
        name = config["metadata"]["name"]

    try:
        with open(args.version_file, "r+", encoding="utf-8") as v_file:
            mainpulate_version(
                package_name=name, version_file=v_file, pre_release=args.prerelease
            )

    except FileNotFoundError:
        print(f"Version file {args.version_file} not found.", file=sys.stderr)
        parser.print_usage()
        exit(-1)


def mainpulate_version(package_name: str, version_file: IO, pre_release: bool):
    """Query the indices, manipuate the version file and print the vesions to screen."""

    # get all relevant versions for manipulation
    test_v = get_remote_version("test.pypi.org", package_name)
    pypi_v = get_remote_version("pypi.org", package_name)
    local_v = get_local_version(version_file)

    next_version = _decide_next_version(pypi_v, test_v, local_v, pre_release)

    version_file.seek(0)
    version_file.truncate()
    version_file.write(str(next_version))
    print(str(next_version))


class InvalidVersionManipulation(Exception):
    pass


def _decide_next_version(
    pypi_v: Version, test_v: Version, local_v: Version, pre_release: bool
):
    """Returns the next version to be published in the local flow.
    See the documentation for how versions are manipulated.
    """

    # simple incrementation yields the naive next release
    next_incremental_release = parse(f"{pypi_v.major}.{pypi_v.minor}.{pypi_v.micro+1}")

    # if local_v is higher than a simple increment, that is what we use
    next_release = max(next_incremental_release, local_v)

    if not pre_release:
        return next_release

    # in case of pre-release, we need more work.
    if parse(test_v.base_version) < next_release:
        # the last test.pypi version relates to a previous release candidate
        # start a new serios of pre-releases
        pre_n = 1
    elif test_v.release == next_release.release and test_v < next_release:
        # we have previously released a pre-release version to test_pypi.
        # just use the next one
        if test_v.pre is not None:
            abrc, n = test_v.pre
            if abrc == "rc":
                # in case of a(lpha) or b(eta) release, still count the rc from 1
                pre_n = n + 1
    else:
        raise InvalidVersionManipulation(
            f"Test Pypi has {test_v}, "
            f"and next final release is {next_release}. "
            f"Releasing a release candidate for {next_release} "
            "would not make that release caindiate the highest on test.pypi. "
            "Please increment you package version to above the test.pypi version "
            "and try again."
        )

    return parse(f"{next_release.base_version}rc{pre_n}")


def get_remote_version(server: str, package: str):
    """Query the repository and return the Version (PEP-0440)
    that is the highest of any kind, not just final versions.
    In case of any issues, return Version("0"), which is lower
    than any other version and therefores usitable for the rest of the logic."""
    try:
        return max(
            parse(k)
            for k in requests.get(f"https://{server}/pypi/{package}/json")
            .json()["releases"]
            .keys()
        )
        # return parse(remote["info"]["version"])
    except:  # noqa: E722  # bare except is fine in this simple case
        return parse("0")


def get_local_version(v_file: IO):
    """read the version from the file. Only keep the base part of the version string."""
    v_file.seek(0)
    v = parse(v_file.read())
    v = parse(v.base_version)  # remove any suffices
    return v
