import argparse

from spetlrtools.test_job.fetch import setup_fetch_parser
from spetlrtools.test_job.submit import setup_submit_parser


def main():
    """Cli main function. Not for use from python."""
    parser = argparse.ArgumentParser(
        description="Run Test Cases on databricks cluster."
    )

    subparsers = parser.add_subparsers(required=True, dest="command")

    setup_submit_parser(subparsers)
    setup_fetch_parser(subparsers)

    args = parser.parse_args()
    args.func(args)
