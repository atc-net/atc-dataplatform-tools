import argparse

from atc_tools.test_job.fetch import setup_fetch_parser
from atc_tools.test_job.submit import setup_submit_parser


def main():
    parser = argparse.ArgumentParser(
        description="Run Test Cases on databricks cluster."
    )

    subparsers = parser.add_subparsers(required=True, dest='command')

    setup_submit_parser(subparsers)
    setup_fetch_parser(subparsers)

    args = parser.parse_args()
    args.func(args)

