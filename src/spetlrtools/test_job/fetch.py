"""
- find the test job (by ID or discover by tag)
- get job status:
- tasks: pending: 0 running: 0 success: 0 failed: 0
- any time a task finishes, print the log
- add a fail_fast so that any time a task fails, job is cancelled
"""
import argparse
import json
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import IO, List, Optional

from spetlrtools.test_job.dbcli import db_check
from spetlrtools.test_job.RunDetails import RunDetails


def setup_fetch_parser(subparsers):
    """
    Adds a subparser for the command 'fetch'.
    :param subparsers: must be the object returned by ArgumentParser().add_subparsers()
    :return:
    """
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "fetch", description="Return test run result."
    )
    parser.set_defaults(func=fetch_main)

    # cluster argument pair
    runid_config = parser.add_mutually_exclusive_group(required=True)
    runid_config.add_argument(
        "--runid",
        type=int,
        help="Run ID of the test job",
        default=None,
    )
    runid_config.add_argument(
        "--runid-json",
        type=argparse.FileType("r"),
        help="File with JSON document describing the Run ID of the test job.",
        default=None,
    )

    parser.add_argument(
        "--stdout",
        type=argparse.FileType("w"),
        required=False,
        help="Output test stdout to this file.",
        default=None,
    )

    parser.add_argument(
        "--failfast",
        action="store_true",
        help="Stop and cancel job on first failed task.",
    )


def collect_args(args):
    """Post process the arguments of the ."""
    if args.runid is None:
        args.runid = json.load(args.runid_json)["run_id"]

    return args


def fetch_main(args):
    """
    Main function of the 'fetch' command. Only to be used via the cli.
    :param args: the parsed arguments from the fetch subparser
    :return:
    """
    db_check()

    # Post process the arguments
    if args.runid is None:
        args.runid = json.load(args.runid_json)["run_id"]

    if fetch(args.runid, args.stdout, args.failfast):
        print("Run failed")
        sys.exit(-1)


@dataclass
class TaskState:
    """The result state of any workflow or task"""

    task_key: str
    life_cycle_state: str
    result_state: str
    end_time: int

    @property
    def ended(self):
        """Has the workflow or task ended?"""
        return self.end_time != 0

    @property
    def result(self):
        """String representing the state of the workflow or task."""
        if not self.ended:
            return self.life_cycle_state
        else:
            return self.result_state

    @property
    def success(self):
        """Has the workflow or task succeeded?"""
        return self.result_state.upper() == "SUCCESS"

    @classmethod
    def fromJson(cls, jobj):
        """Create the result state of the workflow or task from the json object
        returned by the databricks api."""
        state = jobj["state"]
        return cls(
            task_key=jobj["task_key"] if "task_key" in jobj else jobj["run_name"],
            life_cycle_state=state["life_cycle_state"],
            result_state=state["result_state"] if "result_state" in state else "",
            end_time=jobj["end_time"] if "end_time" in jobj else 0,
        )


@dataclass
class MultiTaskState:
    """Result state of a multiTask workflow"""

    overall: Optional[TaskState]
    tasks: List[TaskState]

    @classmethod
    def fromJson(cls, jobj):
        """Create the Result state of a multiTask workflow from the json object returned
        by the databricks api."""

        return cls(
            overall=TaskState.fromJson(jobj),
            tasks=[TaskState.fromJson(task) for task in jobj["tasks"]],
        )

    def accumulate(self):
        """return a dictionary of {'STATE STRING':counts} where counts represents the
        number of sub-tasks that share the same run state."""
        counts = defaultdict(int)
        for task in self.tasks:
            counts[task.result] += 1
        return counts

    def print_status(self):
        """Print the overall result state represented by this object."""
        print(
            "Overall state:",
            self.overall.result,
            "| Task states:",
            " | ".join(f"{k}: {v}" for k, v in self.accumulate().items()),
        )


def fetch(run_id: int, stdout_file: IO[str] = None, failfast=False):
    """Fetch main function.
    See the cli help for parameter descriptions and functionality.
    Can be used programmatically."""
    run = RunDetails(run_id)
    last_state = None
    stdouts = {}
    while True:
        state = MultiTaskState.fromJson(run.details)
        if last_state is None or state != last_state:
            last_state = state
            state.print_status()

        if failfast:
            if any(task.ended and not task.success for task in state.tasks):
                break

        for task in state.tasks:
            if task.ended and task.task_key not in stdouts:
                out = run.get_stdout(task.task_key)
                if stdout_file is None:
                    print(out)
                stdouts[task.task_key] = out

        if state.overall.ended:
            break
        time.sleep(5)
        run.refresh()

    if stdout_file is not None:
        for k, v in stdouts.items():
            stdout_file.write("=" * 50 + f"\nTask Output from {k}\n" + "=" * 50 + "\n")
            stdout_file.write(v)

    if last_state.overall.success:
        print("Run result SUCCESS!")
        return 0
    else:
        if not (last_state.overall.ended):
            print("A task failed. Cancelling test run.")
            run.cancel()
        return 1
