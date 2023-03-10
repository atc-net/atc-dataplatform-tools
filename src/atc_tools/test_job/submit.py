"""
- test the databricks api token
- zip the test archive
- walk the test folders to determine the parallelization
- constuct the test job json
    - tag the test job for easy retrieval
- submit the test job
- return the ID - also to file
"""
import argparse
import copy
import datetime
import inspect
import json
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import List, Union
from typing.io import IO

from . import test_main
from .dbcli import db_check, dbfscall, dbjcall
from .dbfs import DbfsLocation


def setup_submit_parser(subparsers):
    """
    Adds a subparser for the command 'submit'.
    :param subparsers: must be the object returned by ArgumentParser().add_subparsers()
    :return:
    """

    parser: argparse.ArgumentParser = subparsers.add_parser(
        "submit", description="Run Test Cases on databricks cluster."
    )
    parser.set_defaults(func=submit_main)

    parser.add_argument(
        "--wheels",
        type=str,
        required=False,
        help="The glob paths of all wheels under test.",
        default="dist/*.whl",
    )

    parser.add_argument(
        "--tests",
        type=str,
        required=True,
        help="Location of the tests folder. Will be sendt to databricks as a whole.",
    )

    task = parser.add_mutually_exclusive_group(required=True)
    task.add_argument(
        "--task",
        help="Single Test file or folder to execute.",
    )
    task.add_argument(
        "--tasks-from",
        help="path in test archive where each subfolder becomes a task.",
    )

    # cluster argument pair
    cluster = parser.add_mutually_exclusive_group(required=True)
    cluster.add_argument(
        "--cluster",
        type=str,
        help="JSON document describing the cluster setup.",
    )
    cluster.add_argument(
        "--cluster-file",
        type=argparse.FileType("r"),
        help="File with JSON document describing the cluster setup.",
    )

    # spark libraries argument pair
    sparklibs = parser.add_mutually_exclusive_group(required=False)
    sparklibs.add_argument(
        "--sparklibs",
        help="JSON document describing the spark dependencies.",
    )
    sparklibs.add_argument(
        "--sparklibs-file",
        type=argparse.FileType("r"),
        help="File with JSON document describing the spark dependencies.",
    )

    # python dependencies file
    pydep = parser.add_mutually_exclusive_group(required=False)
    pydep.add_argument(
        "--requirement",
        action="append",
        help="a python dependency, specified like for pip",
        default=[],
    )
    pydep.add_argument(
        "--requirements-file",
        type=argparse.FileType("r"),
        help="File with python dependencies, specified like for pip",
    )

    parser.add_argument(
        "--main-script",
        type=argparse.FileType("r"),
        help="Your own test_main.py script file, to add custom functionality.",
    )

    parser.add_argument(
        "--pytest-args", help="Additional arguments to pass to pytest in each test job."
    )

    parser.add_argument(
        "--out-json",
        type=argparse.FileType("w"),
        help="File to store the RunID for future queries.",
    )

    return


def collect_arguments(args):
    """
    Post process the parsed arguments of the 'submit' command argument parser.
    :param args: parsed arguments of the 'submit' command argument parser
    :return:
    """

    # pre-process 'cluster'
    if args.cluster_file:
        args.cluster = args.cluster_file.read()
    args.cluster = json.loads(args.cluster)

    # pre-process 'sparklibs'
    if args.sparklibs_file:
        args.sparklibs = args.sparklibs_file.read()
    if args.sparklibs:
        args.sparklibs = json.loads(args.sparklibs)

    # pre-process 'requirement'
    if args.requirements_file:
        args.requirement = [
            line.strip()
            for line in args.requirements_file.read().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    args.pytest_args = (args.pytest_args or "").split()
    if args.tasks_from:
        args.parallelize = True
        args.task = args.tasks_from
    else:
        args.parallelize = False

    return args


def submit_main(args):
    """the main function of the cli command 'submit'. Not to be used directly."""
    db_check()

    args = collect_arguments(args)

    submit(
        test_path=args.tests,
        task=args.task,
        parallelize=args.parallelize,
        cluster=args.cluster,
        wheels=args.wheels,
        requirement=args.requirement,
        sparklibs=args.sparklibs,
        out_json=args.out_json,
        main_script=args.main_script,
        pytest_args=args.pytest_args,
    )


class DbTestFolder:
    """Context manager that creates a unique test folder on dbfs."""

    def __init__(self):
        self._test_path_base = DbfsLocation(
            "/".join(
                [
                    "test",
                    datetime.datetime.now(datetime.timezone.utc).strftime(
                        "%Y%m%d-%H%M%S"
                    ),
                    uuid.uuid4().hex,
                ]
            )
        )

    def __enter__(self):
        print(f"Making dbfs test folder {self._test_path_base.remote}")
        dbfscall(f"mkdirs {self._test_path_base.remote}")
        return self._test_path_base

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def verify_and_resolve_task(test_path: str, task: Union[str, Path]):
    test_archive = Path(test_path).resolve().absolute()

    task_path = Path(task)
    if not task_path.is_absolute():
        task_path = (test_archive.parent / task_path).resolve().absolute()

    task_path = task_path.resolve().absolute()

    if not (test_archive == task_path or test_archive in task_path.parents):
        raise AssertionError(f"task {task} is not contained in {test_path}")

    if not task_path.exists():
        raise AssertionError(f"task {task_path} does not exist")

    if not (test_archive.parent / task).exists():
        raise AssertionError(f"The task {task} was not found in the test location.")

    return task_path.relative_to(test_archive.parent).as_posix()


def discover_job_tasks(test_path: str, folder: str):
    """If folder is given, create parallel tasks from this level.
    Otherwise, simply return the top folder as the single task.
    Returns a list of strings with the subfolders to process.
    """

    test_archive_parent = Path(test_path).resolve().absolute().parent

    subfolders = [
        verify_and_resolve_task(test_path, x)
        for x in (test_archive_parent / folder).iterdir()
        if x.is_dir()
    ]
    return subfolders


def submit(
    test_path: str,
    task: str,
    cluster: dict,
    wheels: str,
    parallelize: bool,
    requirement: List[str] = None,
    sparklibs: List[dict] = None,
    out_json: IO[str] = None,
    main_script: IO[str] = None,
    pytest_args: List[str] = None,
):
    if requirement is None:
        requirement = []
    if sparklibs is None:
        sparklibs = []
    if pytest_args is None:
        pytest_args = []

    # check the structure of the cluster object
    if not isinstance(cluster, dict):
        raise AssertionError("invalid cluster specification")

    # check the structure of the sparklibs object
    if not isinstance(sparklibs, list):
        raise AssertionError("invalid sparklibs specification")

    for py_requirement in requirement:
        sparklibs.append({"pypi": {"package": py_requirement}})

    with DbTestFolder() as test_folder:
        for wheel in discover_and_push_wheels(wheels, test_folder):
            sparklibs.append({"whl": wheel})

        archive_local = archive_and_push(test_path, test_folder)
        main_file = push_main_file(test_folder, main_script)

        print(f"copied everything to {test_folder.remote}")

        # construct the workflow object
        workflow = dict(run_name="Testing Run", format="MULTI_TASK", tasks=[])

        if parallelize:
            # subtasks will be ['tests/cluster/job1', 'tests/cluster/job2'] or similar
            tasks = discover_job_tasks(test_path, task)
        else:
            tasks = [verify_and_resolve_task(test_path, task)]

        for task in tasks:
            task_sub = task.replace("/", "_")
            task_cluster = copy.deepcopy(cluster)
            task_cluster["cluster_log_conf"] = {
                "dbfs": {"destination": f"{test_folder.remote}/{task_sub}"}
            }

            workflow["tasks"].append(
                dict(
                    task_key=task_sub,
                    libraries=sparklibs,
                    spark_python_task=dict(
                        python_file=main_file.remote,
                        parameters=[
                            # running in the spark python interpreter, the python __file__ variable does not
                            # work. Hence, we need to tell the script where the test area is.
                            f"--basedir={test_folder.local}/{task_sub}",
                            # all tests will be unpacked from here
                            f"--archive={archive_local}",
                            # we can actually run any part of our test suite, but some files need the full repo.
                            # Only run tests from this folder.
                            f"--folder={task}",
                            # additional arguments to pass to pytest
                            f"--pytestargs={json.dumps(pytest_args)}",
                        ],
                    ),
                    new_cluster=task_cluster,
                )
            )
    with tempfile.TemporaryDirectory() as tmp:
        jobfile = f"{tmp}/job.json"
        with open(jobfile, "w") as f:
            json.dump(workflow, f)
        res = dbjcall(f"runs submit --json-file {jobfile}")
        try:
            run_id = res["run_id"]
        except KeyError:
            print(res)
            raise

    # now we have the run_id
    print(f"Started run with ID {run_id}")
    details = dbjcall(f"runs get --run-id {run_id}")
    print(f"Follow job details at {details['run_page_url']}")

    if out_json:
        json.dump({"run_id": run_id}, out_json)


def discover_and_push_wheels(globpath: str, test_folder: DbfsLocation) -> List[str]:
    result = []
    for item in Path().glob(globpath):
        remote_path = f"{test_folder.remote}/{item.parts[-1]}"
        print(f"pushing {item} to test folder")
        dbfscall(f"cp {item} {remote_path}")
        result.append(remote_path)

    return result


def archive_and_push(test_path: str, test_folder: DbfsLocation):
    with tempfile.TemporaryDirectory() as tmp:
        test_path = Path(test_path)
        print(f"now archiving {test_path}")
        archive_path = shutil.make_archive(
            str(Path(tmp) / "tests"),
            "zip",
            test_path / "..",
            base_dir=test_path.parts[-1],
        )
        print("now pushing test archive to test folder")

        dbfscall(f"cp {archive_path} {test_folder.remote}/tests.zip")

    return f"{test_folder.local}/tests.zip"


def push_main_file(
    test_folder: DbfsLocation, main_script: IO[str] = None
) -> DbfsLocation:
    print("now pushing test main file")
    main_file = test_folder / "main.py"
    with tempfile.TemporaryDirectory() as tmp:
        with open(Path(tmp) / "main.py", "w") as f:
            if main_script:
                f.write(main_script.read())
            else:
                print("Using default main script test_main.py")
                f.write(inspect.getsource(test_main))

        dbfscall(f"cp {tmp}/main.py {main_file.remote}")

    return main_file
