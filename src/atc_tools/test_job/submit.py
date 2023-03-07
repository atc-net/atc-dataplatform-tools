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
import subprocess
import sys
import tempfile
import uuid

from pathlib import Path
import shutil
from typing import List

from typing.io import IO

from . import test_main
from .dbcli import dbjcall, dbfscall, dbcall, db_check
from .dbfs import DbfsLocation


def setup_submit_parser(subparsers):
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "submit",
        description="Run Test Cases on databricks cluster."
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

    parser.add_argument(
        "--folder",
        type=str,
        required=False,
        default="",
        help="relative path of test folder in test archive to use in task discovery.",
    )

    parser.add_argument(
        "--out-json",
        type=argparse.FileType("w"),
        required=False,
        help="File to store the RunID for future queries.",
        default=None,
    )

    # cluster argument pair
    cluster = parser.add_mutually_exclusive_group(required=True)
    cluster.add_argument(
        "--cluster",
        type=str,
        help="JSON document describing the cluster setup.",
        default=None,
    )
    cluster.add_argument(
        "--cluster-file",
        type=argparse.FileType("r"),
        help="File with JSON document describing the cluster setup.",
        default=None,
    )

    # spark libraries argument pair
    sparklibs = parser.add_mutually_exclusive_group(required=False)
    sparklibs.add_argument(
        "--sparklibs",
        type=str,
        help="JSON document describing the spark dependencies.",
        default=None,
    )
    sparklibs.add_argument(
        "--sparklibs-file",
        type=argparse.FileType("r"),
        help="File with JSON document describing the spark dependencies.",
        default=None,
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
        default=None,
    )

    parser.add_argument(
        "--main-script",
        type=argparse.FileType('r'),
        help="Your own test_main.py script file, to add custom functionality.",
        default=None
    )

    return

def collect_arguments(args):
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
            l.strip()
            for l in args.requirements_file.read().splitlines()
            if l.strip() and not l.strip().startswith("#")
        ]


    return args


def submit_main(args):
    db_check()

    args = collect_arguments(args)

    submit(
        test_path=args.tests,
        folder=args.folder,
        cluster=args.cluster,
        wheels=args.wheels,
        requirement=args.requirement,
        sparklibs=args.sparklibs,
        out_json=args.out_json,
        main_script=args.main_script
    )


class DbTestFolder:
    def __init__(self):
        test_folder = "/".join(
            [
                "test",
                datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S"),
                uuid.uuid4().hex,
            ]
        )
        self._test_path_base = DbfsLocation(
            remote=f"dbfs:/{test_folder}", local=f"/dbfs/{test_folder}"
        )

    def __enter__(self):
        print(f"Making dbfs test folder {self._test_path_base.remote}")
        dbfscall(f"mkdirs {self._test_path_base.remote}")
        return self._test_path_base

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def discover_job_tasks(test_path: str, folder: str):

    subfolders = [
        x.as_posix() for x in (Path(test_path) / folder).iterdir() if x.is_dir()
    ]
    return subfolders


def discover_and_push_wheels(globpath: str, test_folder: DbfsLocation) -> List[str]:
    result = []
    for item in Path().glob(globpath):
        remote_path =f'{test_folder.remote}/{item.parts[-1]}'
        print(f'pushing {item} to to test folder')
        dbfscall(f"cp {item} {remote_path}")
        result.append(remote_path)

    return result

def archive_and_push(test_path:str, test_folder: DbfsLocation):
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

def push_main_file(test_folder: DbfsLocation, main_script:IO[str]=None)-> DbfsLocation:
    print('now pushing test main file')
    main_file = test_folder / 'main.py'
    with tempfile.TemporaryDirectory() as tmp:
        with open(Path(tmp) / "main.py", "w") as f:
            if main_script:
                f.write(main_script.read())
            else:
                f.write(inspect.getsource(test_main))

        dbfscall(f"cp {tmp}/main.py {main_file.remote}")

    return main_file

def submit(
    test_path: str,
    folder: str,
    cluster: dict,
    wheels: str,
    requirement: List[str] = None,
    sparklibs: List[dict] = None,
    out_json: IO[str] = None,
        main_script: IO[str]=None
):
    if requirement is None:
        requirement = []
    if sparklibs is None:
        sparklibs=[]

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
        main_file = push_main_file(test_folder,main_script)

        print(f"copied everything to {test_folder.remote}")

        # construct the workflow object
        workflow = dict(run_name="Testing Run", format="MULTI_TASK", tasks=[])

        # subtasks will be ['tests/cluster/job1', 'tests/cluster/job2'] or similar
        for task in discover_job_tasks(test_path, folder):
            task_sub = task.replace("/","_")
            task_cluster = copy.deepcopy(cluster)
            task_cluster['cluster_log_conf']={'dbfs':{'destination':f"{test_folder.remote}/{task_sub}"}}

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
                        ],
                    ),
                    new_cluster=task_cluster
                )
            )
    with tempfile.TemporaryDirectory() as tmp:
        jobfile = f"{tmp}/job.json"
        with open(jobfile, 'w') as f:
            json.dump(workflow,f)
        # with open(jobfile) as f:
        #     print(f.read())
        res = dbjcall(f"runs submit --json-file {jobfile}")
        try:
            run_id=res["run_id"]
        except KeyError:
            print(res)
            raise

    # now we have the run_id
    print(f"Started run with ID {run_id}")
    details = dbjcall(f'runs get --run-id {run_id}')
    print(f"Follow job details at {details['run_page_url']}")

    if out_json:
        json.dump({'run_id':run_id},out_json)