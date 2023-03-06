import json
import subprocess
import sys


def db_check():
    try:
        import databricks_cli
    except ImportError:
        print("Databricks CLI is not installed", file=sys.stderr)
        exit(-1)

    try:
        dbjcall("clusters list  --output=JSON")
    except subprocess.CalledProcessError:
        print(
            "Could not query databricks state. Is your token up to date?",
            file=sys.stderr,
        )
        exit(-1)
    dbcall("jobs configure --version=2.1")

def dbjcall(command: str):
    p = subprocess.run(
        "databricks " + command ,
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )
    try:
        return json.loads(p.stdout)
    except:
        print(p.stdout)
        raise


def dbfscall(command: str):
    p = subprocess.run(
        "dbfs " + command,
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )
    return p.stdout


def dbcall(command: str):
    p = subprocess.run(
        "databricks " + command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return p.stdout
