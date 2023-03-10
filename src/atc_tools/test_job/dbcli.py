import json
import subprocess
import sys


def db_check():
    """check that the databricks cli is correctly configured
    and has an up-to-date authorization token.
    Returns Nothing.
    Ends the program if this check fails."""
    try:
        import databricks_cli  # noqa
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
    """Run the command line "databricks "+command and return the unpacked json string."""
    p = subprocess.run(
        "databricks " + command,
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )
    try:
        return json.loads(p.stdout)
    except:  # noqa OK because we re-raise
        print(p.stdout)
        raise


def dbfscall(command: str):
    """Run the command line "dbfs "+command and return the output string."""
    p = subprocess.run(
        "dbfs " + command,
        shell=True,
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )
    return p.stdout


def dbcall(command: str):
    """Run the command line "databricks "+command and return the resulting string."""
    p = subprocess.run(
        "databricks " + command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return p.stdout
