import subprocess
import tempfile
import time

from atc_tools.test_job import test_main
from atc_tools.test_job.dbcli import dbjcall, dbcall, dbfscall
from atc_tools.test_job.dbfs import DbfsLocation

class DbfsFileDoesNotExist(Exception):
    pass

class RunDetails:
    details: dict

    def __init__(self, run_id: int):
        self.run_id = run_id
        self.refresh()
        print(f"Job details: {self.details['run_page_url']}")

    def refresh(self):
        self.details = dbjcall(f"runs get --run-id {self.run_id}")

    def cancel(self):
        dbcall(f"runs cancel --run-id {self.run_id}")

    def get_stdout(self, task_key: str) -> str:
        print(f"Getting stdut for {task_key}")
        (task,) = [t for t in self.details["tasks"] if t["task_key"] == task_key]
        log_destination = task["new_cluster"]["cluster_log_conf"]["dbfs"]["destination"]
        cluster_instance = task["cluster_instance"]["cluster_id"]
        print(log_destination, cluster_instance)
        log_stout_path = f"{log_destination}/{cluster_instance}/driver/stdout"
        with tempfile.TemporaryDirectory() as tmp:

            try:
                self.wait_until_exists(log_stout_path)
            except:
                return f"-=ERROR FETCHING LOG FILES FROM  {log_stout_path}=-"

            dbfscall(f"cp {log_stout_path} {tmp}/stdout")
            with open(f'{tmp}/stdout', encoding='utf-8') as f:
                lines=iter(f)
                for l in lines:
                    if l.strip() == test_main.marker:
                        break
                return "\n".join(lines)

    def wait_until_exists(self, location:str, tries=20, sleep_s = 1):
        for i in range(tries):
            try:
                dbfscall(f"ls {location}")
                return
            except subprocess.CalledProcessError:
                time.sleep(sleep_s)

        raise DbfsFileDoesNotExist()

