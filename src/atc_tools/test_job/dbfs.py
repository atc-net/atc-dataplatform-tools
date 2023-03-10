from dataclasses import dataclass


@dataclass
class DbfsLocation:
    """A Dbfs location needs to be accessed through dbfs:/ when acting remotely
    through the cli.
    When running in the cluster, the files are accessed locally under /dbfs/.
    This class unifies the two methods under one class
    """

    base: str = ""

    @property
    def remote(self):
        return "dbfs:/" + self.base

    @property
    def local(self):
        return "/dbfs/" + self.base

    def __truediv__(self, value) -> "DbfsLocation":
        return DbfsLocation(base=self.base + "/" + value)

    @classmethod
    def from_str(cls, val: str) -> "DbfsLocation":
        if val[:6] not in ["dbfs:/", "/dbfs/"]:
            raise ValueError("Invalid dbfs path")

        return cls(base=val[:6])

    @remote.setter
    def set_remote(self, remote: str) -> None:
        if not remote.startswith("dbfs:/"):
            raise AssertionError("Remote locations must start with dbfs:/")
        self.base = remote[6:]

    @local.setter
    def set_local(self, local: str) -> None:
        if not local.startswith("/dbfs/"):
            raise AssertionError("Local locations must start with /dbfs/")
        self.base = local[6:]
