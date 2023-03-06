from dataclasses import dataclass


@dataclass
class DbfsLocation:
    remote: str = ""
    local: str = ""

    def __truediv__(self, value)->"DbfsLocation":
        return DbfsLocation(remote=self.remote+"/"+value, local=self.local+"/"+value)

    @classmethod
    def from_str(cls, val:str)->"DbfsLocation":
        result = cls()
        if val.startswith("dbfs:/"):
            result.set_remote(val)
        elif val.startswith("/dbfs/"):
            result.set_local()
        else:
            raise ValueError("Invalid dbfs path")
    def set_remote(self, remote:str)->None:
        if not remote.startswith("dbfs:/"):
            raise AssertionError("Remote locations must start with dbfs:/")
        self.local = "/dbfs/" +remote[6:]
        self.remote = remote

    def set_local(self, local:str)->None:
        if not local.startswith("/dbfs/"):
            raise AssertionError("Local locations must start with /dbfs/")
        self.remote = "dbfs:/" +local[6:]
        self.local = local
