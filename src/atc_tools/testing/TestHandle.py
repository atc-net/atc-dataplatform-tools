from atc.tables.TableHandle import TableHandle
from pyspark.sql import DataFrame


class TestHandle(TableHandle):
    def __init__(self, provides: DataFrame = None):
        self.provides = provides
        self.overwritten = None
        self.appended = None
        self.truncated = False
        self.dropped = False
        self.dropped_and_deleted = False

    def read(self) -> DataFrame:
        if self.provides is not None:
            return self.provides
        else:
            raise AssertionError("TableHandle not readable.")

    def overwrite(self, df: DataFrame) -> None:
        self.overwritten = df

    def append(self, df: DataFrame) -> None:
        self.appended = df

    def truncate(self) -> None:
        self.truncated = True

    def drop(self) -> None:
        self.dropped = True

    def drop_and_delete(self) -> None:
        self.dropped_and_deleted = True
