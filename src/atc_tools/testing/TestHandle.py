from typing import List, Union

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
        self.upserted = None
        self.upserted_join_cols = None

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

    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        self.upserted = df
        self.upserted_join_cols = join_cols
