from typing import Any, Iterable

from atc.delta import DeltaHandle
from atc.spark import Spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType


class DataframeCreator:
    def __init__(self, schema: StructType):
        self.schema = schema

    @classmethod
    def from_tc_delta(cls, tc_key: str):
        return cls(DeltaHandle.from_tc(tc_key).read().schema)

    @staticmethod
    def make_partial(
        schema: StructType,
        columns: Iterable[str] = None,
        data: Iterable[Iterable[Any]] = None,
    ) -> DataFrame:

        if columns is None:
            columns = schema.fieldNames()

        if data is None:
            data = []

        schema_fields = {f.name: f for f in schema.fields}
        try:
            temp_schema = [schema_fields[col] for col in columns]

            df: DataFrame = Spark.get().createDataFrame(data, StructType(temp_schema))
            for col in schema.fieldNames():
                if col not in columns:
                    df = df.withColumn(col, lit(None).cast(schema_fields[col].dataType))

            return df.select(*schema.fieldNames())
        except KeyError as e:
            raise KeyError(
                f'Could not find "{e.args[0]}" column in schema with columns {list(schema_fields)}'
            )
