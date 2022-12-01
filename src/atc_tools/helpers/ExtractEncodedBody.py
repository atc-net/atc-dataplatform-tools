import json

import pyspark.sql.functions as f
import pyspark.sql.types as t
from atc.spark import Spark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType


class ExtractEncodedBody:
    def __init__(
        self,
        json_field: str = "Body",
        new_column_w_extracted_body: str = None,
        data_limit: int = None,
    ):
        self.json_field = json_field
        self.new_field_w_extracted_body = new_column_w_extracted_body or json_field
        self._data_limit = data_limit
        self._fields_same_name = self.json_field == self.new_field_w_extracted_body

    def extract_schema(self, df: DataFrame) -> StructType:
        assert self.json_field in df.columns, "The body column is not in the dataframe"

        json_data = self._extract_using_rdd(df)

        return Spark.get().read.json(json_data).schema

    def extract_schema_as_json(self, df: DataFrame, pretty_json: bool = False) -> str:

        schema = self.extract_schema(df)

        # load the json as a data frame. Schema is inferred from data.
        json_schema_raw = schema.jsonValue()

        return json.dumps(json_schema_raw, indent=4 if pretty_json else None)

    def _extract_using_rdd(self, df: DataFrame):

        # extract json data rows using name of json_field
        json_data = df.withColumn(
            self.json_field, f.col(self.json_field).cast(t.StringType())
        )

        # Limiting the data used for extracting schema
        if self._data_limit:
            json_data = json_data.limit(self._data_limit)

        # extract json data rows using name of json_field
        json_data = json_data.rdd.map(lambda row: row.asDict()[self.json_field])

        return json_data

    def transform_df(
        self, df: DataFrame, keep_original_body: bool = False
    ) -> DataFrame:

        if self._fields_same_name and keep_original_body:
            raise ValueError(
                f"The field {self.json_field} is overwritten by the extracted json. Give extracted field new name to "
                f"keep original body. "
            )

        # Step 1: extract the schema
        schema = self.extract_schema(df)

        # Step 2: decode the body column
        df_transformed = df.withColumn(
            self.new_field_w_extracted_body,
            f.from_json(f.decode(self.json_field, "utf-8"), schema),
        )

        # Step 3: Drop the original column
        if (not keep_original_body) and (not self._fields_same_name):
            df_transformed = df_transformed.drop(self.json_field)

        return df_transformed
