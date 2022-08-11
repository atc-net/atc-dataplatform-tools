import datetime
import unittest
from typing import Any, Iterable

from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructField, StructType


class DataframeTestCase(unittest.TestCase):
    def assertDataframeMatches(
        self,
        df: DataFrame,
        columns: Iterable[str] = None,
        expected_data: Iterable[Iterable[Any]] = None,
    ):
        """
        Args:
            df:
            columns: the column names in the expected data. In case of None, all df column are used.
            expected_data: expected data, for the selected columns.
                    This necessary argument has a default value because columns can be set to default.

        Returns:
            None

        Raises:
            AssertionError in case the dataframe does not match the expected data.
        """
        if expected_data is None:
            raise ValueError("No match assert value provided")
        if columns is None:
            columns = df.schema.fieldNames()

        schema_fields = {f.name: f for f in df.schema.fields}

        assert_df = [list(row) for row in df.select(*columns).collect()]

        try:
            # we need to handle array types
            # this code should grow to handle more general types of data-frames in the future
            for i, col in enumerate(columns):
                if isinstance(schema_fields[col].dataType, ArrayType):
                    for row in assert_df:
                        if row[i] is None:
                            continue
                        elif isinstance(
                            schema_fields[col].dataType.elementType, StructType
                        ):
                            row[i] = [tuple(sub_row) for sub_row in row[i]]

                # move every datetime item to UTC time zone. This allows for exact comparison
                for row in assert_df:
                    if isinstance(row[i], datetime.datetime):
                        row[i] = row[i].astimezone(datetime.timezone.utc)

            assert_df = [tuple(row) for row in assert_df]

            assert_df.sort()

        except KeyError as e:
            raise KeyError(
                f'Could not find "{e.args[0]}" column in dataframe schema: {list(schema_fields)}'
            )

        # move every datetime item to UTC time zone. This allows for exact comparison
        expected_data = [list(row) for row in expected_data]
        for row in expected_data:
            for i, item in enumerate(row):
                if isinstance(row[i], datetime.datetime):
                    row[i] = row[i].astimezone(datetime.timezone.utc)
        expected_data = [tuple(row) for row in expected_data]
        expected_data.sort()

        self.assertEqual(expected_data, assert_df)

    def assertEqualSchema(
        self, schema1: StructType, schema2: StructType, compare_nullability=False
    ):
        """
        Comparing pyspark schemas can fail due to unequal nullability.
        This method allows the user to ignore this aspect of the StructType.
        """
        json1 = schema1.jsonValue()
        json2 = schema2.jsonValue()

        if not compare_nullability:
            # create a function for recursive clearing
            def clear_nullable(field: StructField):
                del field["nullable"]
                if not field["metadata"]:
                    del field["metadata"]
                if field["type"] == "struct":
                    for item in field["fields"]:
                        clear_nullable(item)

            for j in [json1, json2]:
                for item in j["fields"]:
                    clear_nullable(item)

        self.assertEqual(json1, json2)
