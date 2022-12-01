import json
import unittest

from atc.spark import Spark
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
)

from atc_tools.helpers.ExtractEncodedBody import ExtractEncodedBody


class ExtractEncodedBodyTest(unittest.TestCase):
    # Encode binary data
    binary_data1 = json.dumps({"a": "text", "b": 1, "c": False, "d": 2.5}).encode(
        "utf-8"
    )
    binary_data2 = json.dumps({"a": "text2", "b": 2, "c": True, "d": 3.5}).encode(
        "utf-8"
    )

    # The expected schema extracted from the above binary encoded data
    expected_schema = (
        '{"type": "struct", "fields": [{"name": "a", "type": "string", "nullable": true, '
        '"metadata": {}}, {"name": "b", "type": "long", "nullable": true, "metadata": {}}, '
        '{"name": "c", "type": "boolean", "nullable": true, "metadata": {}}, {"name": "d", '
        '"type": "double", "nullable": true, "metadata": {}}]}'
    )

    # expected_schema = json.loads(expected_schema)

    expected_schema_pretty = """{
    "type": "struct",
    "fields": [
        {
            "name": "a",
            "type": "string",
            "nullable": true,
            "metadata": {}
        },
        {
            "name": "b",
            "type": "long",
            "nullable": true,
            "metadata": {}
        },
        {
            "name": "c",
            "type": "boolean",
            "nullable": true,
            "metadata": {}
        },
        {
            "name": "d",
            "type": "double",
            "nullable": true,
            "metadata": {}
        }
    ]
}"""

    # When the dataframe is simply transformed - this is the expected pyspark schema
    expected_transformed_schema = StructType(
        [
            StructField(
                "Body",
                StructType(
                    [
                        StructField("a", StringType(), True),
                        StructField("b", LongType(), True),
                        StructField("c", BooleanType(), True),
                        StructField("d", DoubleType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    # Creating dataframe with the encoded body
    data1 = [(binary_data1,), (binary_data2,)]
    schema1 = StructType([StructField("Body", BinaryType(), True)])
    df_input = Spark.get().createDataFrame(data=data1, schema=schema1)

    # Initialize extractor
    extractor = ExtractEncodedBody()

    # Initialize extractor with data limit
    extractor_limit = ExtractEncodedBody(data_limit=1)

    # Initialize a bad extractor. The Unknown_col is not in the input data.
    extractor_bad = ExtractEncodedBody("Unkown_col")

    # Initialize an extractor with a new column for extracted body.
    extractor_new_field = ExtractEncodedBody(new_column_w_extracted_body="BodyNew")

    # Expected schema when the body is extracted to a new column.
    expected_transformed_schema_w_new_col = StructType(
        [
            StructField(
                "BodyNew",
                StructType(
                    [
                        StructField("a", StringType(), True),
                        StructField("b", LongType(), True),
                        StructField("c", BooleanType(), True),
                        StructField("d", DoubleType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    # pyspark schema of the body
    body_pyspark_schema = StructType(
        [
            StructField("a", StringType(), True),
            StructField("b", LongType(), True),
            StructField("c", BooleanType(), True),
            StructField("d", DoubleType(), True),
        ]
    )

    # Expected schema when the body is extracted to a new column, but also keep the original Body.
    expected_transformed_schema_w_new_col_keep_orignal = StructType(
        [
            StructField(
                "Body",
                BinaryType(),
                True,
            ),
            StructField(
                "BodyNew",
                StructType(
                    [
                        StructField("a", StringType(), True),
                        StructField("b", LongType(), True),
                        StructField("c", BooleanType(), True),
                        StructField("d", DoubleType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    def test_01a_extract_schema(self):
        """Extracting the schema from the body"""

        # Extracting the schema
        extracted_schema = self.extractor.extract_schema_as_json(self.df_input)

        self.assertEqual(self.expected_schema, extracted_schema)

        # Extracting the pretty schema
        extracted_schema = self.extractor.extract_schema_as_json(
            self.df_input, pretty_json=True
        )

        self.assertEqual(self.expected_schema_pretty, extracted_schema)

        # Extracting pyspark schema
        extracted_pyspark_body_schema = self.extractor.extract_schema(self.df_input)

        self.assertEqual(self.body_pyspark_schema, extracted_pyspark_body_schema)

    def test_01b_extract_schema_w_limit(self):
        """Extracting the schema from the body with data limit"""
        # Extracting the schema
        extracted_schema = self.extractor_limit.extract_schema_as_json(self.df_input)

        self.assertEqual(self.expected_schema, extracted_schema)

    def test_02_transform_df_w_schema(self):
        """Transform the dataframe, so the body is decoded using the extracted schema"""

        # Extracting the body
        df_unpacked = self.extractor.transform_df(self.df_input)

        # Check that the pyspark schema is as expected
        self.assertEqual(self.expected_transformed_schema, df_unpacked.schema)

        # Check that the data is as expected
        self.assertEqual(
            df_unpacked.collect()[0][0],
            Row(a="text", b=1, c=False, d=2.5),
        )

    def test_03_transform_df_w_schema_keep_original_fails(self):
        """Transform the dataframe, so the body is decoded using the extracted schema.
        It should fail if we also want to keep the original since there will be name conflicts."""

        # Extracting the body

        with self.assertRaises(ValueError) as cm:
            _ = self.extractor.transform_df(self.df_input, keep_original_body=True)
        the_exception = cm.exception

        self.assertEqual(
            str(the_exception),
            str(
                ValueError(
                    "The field Body is overwritten by the extracted json. Give extracted field new name to keep "
                    "original body. "
                )
            ),
        )

    def test_04_wrong_col_name(self):
        """Check that class assertion on column in dataframe works."""

        with self.assertRaises(AssertionError) as cm:
            _ = self.extractor_bad.extract_schema(self.df_input)
        the_exception = cm.exception

        self.assertEqual(
            str(the_exception),
            str(ValueError("The body column is not in the dataframe")),
        )

    def test_05_extract_to_new_col(self):
        """Extracts the body to a new column."""

        # Extracting the body with new field
        df_unpacked = self.extractor_new_field.transform_df(self.df_input)

        # Check that the pyspark schema is as expected
        self.assertEqual(self.expected_transformed_schema_w_new_col, df_unpacked.schema)

    def test_06_extract_to_new_col_keep_original(self):
        """Extracts the body to a new column. Keep original body also."""

        # Extracting the body with new field
        df_unpacked = self.extractor_new_field.transform_df(
            self.df_input, keep_original_body=True
        )

        # Check that the pyspark schema is as expected
        self.assertEqual(
            self.expected_transformed_schema_w_new_col_keep_orignal, df_unpacked.schema
        )
