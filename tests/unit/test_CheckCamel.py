import unittest
from pyspark.sql.types import StructType, StructField, StringType
from atc.spark import Spark
from src.atc_tools.format.validate_camelcased_cols import validate_camelcased_cols


class CamelCaseTest(unittest.TestCase):
    def test_01_pass(self):

        data1 = [
            (None,),
        ]

        schema1 = StructType(
            [
                StructField("camelCased", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data1, schema=schema1)

        self.assertTrue(validate_camelcased_cols(df))

    def test_02_pass(self):

        data2 = [
            (
                None,
                None,
            ),
        ]

        schema2 = StructType(
            [
                StructField("camelCased", StringType(), True),
                StructField("camelCasedAnother", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data2, schema=schema2)

        self.assertTrue(validate_camelcased_cols(df))

    def test_03_subset_pass(self):

        data3 = [
            (
                None,
                None,
            ),
        ]

        schema3 = StructType(
            [
                StructField("camelCased", StringType(), True),
                StructField("NotcamelCased", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data3, schema=schema3)

        self.assertTrue(validate_camelcased_cols(df, ["camelCased"]))

    def test_04_no_pass(self):

        data4 = [
            (
                None,
                None,
            ),
        ]

        schema4 = StructType(
            [
                StructField("camelCased", StringType(), True),
                StructField("NotcamelCased", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data4, schema=schema4)

        self.assertFalse(
            validate_camelcased_cols(
                df,
            )
        )

    def test_05_no_pass(self):

        data5 = [
            (
                None,
                None,
            ),
        ]

        schema5 = StructType(
            [
                StructField("NotcamelCased1", StringType(), True),
                StructField("NotcamelCased2", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data5, schema=schema5)

        self.assertFalse(
            validate_camelcased_cols(
                df,
            )
        )

    def test_07_no_pass(self):

        data7 = [
            (
                None,
                None,
            ),
        ]

        schema7 = StructType(
            [
                StructField("camelCased", StringType(), True),
                StructField("notCamelCAsed", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data7, schema=schema7)

        self.assertFalse(validate_camelcased_cols(df, ["notCamelCAsed"]))

    def test_08_valueerror(self):
        data8 = [
            (
                None,
                None,
            ),
        ]

        schema8 = StructType(
            [
                StructField("colA", StringType(), True),
                StructField("ColB", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data8, schema=schema8)

        with self.assertRaises(ValueError) as cm:
            validate_camelcased_cols(df, ["colC"])
        the_exception = cm.exception

        self.assertEqual(
            str(the_exception),
            str(ValueError("Some of the columns to check is not in the dataframe.")),
        )
