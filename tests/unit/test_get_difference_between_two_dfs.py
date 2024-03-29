import unittest
import warnings

from atc.spark import Spark
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlrtools.helpers.get_difference_between_two_dfs import (
    get_difference_between_two_dfs,
)


class DifferenceBetweenTwoDfsTest(unittest.TestCase):
    data1 = [
        ("1", "Hello", 1),
    ]

    schema1 = StructType(
        [
            StructField("Col1", StringType(), True),
            StructField("Col2", StringType(), True),
            StructField("Col3", IntegerType(), True),
        ]
    )

    df1 = Spark.get().createDataFrame(data=data1, schema=schema1)

    data2 = [
        ("1", "Hello", 2),
    ]

    schema2 = StructType(
        [
            StructField("Col1", StringType(), True),
            StructField("Col2", StringType(), True),
            StructField("Col3", IntegerType(), True),
        ]
    )

    df2 = Spark.get().createDataFrame(data=data2, schema=schema2)

    data3 = [
        ("1", "Dummy", 1),
    ]

    schema3 = StructType(
        [
            StructField("Col1", StringType(), True),
            StructField("OnlyInDf3", StringType(), True),
            StructField("Col3", IntegerType(), True),
        ]
    )

    df3 = Spark.get().createDataFrame(data=data3, schema=schema3)

    data4 = [
        ("1", "Dummy", 1),
    ]

    schema4 = StructType(
        [
            StructField("Col1", StringType(), True),
            StructField("OnlyInDf4", StringType(), True),
            StructField("Col3", IntegerType(), True),
        ]
    )

    df4 = Spark.get().createDataFrame(data=data4, schema=schema4)

    def test_01a_all_match(self):
        """There should be no difference between df1 compared with itself."""
        self.assertEqual(
            0,
            get_difference_between_two_dfs(
                self.df1, self.df1, join_cols=["col1"]
            ).count(),
        )

    def test_01b_all_match_can_print(self):
        """There should be no difference between df1 compared with itself."""
        self.assertEqual(
            0,
            get_difference_between_two_dfs(
                self.df1, self.df1, join_cols=["col1"], print_result=True
            ).count(),
        )

    def test_02_has_difference(self):
        """There should be difference between df1 and df2."""
        result = get_difference_between_two_dfs(self.df1, self.df2, join_cols=["col1"])

        self.assertEqual(
            1,
            result.count(),
        )

        self.assertEqual("1", result.collect()[0]["col1"])
        self.assertEqual("Hello", result.collect()[0]["df1_col2"])
        self.assertEqual("Hello", result.collect()[0]["df2_col2"])
        self.assertEqual(1, result.collect()[0]["df1_col3"])
        self.assertEqual(2, result.collect()[0]["df2_col3"])

    def test_03_ignore_difference(self):
        """When ignoring the column with the difference, there should be no difference detected."""

        self.assertEqual(
            0,
            get_difference_between_two_dfs(
                self.df1, self.df2, join_cols=["col1"], ignore_cols=["col3"]
            ).count(),
        )

    def test_04_warnings(self):
        with warnings.catch_warnings(record=True) as warning_list:
            get_difference_between_two_dfs(self.df3, self.df4, join_cols=["col1"])
            print(warning_list[0].message)
            self.assertIn(
                "Some of the columns in df1 is not in df2",
                str(warning_list[0].message),
            )

            self.assertIn(
                "Some of the columns in df2 is not in df1",
                str(warning_list[1].message),
            )
