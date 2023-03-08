import warnings
from itertools import chain
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def get_difference_between_two_dfs(
    df1: DataFrame,
    df2: DataFrame,
    join_cols: List[str],
    ignore_cols: List[str] = None,
    print_result: bool = False,
) -> DataFrame:
    """
    This function compares two dataframe using hashing of the columns.
    The function uses left join, which that all records from df1 are matched with records from df2.

    NB:
    The function only compares columns that appear in both tables.

    :param df1: A dataframe to compared with df2
    :param df2: A dataframe to compared with df1
    :param join_cols: A string list for the join column names
    :param ignore_cols: Columns to be ignored when comparing
    :param print_result: If true, the differences is printed

    :return: A dataframe with the differences between df1 and df2
            The column names are prefixed with df1_ or df_2 for easier comparison

    | join_col| df1_col1 | df2_col1 |
    |---------|----------|----------|
    | 1       | x        | y        |
    |---------|----------|----------|

    """

    # Lowering join and ignore columns
    join_cols = [col.lower() for col in join_cols]

    if ignore_cols is None:
        ignore_cols = []
    else:
        ignore_cols = [col.lower() for col in ignore_cols]

    # Get columns without the ignored columns
    df1_cols_without_ignores = set(
        [col.lower() for col in df1.columns if col.lower() not in ignore_cols]
    )

    df2_cols_without_ignores = set(
        [col.lower() for col in df2.columns if col.lower() not in ignore_cols]
    )

    if not df2_cols_without_ignores.issubset(df1_cols_without_ignores):
        warnings.warn(
            "Some of the columns in df1 is not in df2. The tables will therefore always be different!\n"
            "Remember, this function only compares the columns that appear in both tables.\n"
            "Continuing..."
        )

    if not df1_cols_without_ignores.issubset(df2_cols_without_ignores):
        warnings.warn(
            "Some of the columns in df2 is not in df1. The tables will therefore always be different!\n"
            "Remember, this function only compares the columns that appear in both tables. \n"
            "Continuing..."
        )

    # Select same columns that appear in both tables. Without the ignore_cols.
    _cols = list(df1_cols_without_ignores & df2_cols_without_ignores)

    # Generate hash columns
    df1_sub = df1.select(_cols).withColumn(
        "row_sha2", f.sha2(f.concat_ws("||", *_cols), 256)
    )
    df2_sub = df2.select(_cols).withColumn(
        "row_sha2", f.sha2(f.concat_ws("||", *_cols), 256)
    )

    # Select the order as a.col1, b.col1, a.col2, b.col2.... this way it is easier to see differences.
    # Also prefixes df1_ and df2_

    order_list = [
        [f.col(f"a.{col}").alias(f"df1_{col}"), f.col(f"b.{col}").alias(f"df2_{col}")]
        for col in _cols
        if col not in ignore_cols + join_cols
    ]

    order_list = list(chain.from_iterable(order_list))

    # Generating the differences
    _join_cols_a = [f"a.{col}" for col in join_cols]

    df_res = (
        df1_sub.alias("a")
        .join(df2_sub.alias("b"), join_cols, "left")
        .filter("a.row_sha2 != b.row_sha2")
        .select(*_join_cols_a, *order_list)
    )

    if print_result:
        print(f"The number of records with differences: {df_res.count()}")
        df_res.show()

    return df_res
