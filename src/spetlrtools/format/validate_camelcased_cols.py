import re
from typing import List

from pyspark.sql import DataFrame


def validate_camelcased_cols(
    df: DataFrame, cols_to_check: List[str] = None, print_result: bool = False
):
    """
    The function takes a dataframe and validates if all columns or a given subset of columns are camelCased.

    :param df: A pyspark dataframe
    :param cols_to_check: Optional, a subset of the columns in the dataframe to check.
    :param print_result: If True, then the cols which are not camelCased are printed
    :return: True if all columns are camelcased
    """

    if cols_to_check is None:
        cols_to_check = df.columns
    else:
        if not all(col in df.columns for col in cols_to_check):
            raise ValueError("Some of the columns to check is not in the dataframe.")

    results_col = []
    check_passed = True
    for col in cols_to_check:
        is_camel_cased = bool(re.match("[a-z]+([A-Z][a-zA-Z]*)?", col))
        has_two_upper = bool(re.match(".*[A-Z][A-Z]", col))
        if not is_camel_cased or has_two_upper:
            check_passed = False
            results_col.append(col)

    # Todo : print results
    if print_result:
        print(
            f"The following columns are not camelCased: {results_col}"
            if any(results_col)
            else "All tested columns are camelCased!"
        )

    return check_passed
