from typing import List
from pyspark.sql import DataFrame
import re

def DfIsCamel(df: DataFrame, cols_to_check: List[str] = None, print_result: bool = False):
    """
    :param df: A pyspark dataframe
    :param cols_to_check: Optional, a subset of the columns in the dataframe to check.
    :return: True if all columns are camelcased
    """


    if cols_to_check is None:
        cols_to_check = df.columns
    else:
        if not all(col in cols_to_check for col in df.columns):
            raise ValueError("Some of the columns to check is not in the dataframe.")


    results = []
    for col in cols_to_check:
        # If camelcased (regex 1) and dont have two uppercases (regex  then True
        isCamelCased = bool(re.match("[a-z]+([A-Z][a-zA-Z]*)?", col))
        hasTwoUpper =  bool(re.match(".*[A-Z][A-Z]", col))
        results.append(isCamelCased and not hasTwoUpper)

    # Todo : print results
    if print_result:
        cols_to_check[results]

    return all(results)

