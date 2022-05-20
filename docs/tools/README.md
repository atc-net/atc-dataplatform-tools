# Overview of atc-dataplatform-tools

## ValidateCamelCasedCols

The function takes a dataframe and validates if all columns or a given subset of columns are camelCased.
The algorithm is simple, where the following must hold:
* Column name must be camelCased.
* Column name must NOT contain two or more recurrent characters. 

```python
        from pyspark.sql.types import StructType, StructField, StringType
        from atc.spark import Spark
        from atc_tools.format.CheckCamelCase import ValidateCamelCasedCols
        data1 = [
            (None,),
        ]

        schema1 = StructType(
            [
                StructField("camelCased", StringType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data1, schema=schema1)

        ValidateCamelCasedCols(df)

        OUTPUT: True
```