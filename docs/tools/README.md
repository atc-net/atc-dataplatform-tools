# Overview of atc-dataplatform-tools

## ValidateCamelCasedCols

The function takes a dataframe and validates if all columns or a given subset of columns are camelCased.
The algorithm is simple, where the following must hold:
* Column name must be camelCased.
* Column name must NOT contain two or more recurrent characters. 

``` python
from pyspark.sql.types import StructType, StructField, StringType
from atc.spark import Spark
from atc_tools.format.validate_camelcased_cols import validate_camelcased_cols
data1 = [
    (None,),
]

schema1 = StructType(
    [
        StructField("camelCased", StringType(), True),
    ]
)

df = Spark.get().createDataFrame(data=data1, schema=schema1)

validate_camelcased_cols(df)

OUTPUT: True
```

## ExtractEncodedBody

*This is just a tool for investigating - not for production purposes.*

Some files - like eventhub capture files - contains a binary encoded *Body* column. The `ExtractEventhubBody` class can help decode the column.
Either one can get the encoded schema as a json schema (`extract_json_schema`) or transform the dataframe using `transform_df`.

Be aware, that the schema extraction can be a slow process, so it is not recommended to use the extractor in a production setting. 
*HINT: You should in stead find a way to have a static schema definition. Either as a json schema, pyspark struct 
or read the schema from a target table - and use that for decode the Body.*

``` python
from atc_tools.helpers.ExtractEncodedBody import ExtractEncodedBody

# EventHub dataframe 
df_encodedbody.show()

OUTPUT:
+-------------+
|Body         |
+-------------+
|836dhk392649i|
+-------------+

# Extracting/decoding the body
ExtractEncodedBody().transform_df(df_encodedbody).show()

OUTPUT:
+-------------------+
|Body               |
+-------------------+
|{text,1,False, 2.5}|
+-------------------+
```