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

## ModuleHelper

The `ModuleHelper` class provides developers with a useful tool for interacting with modules in Python. Its primary purpose is to allow developers to retrieve all modules from a given package or module in a flexible manner, without requiring detailed knowledge of the module structure. Additionally, the `ModuleHelper` class enables developers to retrieve classes and/or subclasses of a specified type from a package or module, further simplifying the process of working with multiple modules.

For example, consider a scenario where a developer is working on a large-scale Python project with numerous modules, many of which may not be directly related to the current task at hand. By using the `ModuleHelper` class, the developer can quickly and easily retrieve all relevant modules or classes/subclasses, without needing to know the precise structure or location of each individual module/class/subclass. This can save significant time and effort, as well as making the code more maintainable and easier to understand.

### Example - `get_modules()` method

Consider the following project:

```
.
└── src/
    └── the_project/
        ├── denmark/
        │   ├── foo/
        │   │   └── sub
        │   ├── bar/
        │   │   └── sub
        │   └── ...
        ├── norway/
        │   └── ...
        └── sweden/
            └── ...
```

The modules `foo` and `bar` along with their submodules can be retrieved using the `get_modules()` method:

```python
from atc_tools.helpers import ModuleHelper

denmark_modules = ModuleHelper.get_modules(
    package="the_project.denmark",
)
```

The above returns a dictionary, where the keys point to the location of the modules. The values are the respective module of type `ModuleType` from the builtin types library:
```python
{
    "the_project.denmark.foo": ModuleType,
    "the_project.denmark.bar": ModuleType,
    "the_project.denmark.foo.sub": ModuleType,
    "the_project.denmark.bar.sub": ModuleType,
}
```

### Example - `get_classes_of_type()` method

Building on the previous example, say that for the project the following classes are defined within the `foo` module:

```python
class A:
    ... # implementation of class A

class B(A):
    ... # implementation of class B
```

And, within the `bar` module the following:

```python
from the_project.denmark.foo import A

class C(A):
    ... # implementation of class A
```

We have that `foo` defines a `class A`, where `class B` and `class C` are subclasses (inherited) hereof (and they reside within different modules).

Using the `get_classes_of_type()` method from the `ModuleHelper` the main class A can be retrieved together with its subclasses class B and C:

```python
from atc_tools.helpers import ModuleHelper
from the_project.denmark.foo import A

classes_and_subclasses_of_type_A = ModuleHelper.get_classes_of_type(
    package="the_project.denmark",
    obj=A,
)
```

The above returns a dictionary, where the keys point to the location of the classes. The values are a respective dictionary containing information about the module that the class is associated with and the class itself:

```python
{
    "the_project.denmark.foo.A": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "the_project.denmark.foo.B": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "the_project.denmark.foo.C": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
}
```

The `get_classes_of_type()` method is configurable such that only classes of the `obj` type is returned:
```python
from atc_tools.helpers import ModuleHelper
from the_project.denmark.foo import A

only_main_classes_of_type_A = ModuleHelper.get_classes_of_type(
    package="the_project.denmark",
    obj=A,
    main_classes=True,
    sub_classes=False,
)
```

The above returns: 
```python
{
    "the_project.denmark.foo.A": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type}
}
```

Or it can be configured to only return subclasses:

```python
from atc_tools.helpers import ModuleHelper
from the_project.denmark.foo import A

only_main_classes_of_type_A = ModuleHelper.get_classes_of_type(
    package="the_project.denmark",
    obj=A,
    main_classes=False,
    sub_classes=True,
)
```

The above returns: 
```python
{
    "the_project.denmark.foo.B": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "the_project.denmark.foo.C": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
}
```