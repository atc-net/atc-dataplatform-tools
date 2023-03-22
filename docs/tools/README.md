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
    ├── dataplatform/
    │   ├── foo/
    │   │   ├── __init__.py
    │   │   └── main.py
    │   └── bar/
    │       ├── __init__.py
    │       └── sub.py
    └── __init__.py
```

The modules `dataplatform.foo.main` and `dataplatform.bar.sub` can be retrieved using the `get_modules()` method (if either module had any submodules those would be retrieved as well):

```python
from atc_tools.helpers import ModuleHelper

denmark_modules = ModuleHelper.get_modules(
    package="dataplatform",
)
```

The above returns a dictionary, where the each key point to the location of a module. The values are the respective module of type `ModuleType` (from the builtin types library):

```python
{
    "dataplatform.foo.main": ModuleType,
    "dataplatform.bar.sub": ModuleType,
}
```

### Example - `get_classes_of_type()` method

Building on the previous example, say that `/main.py` looks as follows:

```python
class A:
    ... # implementation of class A

class B(A):
    ... # implementation of class B
```

And, within `/sub.py` the following:

```python
from dataplatform.foo.main import A

class C(A):
    ... # implementation of class C

class D:
    ... # implementation of class D
```

We have that `/main.py` defines a `class A`. And `class B` and `class C` are subclasses (inherited) hereof. Keep in mind, `class C` is inherits from `class A` and that `class A` is imported from the `dataplatform.foo.main` module. `class D` just sits in `dataplatform.bar.sub` but is not a subclass of any of the other classes.

Using the `get_classes_of_type()` method from the `ModuleHelper` all definitions of `class A` can be retrieved together with its subclasses `class B` and `class C` (and not `class D`):

```python
from atc_tools.helpers import ModuleHelper
from dataplatform.foo.main import A

classes_and_subclasses_of_type_A = ModuleHelper.get_classes_of_type(
    package="dataplatform",
    obj=A,
)
```

The above returns a dictionary, where the keys point to the location of the classes. The values are a respective dictionary containing information about the module that the class is associated with and the class itself:

```python
{
    "dataplatform.foo.main.A": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "dataplatform.foo.main.B": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "dataplatform.bar.sub.C": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
}
```

The `get_classes_of_type()` method is configurable such that only classes of the `obj` type is returned and not its subclasses:
```python
from atc_tools.helpers import ModuleHelper
from dataplatform.foo.main import A

only_main_classes_of_type_A = ModuleHelper.get_classes_of_type(
    package="dataplatform",
    obj=A,
    sub_classes=False,
)
```

The above returns: 
```python
{
    "dataplatform.foo.main.A": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type}
}
```

Or it can be configured to only return subclasses:

```python
from atc_tools.helpers import ModuleHelper
from dataplatform.foo.main import A

only_main_classes_of_type_A = ModuleHelper.get_classes_of_type(
    package="dataplatform",
    obj=A,
    main_classes=False,
)
```

The above returns: 
```python
{
    "dataplatform.foo.main.B": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
    "dataplatform.bar.sub.C": {"module_name": str, "module": ModuleType, "cls_name": str, "cls", type},
}
```

## TaskEntryPointHelper

The `TaskEntryPointHelper` provides the method `get_all_task_entry_points()`, which uses the ModuleHelper (see the documentation above) to retrieve all `task()` methods of the subclasses of the class `TaskEntryPoint`. Note that `TaskEntryPoint` is an abstract base class from atc-dataplatform, see the documentation over there.

### Example - `get_all_task_entry_points()` method

Consider the following project:

```
.
└── src/
    ├── dataplatform/
    │   ├── foo/
    │   │   ├── __init__.py
    │   │   └── main.py
    │   └── bar/
    │       ├── __init__.py
    │       └── sub.py
    └── __init__.py
```

The module `dataplatform.foo.main` has:

```python
from atc.entry_points import TaskEntryPoint

class First(TaskEntryPoint):
    @classmethod
    def task(cls) -> None:
        ...  # implementation of the task here
```

And the module `dataplatform.bar.sub` has:

```python
from atc.entry_points import TaskEntryPoint

class Second(TaskEntryPoint):
    @classmethod
    def task(cls) -> None:
        ...  # implementation of the task here
```

Now, by utilizing the `get_all_task_entry_points()` method all the `task()` class methods can automatically be discovered as entry points:

```python
from atc_tools.entry_points import TaskEntryPointHelper

TaskEntryPointHelper.get_all_task_entry_points(
    packages=["dataplatform.foo", "dataplatform.bar"],
)
```

This returns a dictionary:
```python
{
    "atc_tools.task_entry_points": [
        "dataplatform.foo.main.First = dataplatform.foo.main:First.task",
        "dataplatform.bar.sub.Second = dataplatform.bar.sub:Second.task",
    ],
}
```

The developer can add this key-value pair to their setup of their package. When new subclasses of the `TaskEntryPoint` class are added then this function automatically discover the entry points for their `task()` methods.

If the developer wants to see the entry points, a path to a txt file can be added when executing the method:

```python
from atc_tools.entry_points import TaskEntryPointHelper

TaskEntryPointHelper.get_all_task_entry_points(
    packages=["dataplatform.foo", "dataplatform.bar"],
    output_txt_file="discovered_task_entry_points.txt",
)
```

This produces a text file named `discovered_task_entry_points.txt` and contains:
```
dataplatform.foo.main.First = dataplatform.foo.main:First.task
dataplatform.bar.sub.Second = dataplatform.bar.sub:Second.task
```

This way it is easy to verify and check entry points manually if the developers workflow depends on this.

### Example - Using the `get_all_task_entry_points()` method with a different base class

The `get_all_task_entry_points()` method is tied closely with the atc-dataplatform `TaskEntryPoint` class. If there is a use case for implementing other custom base classes (with a `task()` abstract class method) then a `entry_point_objects` list variable can be set to look for a different base classes. See below example:

```python
from abc import ABC, abstractmethod
from atc_tools.entry_points import TaskEntryPointHelper

class OtherBaseClass(ABC):
    @classmethod
    @abstractmethod
    def task(cls) -> None:
        pass


class A(OtherBaseClass):
    @classmethod
    def task(cls) -> None:
        pass


class B(OtherBaseClass):
    @classmethod
    def task(cls) -> None:
        pass


class AnotherBaseClass(ABC):
    @classmethod
    @abstractmethod
    def task(cls) -> None:
        pass


class C(AnotherBaseClass):
    @classmethod
    def task(cls) -> None:
        pass


TaskEntryPointHelper.get_all_task_entry_points(
    packages=["dataplatform.foo", "dataplatform.bar"],
    entry_point_objects=[OtherBaseClass],
)
```

This returns a dictionary of entry points pointing to `A`, `B`, and `C` as they are children of the new `OtherBaseClass` and `AnotherBaseClass` classes.
```