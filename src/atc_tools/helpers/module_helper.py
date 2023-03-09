import importlib
import pkgutil
import sys
from inspect import getmembers, isclass
from types import ModuleType
from typing import Dict, Union


class ModuleHelper:
    @staticmethod
    def get_modules(package: Union[str, ModuleType]) -> Dict[str, ModuleType]:
        """
        Retrieves all modules from a given package or module.

        Args:
            package (Union[str, ModuleType]):
                The name of the package or a reference to the module.

        Returns:
            Dict[str, ModuleType]: A dictionary containing all modules.
        """
        modules = {}

        package = importlib.import_module(package)

        if not hasattr(package, "__path__"):
            modules[package.__name__] = package

        else:
            for ModuleInfo in pkgutil.walk_packages(package.__path__):
                module_name = f"{package.__name__}.{ModuleInfo.name}"

                if ModuleInfo.ispkg:
                    modules.update(ModuleHelper.get_modules(module_name))

                else:
                    module = importlib.import_module(module_name)
                    modules.update({module_name: module})

        return modules

    @staticmethod
    def get_classes_of_type(
        package: Union[str, ModuleType],
        obj: type,
        main_classes: bool = True,
        sub_classes: bool = True,
    ) -> Dict[str, dict]:
        """
        Retrieves all classes of a specified type from a package or module.

        Args:
            package (Union[str, ModuleType]):
                The name of the package or a reference to the module.
            obj (type):
                The type of classes to retrieve.
            main_classes (bool) default = True:
                If the main classes of the type obj should be retrieved.
            sub_classes (bool) default = True:
                If the sub classes of the type obj should be retrieved.

        Returns:
            Dict[str, dict]:
                A dictionary containing all classes of the specified type,
                along with module and class information.
        """
        modules = ModuleHelper.get_modules(package=package)

        objects = {}
        for module_name, module in modules.items():
            for cls_name, cls in getmembers(sys.modules[module_name], isclass):
                if main_classes:
                    if cls_name == obj.__name__:
                        objects[f"{module_name}.{cls_name}"] = {
                            "module_name": module_name,
                            "module": module,
                            "cls_name": cls_name,
                            "cls": cls,
                        }

                if sub_classes:
                    for base_cls in cls.__bases__:
                        if base_cls.__name__ == obj.__name__:
                            objects[f"{module_name}.{cls_name}"] = {
                                "module_name": module_name,
                                "module": module,
                                "cls_name": cls_name,
                                "cls": cls,
                            }

        return objects
