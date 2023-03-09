import sys
import unittest

from atc_tools.helpers import ModuleHelper


class DummyType:
    pass


class ModuleOne(DummyType):
    pass


class ModuleTwo(DummyType):
    pass


class TestModuleHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sys.path.insert(0, "tests")
        cls.test_module = "unit.helpers.module_helper.test_module_helper"

    def test_get_modules(self):
        modules = ModuleHelper.get_modules(self.test_module)

        self.assertTrue("unit.helpers.module_helper.test_module_helper" in modules)

    def test_get_modules_and_sub_modules(self):
        # to test if the get_modules() method is able to get modules and sub modules
        modules = ModuleHelper.get_modules("unit.helpers.module_helper.dummy_module")

        self.assertTrue("unit.helpers.module_helper.dummy_module.foo" in modules)
        self.assertTrue(
            "unit.helpers.module_helper.dummy_module.submodule.bar" in modules
        )

    def test_get_modules_with_invalid_package(self):
        package = "invalid_package_name"
        with self.assertRaises(ModuleNotFoundError):
            ModuleHelper.get_modules(package)

    def test_get_classes_of_type(self):
        classes = ModuleHelper.get_classes_of_type(
            self.test_module,
            DummyType,
        )

        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.DummyType" in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleOne" in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleTwo" in classes
        )

    def test_get_classes_of_type_only_main_classes(self):
        classes = ModuleHelper.get_classes_of_type(
            self.test_module,
            DummyType,
            True,
            False,
        )

        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.DummyType" in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleOne" not in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleTwo" not in classes
        )

    def test_get_classes_of_type_only_sub_classes(self):
        classes = ModuleHelper.get_classes_of_type(
            self.test_module,
            DummyType,
            False,
            True,
        )

        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.DummyType" not in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleOne" in classes
        )
        self.assertTrue(
            "unit.helpers.module_helper.test_module_helper.ModuleTwo" in classes
        )


if __name__ == "__main__":
    unittest.main()
