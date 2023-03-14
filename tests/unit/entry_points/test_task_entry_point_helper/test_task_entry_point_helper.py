import os
import sys
import unittest

from atc_tools.entry_points import TaskEntryPointHelper


class TestModuleHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sys.path.insert(0, "tests")
        cls.test_path = "unit.entry_points.test_task_entry_point_helper"
        cls.test_file_path = (
            "tests/unit/entry_points/test_task_entry_point_helper/test_file.txt"
        )

    def test_get_all_task_entry_points(self):
        entry_points = TaskEntryPointHelper.get_all_task_entry_points(
            [self.test_path],
        )

        expected_output = {
            "atc_tools.task_entry_points": [
                f"{self.test_path}.dummy_module.foo.A = "
                + f"{self.test_path}.dummy_module.foo:A.task",
                f"{self.test_path}.dummy_module.submodule.bar.B = "
                + f"{self.test_path}.dummy_module.submodule.bar:B.task",
            ]
        }

        self.assertEqual(entry_points, expected_output)

    def test_get_task_entry_points_type_error(self):
        with self.assertRaises(TypeError):
            TaskEntryPointHelper.get_all_task_entry_points(
                "some_module_that_does_not_exist"
            )

    def test_get_task_entry_points_write_to_file(self):
        TaskEntryPointHelper.get_all_task_entry_points(
            [self.test_path],
            self.test_file_path,
        )

        with open(self.test_file_path) as file:
            contents = file.read()

        expected_content = (
            f"{self.test_path}.dummy_module.foo.A = "
            + f"{self.test_path}.dummy_module.foo:A.task\n"
            f"{self.test_path}.dummy_module.submodule.bar.B = "
            + f"{self.test_path}.dummy_module.submodule.bar:B.task\n"
        )

        self.assertEqual(contents, expected_content)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(cls.test_file_path):
            os.remove(cls.test_file_path)


if __name__ == "__main__":
    unittest.main()
