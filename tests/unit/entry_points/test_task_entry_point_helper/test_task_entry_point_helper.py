import os
import sys
import unittest
from abc import ABC, abstractmethod

from atc_tools.entry_points import TaskEntryPointHelper


class TestModuleHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sys.path.insert(0, "tests")
        cls.test_path = "unit.entry_points.test_task_entry_point_helper.dummy_module"
        cls.test_file_path = (
            "tests/unit/entry_points/test_task_entry_point_helper/test_file.txt"
        )

    def test_get_all_task_entry_points(self):
        entry_points = TaskEntryPointHelper.get_all_task_entry_points(
            [self.test_path],
        )

        expected_output = {
            "atc_tools.task_entry_points": [
                f"{self.test_path}.foo.A = " + f"{self.test_path}.foo:A.task",
                f"{self.test_path}.submodule.bar.B = "
                + f"{self.test_path}.submodule.bar:B.task",
            ]
        }

        self.assertEqual(entry_points, expected_output)

    def test_type_error(self):
        with self.assertRaises(TypeError):
            TaskEntryPointHelper.get_all_task_entry_points(
                "some_module_that_does_not_exist"
            )

    def test_write_to_file(self):
        TaskEntryPointHelper.get_all_task_entry_points(
            [self.test_path],
            self.test_file_path,
        )

        with open(self.test_file_path) as file:
            contents = file.read()

        expected_content = (
            f"{self.test_path}.foo.A = " + f"{self.test_path}.foo:A.task\n"
            f"{self.test_path}.submodule.bar.B = "
            + f"{self.test_path}.submodule.bar:B.task\n"
        )

        self.assertEqual(contents, expected_content)

    def test_use_other_entry_point_object(self):
        class OtherBaseClass(ABC):
            @classmethod
            @abstractmethod
            def task(cls) -> None:
                pass

        test_path = "unit.entry_points.test_task_entry_point_helper.other_dummy_module"

        entry_points = TaskEntryPointHelper.get_all_task_entry_points(
            packages=[test_path],
            entry_point_object=OtherBaseClass,
        )

        expected_output = {
            "atc_tools.task_entry_points": [
                f"{test_path}.foo.A = {test_path}.foo:A.task",
                f"{test_path}.foo.B = {test_path}.foo:B.task",
            ]
        }

        self.assertEqual(entry_points, expected_output)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(cls.test_file_path):
            os.remove(cls.test_file_path)


if __name__ == "__main__":
    unittest.main()
