import unittest

from packaging.version import parse

from spetlrtools.manipulate_version import (
    InvalidVersionManipulation,
    _decide_next_version,
)


class VersioningTest(unittest.TestCase):
    def test_01_local_version_highest_final_release(self):
        local_v = parse("0.1.16")

        # local absolutely highest
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.15rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.16", str(next_version))

        # local above rc
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.16rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.16", str(next_version))

        # local below rc
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.17rc5")

        # in the case of fina release, the test version does not matter
        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.16", str(next_version))

    def test_02_local_version_highest_pre_release(self):
        local_v = parse("0.1.16")

        # local absolutely highest
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.15rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
        )

        self.assertEqual("0.1.16rc1", str(next_version))

        # local above rc
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.16rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
        )

        self.assertEqual("0.1.16rc6", str(next_version))

        # local below rc
        pypi_v = parse("0.1.15")
        test_v = parse("0.1.17rc5")

        # in the case of pre-release, the test version does matter
        with self.assertRaises(InvalidVersionManipulation):
            _decide_next_version(
                pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
            )

    def test_03_auto_increment_final_release(self):
        local_v = parse("0.1.0")

        # auto increment standard situation
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.15rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.15", str(next_version))

        # auto increment, test.pypi is behind
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.12rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.15", str(next_version))

        # auto increment, test.pypi is far ahead
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.17rc5")

        # in the case of fina release, the test version does not matter
        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=False
        )

        self.assertEqual("0.1.15", str(next_version))

    def test_04_auto_increment_pre_release(self):
        local_v = parse("0.1.0")

        # auto increment standard situation
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.15rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
        )

        self.assertEqual("0.1.15rc6", str(next_version))

        # auto increment, test.pypi is behind
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.12rc5")

        next_version = _decide_next_version(
            pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
        )

        self.assertEqual("0.1.15rc1", str(next_version))

        # auto increment, test.pypi is far ahead
        pypi_v = parse("0.1.14")
        test_v = parse("0.1.17rc5")

        # in the case of pre-release, the test version does matter
        with self.assertRaises(InvalidVersionManipulation):
            _decide_next_version(
                pypi_v=pypi_v, test_v=test_v, local_v=local_v, pre_release=True
            )
