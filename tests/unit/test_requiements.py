import io
import unittest

import atc_tools.requirements


class RequirementsTest(unittest.TestCase):


    def test_function(self):
        f = io.StringIO("atc-dataplatform")
        atc_tools.requirements.manipulate_file(f)
        f.seek(0)
        s=f.read()
        parts = s.strip().split("==")
        self.assertEqual(2,len(parts))
        self.assertEqual("atc-dataplatform",parts[0])
        self.assertRegex(parts[1],r"\d+\.\d+\.\d+")