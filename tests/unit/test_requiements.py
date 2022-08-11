import unittest

import atc_tools.requirements


class RequirementsTest(unittest.TestCase):
    def test_function(self):
        freeze = atc_tools.requirements.freeze_req(
            """
            backports.zoneinfo >= 0.1

            # comment here
            pytest
            """,
            reject="pip",
        )
        deps = {lib["name"]: lib["version"] for lib in freeze}

        self.assertIn("backports.zoneinfo", deps)
        self.assertIn("pytest", deps)
        self.assertIn("iniconfig", deps)
        self.assertTrue("pip" not in deps)
