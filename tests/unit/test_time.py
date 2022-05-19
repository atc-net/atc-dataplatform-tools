import unittest

from atc_tools.time import TimeSequence


class TimeTest(unittest.TestCase):
    def test_01_sequence(self):
        seq = TimeSequence("2021/01/10 14:23", delta_seconds=3600)

        t1 = seq.next()
        t2 = seq.next()
        t3 = next(seq)
        t4 = seq.reverse(2)

        self.assertEqual(t4,t1)
        self.assertEqual((t3-t2).total_seconds(), 3600)

