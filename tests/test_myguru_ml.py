#!/usr/bin/env python3
import unittest

import numpy as np
import pandas as pd

import os
import sys
if os.path.dirname(sys.argv[0]):
    sys.path.append(os.path.abspath('./myguru-lib'))
else:
    sys.path.append(os.path.abspath('../myguru-lib'))

import myguru_ml as mlu

class TestGuruUtils(unittest.TestCase):
    def test(self):

        def _test_trynorm():
            tdict = {
                "full_transcript": "some strings",
                "detaled": [
                    {"phrase": "phrase1", "negative_sentiment": 0.17, "gender": "male"},
                    {"phrase": "phrase2", "negative_sentiment": 0.0, "gender": "female"},
                ]
            }
            tdf = mlu._trynorm(tdict)
            self.assertEqual(['full_transcript', 'detaled'], tdf.columns.tolist())
            

        _test_trynorm()

if __name__ == "__main__":
  unittest.main()
