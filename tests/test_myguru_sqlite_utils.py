#!/usr/bin/env python3
import unittest

import numpy as np
import pandas as pd
import sqlite3

import os
import sys
if os.path.dirname(sys.argv[0]):
    sys.path.append(os.path.abspath('./myguru-lib'))
else:
    sys.path.append(os.path.abspath('../myguru-lib'))

import myguru_sqlite_utils as db

class TestGuruUtils(unittest.TestCase):
    def test(self):
        tmp_db_name = 'tmp.sqlite3'

        self.assertFalse(db.is_db_initialized)
        self.assertIsNone(db.con)

        con_t = db._get_db_con(database_name=tmp_db_name)

        self.assertTrue(db.is_db_initialized)
        self.assertIsNotNone(db.con)
        self.assertIsInstance(db.con, sqlite3.Connection)

        self.assertIsInstance(con_t, sqlite3.Connection)
        self.assertIsNotNone(con_t.row_factory)
        self.assertTrue(callable(con_t.row_factory)) # check for <class 'function'>
        self.assertEqual(con_t.row_factory.__name__, '_dict_factory')



        # self.assertEqual(gu.get_new_bool_col_name('date_app_registration'), 'has_app_registration')

if __name__ == "__main__":
  unittest.main()
