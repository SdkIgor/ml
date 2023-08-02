#!/usr/bin/env python3
import unittest

from freezegun import freeze_time

import numpy as np
import pandas as pd

import os
import sys
if os.path.dirname(sys.argv[0]):
    sys.path.append(os.path.abspath('./myguru-lib'))
else:
    sys.path.append(os.path.abspath('../myguru-lib'))

import myguru_utils as gu

class TestGuruUtils(unittest.TestCase):
    def test(self):
        self.assertTrue(hasattr(gu, 'mark_confirm_and_result'))
        self.assertTrue(hasattr(gu, 'is_sql_int'))
        self.assertTrue(hasattr(gu, 'can_fixint'))
        self.assertTrue(hasattr(gu, 'get_db_name'))

        self.assertEqual(gu.mark_confirm_and_result('Согласен/'), { 'confirm': 1, 'result': None })
        self.assertEqual(gu.mark_confirm_and_result('согласен-не помнит'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('согласен-автоответчик'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ бросил трубку'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ но'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ 3 раза звонила не взял трубку'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ тел не доступен'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ не выход'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('согласен-не получится'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/не доступен'), { 'confirm': 1, 'result': 0 })
        self.assertEqual(gu.mark_confirm_and_result('Согласен/ выход'), { 'confirm': 1, 'result': 1 })
        self.assertEqual(gu.mark_confirm_and_result('согласен-согласен'), { 'confirm': 1, 'result': 1 })
        self.assertEqual(gu.mark_confirm_and_result('согласен-согласен,я просто забыла запись включить,извинилась ,сказала  заявка отменена'), { 'confirm': 1, 'result': 1 })

        self.assertTrue(gu.is_sql_int('int(11)'))
        self.assertTrue(gu.is_sql_int('INT(5)'))
        self.assertTrue(gu.is_sql_int('INTEGER'))
        self.assertTrue(gu.is_sql_int('smallint(4)'))
        self.assertTrue(gu.is_sql_int('MEDIUMINT(10)'))
        self.assertTrue(gu.is_sql_int('bigint(11)'))
        self.assertFalse(gu.is_sql_int('STRING'))
        self.assertFalse(gu.is_sql_int('BOOL'))

        d = { 'datatype': [ np.dtype(float) ], 'column_type': [ 'int(11)' ], 'nunique': [ 3 ] }
        row = pd.DataFrame(d).iloc[0]
        self.assertTrue(gu.can_fixint(row))

        d = { 'datatype': [ np.dtype(object) ], 'column_type': [ 'varchar(255)' ], 'nunique': [ 25 ] }
        row = pd.DataFrame(d).iloc[0]
        self.assertFalse(gu.can_fixint(row))

        d = { 'datatype': [ np.dtype(int) ], 'column_type': [ 'int(3)' ], 'nunique': [ 100 ] }
        row = pd.DataFrame(d).iloc[0]
        self.assertFalse(gu.can_fixint(row))

        dsn = 'mysql+pymysql://user:password@192.168.0.1/database'
        self.assertEqual(gu.get_db_name(dsn), 'database')

        self.assertEqual(gu.leave_only_num(''), None)
        self.assertEqual(gu.leave_only_num('550 р /ч'), 550)
        self.assertEqual(gu.leave_only_num('   1000'), 1000)
        self.assertEqual(gu.leave_only_num('1000 рублей'), 1000)

        self.assertEqual(gu.get_new_bool_col_name('date_app_registration'), 'has_app_registration')
        self.assertEqual(gu.get_new_bool_col_name('user_phone2'), 'has_user_phone2')

        @freeze_time("2023-07-30 12:00:00")
        def _test_humanize_time_aftertoday():
            x = gu.humanize_time_aftertoday('2023-07-30T17:00')
            y = 'сегодня в 17:00 (то есть через 5 часов)'
            self.assertEqual(x,y)

            x = gu.humanize_time_aftertoday('2023-07-31T17:00')
            y = 'завтра в 17:00'
            self.assertEqual(x,y)


        def _test_humanize_order_desc():

            test_io_intro = [
                {
                    "input":{
                        "work_name": "Caнтехника"
                    },
                    "output": "Здравствуйте! Это сервис МайГуру. Предлагаем вам новый заказ из категории Caнтехника."
                },
                {
                    "input":{
                        "work_name": "Caнтехника",
                        "talker_name": "Павел",
                    },
                    "output": "Павел, здравствуйте! Это сервис МайГуру. Предлагаем вам новый заказ из категории Caнтехника."
                }
            ]

            for i in test_io_intro:
                x = gu.humanize_order_desc(i['input'], textgen_type='intro')
                self.assertEqual( x, i['output'] )

            # test_payload_1 = {
            #     "work_name": "Caнтехника",
            #     "city_id": 2,
            #     "city_name": "Воронеж",
            #     "people_n": 3,
            #     "dt_start": "2023-06-23 17:00",
            #     "address": "Лизюкова 4",
            #     "price_rub": 3000,
            #     "comment": "Ремонт теплотрассы",
            # }
            #
            # x = gu.humanize_order_desc(test_payload_1, textgen_type='intro')
            # print(x)
            #
            # test_payload_1["talker_name"] = "Павел"


        _test_humanize_time_aftertoday()
        _test_humanize_order_desc()


if __name__ == "__main__":
  unittest.main()
