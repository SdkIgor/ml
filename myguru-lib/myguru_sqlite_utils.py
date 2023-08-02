import sys
import json
import sqlite3
import myguru_utils as utils

'''
    Модуль для работы с базой данных обзвонов (для простоты и быстроты использует SQLite)
'''

this = sys.modules[__name__]
this.db_name = None
this.con = None
this.is_db_initialized = False

def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def _fix_sqlite_conn(con):
    '''
    returning "dictionary" rows after fetchall or fetchone.
    '''
    con.row_factory = _dict_factory
    return con

def init_con(database_name=this.db_name):
    '''
    Возвращает db connection (объект sqlite3.Connection)

    Автоматически инициализует базу данных при первом вызове
    '''
    this.con = sqlite3.connect(database_name, check_same_thread=False)
    if not this.is_db_initialized:
        this.con = _fix_sqlite_conn(con)
        this.con = _init_database(con)
        this.is_db_initialized = True
    return this.con

def _init_database(con=this.con):
    con.cursor().execute('''
       CREATE TABLE IF NOT EXISTS tasks(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           work_id INTEGER,
           work_name TEXT,
           city_id INTEGER,
           city_name TEXT,
           people_n INTEGER,
           price_rub INTEGER,
           address TEXT,
           dt_start TEXT,
           question_ids TEXT,
           comment TEXT,
           phones_order TEXT,
           callJobIds TEXT
       )''')
    con.commit()
    return con

def add_task(payload_dict, con=this.con):
    required_params = ['work_id', 'city_id', 'address', 'dt_start', 'people_n', 'price_rub']
    # aaditional_params = [ 'work_name', 'city_name']
    if 'question_ids' in payload_dict:
        payload_dict['question_ids'] = ', '.join(payload_dict['question_ids'])

    if(utils.check_keys(payload_dict, required_params)):
        columns = ', '.join(payload_dict.keys())
        placeholders = ':'+', :'.join(payload_dict.keys())
        query = 'INSERT INTO tasks (%s) VALUES (%s)' % (columns, placeholders)
        # print(query)
        # print(payload_dict['question_ids'])
        c = this.con.cursor()
        c.execute(query, payload_dict)
        this.con.commit()
        print('ROW', c.lastrowid)
        return c.lastrowid
    else:
        raise TypeError("Not all obligatory params provided for add_task")

def add_phones_order(order_id, phones_order_json, con=this.con):
    c = this.con.cursor()
    sql = 'UPDATE tasks SET phones_order = ? WHERE id = ?'
    c.execute(sql, (phones_order_json, order_id))
    this.con.commit()

def add_callJobIds(order_id, callJobIds_json, con=this.con):
    c = this.con.cursor()
    sql = 'UPDATE tasks SET callJobIds = ? WHERE id = ?'
    c.execute(sql, (callJobIds_json, str(order_id)))
    this.con.commit()


# order_id = callbot_task_id
def get_task(order_id, con=this.con):
    c = this.con.cursor()
    c.execute("SELECT * FROM tasks WHERE id=?", (order_id,),)
    row = c.fetchone()
    # print(row)
    row['dt_start_human'] = utils.humanize_time_aftertoday(row['dt_start'])

    for col in [ 'phones_order', 'callJobIds']:
        if row[col]:
            row[col] = json.loads(row[col])

    row['limit'] = int(row['people_n'])
    return row
