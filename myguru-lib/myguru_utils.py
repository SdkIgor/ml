import re
import sys
from math import isnan
from urllib.parse import urlparse
from datetime import date, datetime, timedelta
from babel.dates import format_date
import humanize

'''
    Специфичные для проекта методы для работы со стандартными типами данных (строки, dict)

'''

this = sys.modules[__name__]
this.default_dt_format = '%Y-%m-%dT%H:%M'


def check_keys(dictionary, key_list):
    for key in key_list:
        if (key not in dictionary) or (dictionary[key]==''):
            return False
    return True

def mark_confirm_and_result(comment):
    '''
    Функция для разметки полей confirm и result на основе поля comment
    Вход - строка, выход - dict
    '''
    result = { 'confirm': None, 'result': None }
    x = []

    if '/' in comment:
        x = re.split('/', comment)
    if '-' in comment:
        x = re.split('-', comment)

    if re.search('согласен', str(x[0]), re.IGNORECASE):
        result['confirm'] = 1
    else:
        result['confirm'] = 0

    if re.search('согласен', str(x[1]), re.IGNORECASE):
        result['result'] = 1
    elif re.search('не|автоотв|сброс|бросил|выкл| но|работаю уже|отказался', str(x[1]), re.IGNORECASE):
        result['result'] = 0
    elif (len(str(x[1]))<2):
        result['result'] = None  # строки типа 'Согласен/'
    else:
        result['result'] = 1

    return result


def is_sql_int(datatype_string):
    '''
    Функция для проверки является ли тип данных sql целочисленным типом
    Вход - строка, выход - 0 или 1
    '''
    p = re.compile('^int|integer|smallint|mediumint|bigint', re.IGNORECASE)
    if p.match(datatype_string):
        return 1
    else:
        return 0

def is_sql_str(datatype_string):
    '''
    Функция для проверки является ли тип данных sql строковым типом
    Вход - строка, выход - 0 или 1
    '''
    p = re.compile('^string|char|varchar|text|tinytext|mediumtext|longtext', re.IGNORECASE)
    if p.match(datatype_string):
        return 1
    else:
        return 0


def can_fixint(row):
    '''
    Определяет, нужно ли приводить колонку к целочисленным значениям после обработки отсутствующих значений через dropna/fillna
    Анализирует свойства column_type и datatype, как они получаются - см. функцию investigate_table и юнит-тест
    Вход - строка dataframe (pd.DataFrame), выход - 0 или 1
    Не должно влиять на ml_good_cols_list
    NB: при вызове ранее функции pd.convert_dtypes() может оказаться бесполезной
    '''
    if is_sql_int(row['column_type']) and row['nunique'] > 2 and row['datatype'].kind == 'f':
        return 1
    return 0

def can_fixbool(row):
    '''
    Определяет, нужно ли приводить колонку к бинарным значениям после обработки отсутствующих значений через dropna/fillna
    Анализирует свойства column_type и datatype, как они получаются - см. функцию investigate_table и юнит-тест
    Вход - строка dataframe (pd.DataFrame), выход - 0 или 1
    NB: при вызове ранее функции pd.convert_dtypes() может оказаться бесполезной
    '''

    # если в колонке всего два возможных значения - пустая строка и какая-то непустая
    if is_sql_str(row['column_type']) and row['nunique'] == 2 and row['datatype'].kind == 'O':
        return 1

    # если в колонке всего три возможных значения - пустая строка и две каких-то непустых
    # if is_sql_str(row['column_type']) and row['nunique'] == 3 and row['datatype'].kind == 'O':
    # 	return 1
    return 0

# def fixbool(row):
# 	if row['nunique'] == 2 and row['datatype'].kind == 'O':


# TODO: can fixdict (оценить соотношение total_rows/nunique)



def get_db_name(dsn):
    r = urlparse(dsn)
    return re.sub('/', '', r.path)

def get_srv_dsn(dsn):
    r = urlparse(dsn)
    return r.scheme + '://' + r.netloc

def leave_only_num(str):
    if str == '':
        return None
    fixed = re.sub("[^0-9]", "", str)
    return int(fixed)

def iso8601_to_days_from(date_string):
    '''
        Может быть полезна для обработки полей `user.date_viber_registration` и `user.date_app_registration`
    '''
    if ((date_string == '') | (date_string is None) | isnan(date_string)):
        return None #  min_val?
    datetime_object = datetime.fromisoformat(date_string)
    today = datetime.now()
    delta = today - datetime_object
    return delta.days

def unixtime_to_days_from(date_string, min_val=1635933110):

    if ((date_string == '') | (date_string is None) | isnan(date_string)):
        return None #  min_val?
    ts = int(date_string)
    datetime_object = datetime.utcfromtimestamp(ts) # datetime.datetime
    today = datetime.now()
    # today = date.today() # datetime.date
    delta = today - datetime_object
    # TypeError: unsupported operand type(s) for -: 'datetime.date' and 'datetime.datetime'
    return int(delta.days)


def get_new_bool_col_name(old_name_str):
    new_name = old_name_str
    if old_name_str.startswith("date_"):
        new_name = re.sub("date_", "", new_name)
    new_name = 'has_' + new_name
    return new_name


def humanize_time_aftertoday(dt_str):
    humanize.i18n.activate("ru_RU")
    dt_obj = datetime.strptime(dt_str, this.default_dt_format)
    time_delta_from_today = dt_obj.date() - date.today()
    weekday = format_date(dt_obj, format="EEEE", locale='ru_RU')
    time_part = dt_obj.strftime('%H:%M')
    result_str = ""

    if (humanize.naturaldate(dt_obj) == "сегодня"):
        result_str += "сегодня"
    elif (humanize.naturaldate(dt_obj) == "завтра"):
        result_str += "завтра"
    elif (time_delta_from_today.days == 2):
        result_str += "послезавтра"
        result_str += " (это " + weekday + ")"
    else:
        result_str += format_date(dt_obj, format="d MMMM", locale='ru_RU')
        result_str += " (это " + weekday + ")"
    result_str += " в "
    result_str += time_part

    time_delta_before_date = dt_obj - datetime.now()
    if (time_delta_before_date.total_seconds()/3600 < 24 ):
        tdiff_hh_mm = dt_obj - datetime.now()
        delta_before_string = humanize.precisedelta(tdiff_hh_mm, minimum_unit='minutes', format="%u")
        # tdiff_hh_mm is datetime.timedelta so can not use locale
        result_str += " (то есть через " + delta_before_string + ")"

    return result_str


def fix_comment(comment_str):
    return re.compile("^нужно", re.IGNORECASE).sub('', comment_str)

def humanize_order_desc(order_desc_dict, textgen_type):
    """
    Представляет информацию о заказе в человекопонятном виде
    Для звонков рекомендуется использовать режимы intro + details, а для чатов - full
    В идеале вызывать эту функцию в момент синтеза речи (т.к. вывод функции humanize_time_aftertoday зависит от времемни)
    Обязательные поля: work_name, city_name, address, dt_start, price_rub
    Опциональные поля: talker_name, comment
    """
    text = ""
    available_types = [ "intro", "details", "full"]  # full = intro + details

    if ((textgen_type == "intro") or (textgen_type == "full")):
        if "talker_name" in order_desc_dict:
            text += order_desc_dict["talker_name"] + ", "
        text += "здравствуйте! "
        if "talker_name" not in order_desc_dict:
            text = text.capitalize()
        text += "Это сервис МайГуру. "
        text += "Предлагаем вам новый заказ из категории " + order_desc_dict["work_name"] + "."

    if ((textgen_type == "details") or (textgen_type == "full")):
        if "comment" in order_desc_dict:
            text += "Нужно " + fix_comment(order_desc_dict["comment"]) + ". "
        text += "Адрес: город " + order_desc_dict["city_name"] + " " + order_desc_dict["address"] + ". "
        text += "На адресе нужно быть " + humanize_time_aftertoday(order_desc_dict["dt_start"]) + ". "
        text += "Оплата за работу " + str(order_desc_dict["price_rub"]) + " рублей. "

    return text

def tomorrow_midday():
    '''
    Всегда возвращает строку со временем завтрашнего полудня
    Возвращает в формате this.default_dt_format
    Используется для тестовых целей
    '''
    x = datetime.today()
    x = x.replace(hour=0, minute=0, second=0, microsecond=0)
    x += timedelta(days=1, hours=12)
    return x.strftime(this.default_dt_format)

