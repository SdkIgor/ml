import os
import copy
import warnings
import pandas as pd
import collections.abc
from df_utils import get_df_meta
from myguru_utils import can_fixint, can_fixbool, get_srv_dsn, get_db_name, leave_only_num, iso8601_to_days_from, unixtime_to_days_from, get_new_bool_col_name


'''
Специфичные фунции для работы с данными CRM MyGuru
Функции общего назначения вынесены в guru_utils
'''

CRM_DB_DSN = os.environ.get("MYGURU_CRM_DB_DSN") or 'mysql+pymysql://pavel:rL0eW8pC7ekX7z@185.149.241.75/crm'
CRM_DB_NAME = get_db_name(CRM_DB_DSN)
CRM_SRV_DSN = get_srv_dsn(CRM_DB_DSN)

def get_dsn():
    return CRM_DB_DSN

# Exploratory Data Analysis
# old name: investigate_table
# TODO: validation of table_name/sql_query/csv/df params (can use them simultaneously)
# TODO: extract table from query param
# TODO: support of queries with SQL JOINS
# TODO: autofix suggestion
# TODO: fix 'if table_name is not None:' construction repeating
# TODO: use https://github.com/AutoViML/AutoViz
def analyse_dataset(dsn=CRM_DB_DSN, table_name=None, sql_query=None, csv=None, df=None, sort_by=None, debug=False):
    '''
    Возвращает два свойства - data (сами данные таблицы) и meta (метаданные про таблицу)
    Оба свойства - объекты pandas.DataFrame

    Входные параметры:

        table_name - имя таблицы для автоматического анализа

        dsn - dsn до базы данных, содержащей интересующую таблицу

        sql_query - кастомный SQL SELECT (например, если нужно проанализировать только часть таблички)

        csv - имя csv файла с датасетом

        df - объет pandas dataframe с датасетом

        print - напечатать результаты в консоли в человекочитаемом виде (таблица Markdown)

        sort_cols - способ соритировки колонок в человекочитаемом результате (может быть percent_missing, nunique, datatype )

        debug - выводить промежуточные логи обработки

    Для каждой колонки таблицы данный метод подсчитывает следующие метаданные:

        column_name, percent_missing, nunique и datatype - как в df_utils.get_df_meta()

        column_type - тип колонки SQL

        fixint - флаг что данные возможно привести к типу int (см. can_fixint из guru_utils)

        fixbool - флаг что данные возможно привести к типу bool (см. can_fixbool из guru_utils)

    Примеры использования функции:

        # Если нужно проанализировать данные из csv
        db.analyse_dataset(csv='neuro/enriched.csv', sort_by='datatype', debug=True)

        # Если нужно проанализировать данные из базы
        # q = 'SELECT * FROM user WHERE status = 10'
        # db.analyse_dataset(sql_query=q, table_name='user', sort_by='nunique', debug=True)

    '''

    # 'процент разнообразия =
    # nunique/rows_total


    percent_missing_threshold = 90 # макимально норм % пропущенных данных в столбце для обучающей выборки.

    if sql_query is not None:
        df = pd.read_sql(sql_query, dsn)

    if table_name is not None and df is None:
        df = pd.read_sql_table(table_name, dsn)

    if csv is not None:
        df = pd.read_csv(csv)

    df = df.convert_dtypes()

    stat_df = get_df_meta(df)

    metrics = {
        "rows": {},
        "ml_ok_cols": {},
        "ml_maybe_ok_cols": {},
        "ml_bad_cols": {},
    }

    metrics['rows']['total'] = len(df.index)
    metrics['rows']['with_na'] = df.isna().any(axis=1).sum() # строк с как минимум одним Na
    metrics['rows']['after_dropna'] = metrics['rows']['total'] - metrics['rows']['with_na']

    if table_name is not None:
        sch_df = pd.read_sql(
            'SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = %(table_schema)s AND table_name = %(table_name)s',
            CRM_SRV_DSN + '/information_schema',
            params={ 'table_schema': CRM_DB_NAME, 'table_name': table_name }
        )
        stat_df = pd.merge(stat_df, sch_df, how='inner', on='column_name')
        stat_df['fixint'] = stat_df.apply (lambda row: can_fixint(row), axis=1)
        stat_df['fixbool'] = stat_df.apply (lambda row: can_fixbool(row), axis=1)

    # TO-DO: add 'reason_type' field for each bad columns
    ok_cols_df = stat_df.loc[stat_df['datatype'] != 'object']
    ok_cols_df = ok_cols_df.loc[ok_cols_df['percent_missing'] < percent_missing_threshold]
    ok_cols_df = ok_cols_df.loc[(ok_cols_df['nunique'] > 1) & (ok_cols_df['percent_missing'] > 0)]
    ok_cols_df = ok_cols_df.loc[ok_cols_df['nunique'] < df.shape[0]]

    metrics['ml_ok_cols']['df'] = ok_cols_df
    metrics['ml_ok_cols']['list'] = metrics['ml_ok_cols']['df']['column_name'].tolist()
    metrics['ml_ok_cols']['total'] = len( metrics['ml_ok_cols']['list'] )

    not_bad_fields_list = metrics['ml_ok_cols']['list']

        # Ищем колонки, которые могут быть потенциально пригодными для нейросети
        # (по ним лучше принимать решения вручную)
    if table_name is not None:
        dfx = stat_df[~stat_df.column_name.isin(metrics['ml_ok_cols']['list'])]
        dfx = dfx.loc[(dfx['fixbool'] == 1) | (dfx['fixint'] == 1)]

        metrics['ml_maybe_ok_cols']['df'] = dfx
        metrics['ml_maybe_ok_cols']['list'] = dfx['column_name'].tolist()
        metrics['ml_maybe_ok_cols']['total'] = len( dfx['column_name'].tolist() )

        not_bad_fields_list += metrics['ml_maybe_ok_cols']['list']

    metrics['ml_bad_cols']['df'] = stat_df[~stat_df.column_name.isin(not_bad_fields_list)]
    metrics['ml_bad_cols']['list'] = metrics['ml_bad_cols']['df']['column_name'].tolist()
    metrics['ml_bad_cols']['total'] = len( metrics['ml_bad_cols']['list'] )

    if sort_by is not None:
        df_to_sort = [ metrics['ml_ok_cols']['df'], metrics['ml_bad_cols']['df'] ]
        if table_name is not None:
            df_to_sort += [ metrics['ml_maybe_ok_cols']['df'] ]
        for dfi in df_to_sort:
            dfi = dfi.sort_values(by=sort_by)

    # Выводим результаты в человекочитаемом виде
    if debug:
        metrics_to_print = copy.deepcopy(metrics)
        for k in ['ml_ok_cols', 'ml_maybe_ok_cols', 'ml_bad_cols']:
            metrics_to_print[k].pop('list', None)
            metrics_to_print[k].pop('df', None)
        print(metrics_to_print)
        print("\n")

        print_config = [
            { 'df':metrics['ml_ok_cols']['df'], 'label': '=> Columns GOOD for ML: ' },
            { 'df': metrics['ml_bad_cols']['df'], 'label': '=> Columns BAD for ML: '},
        ]

        if table_name is not None:
            x = { 'df': metrics['ml_maybe_ok_cols']['df'], 'label': '=> Columns MAYBE GOOD for ML: '}
            print_config.append(x)

        for i in print_config:
            # TO-DO: fix TypeError: list indices must be integers or slices, not str
            if isinstance(i['df'], pd.DataFrame):
                total_rows = i['df'].shape[0]
                i['label'] += str(total_rows)
            print(i['label'])
            print(i['df'].to_markdown())
            print("\n")

    # metrics
    return {
        'data': df,
        'meta': stat_df,
        'metrics': metrics
        }

# TO-DO: check if col is in x['metrics']['ml_ok_cols']['list']
def do_autofix_for_ml(df, dropnan=None):
    '''
        Выполняет вручную коррекцию данных базы для ml

        Оставляет в dataframe только колонки из таблички user, указанные fix_user_cnf

        Прописал вручную на основе просмотра запросов SELECT DISTINCT (и get_users_data_df(df)['metrics']['ml_ok_cols']['list'])

        TODO: Вторая итерация после просмотра корреляционной матрицы

        Значения типов

            asbool - NaN=0, other values=bool, column name = old_column_name

            drop - не учитывать в ML

            str2int - удалить все нечисловые символы и привести к типу int (специально для колонок типа price_per_hour)
    '''
    # possible types: bool, int, float, drop
    fix_user_cnf = [
           { 'col_name': 'user_id' },
           { 'col_name': 'password_hash', 'fix_type': 'asbool' },
           { 'col_name': 'email', 'fix_type': 'asbool' },
           { 'col_name': 'first_name', 'fix_type': 'drop' },
           { 'col_name': 'last_name', 'fix_type': 'drop' },
           { 'col_name': 'patronymic', 'fix_type': 'drop' },
           { 'col_name': 'user_phone2', 'fix_type': 'asbool' },
           { 'col_name': 'price_per_hour', 'fix_type': 'str2int' },
           { 'col_name': 'user_description', 'fix_type': 'asbool' },
           { 'col_name': 'conditions', 'fix_type': 'asbool' },
           { 'col_name': 'description', 'fix_type': 'asbool' },
           # { 'col_name': 'gender', 'fix_type': 'gender' }, # TODO
           { 'col_name': 'gender', 'fix_type': 'drop' },
           { 'col_name': 'education', 'fix_type': 'asbool' },
           { 'col_name': 'experience', 'fix_type': 'asbool' },
           { 'col_name': 'buy_premium', 'fix_type': 'asbool' },
           { 'col_name': 'has_brigade', 'fix_type': 'asbool' },
           { 'col_name': 'experience_years', 'fix_type': 'asbool' }, # validate if it's not more than age
           { 'col_name': 'id_how_did_know' }, # it's OK by sense to leave NaN
           { 'col_name': 'premium_description', 'fix_type': 'asbool' }, # возможно зависит от buy_premium
           { 'col_name': 'foto', 'fix_type': 'asbool' },
           { 'col_name': 'viber', 'fix_type': 'asbool' },
           { 'col_name': 'app_token_new', 'fix_type': 'asbool' },
           { 'col_name': 'notification_sound', 'fix_type': 'asbool' }, # NaN = default ?
           # { 'col_name': 'app_version' }, # TODO: enum dict
           # { 'col_name': 'app_last_visit', 'fix_type': 'unixtime' },
           { 'col_name': 'app_last_visit_ip', 'fix_type': 'drop' },
           # { 'col_name': 'date_viber_registration', 'fix_type': 'iso8601' },
           # { 'col_name': 'date_app_registration', 'fix_type': 'iso8601' },
           { 'col_name': 'date_viber_registration', 'fix_type': 'asbool' },
           { 'col_name': 'date_app_registration', 'fix_type': 'asbool' },
           { 'col_name': 'device_info', 'fix_type': 'asbool' }, # зависит от app_version и app_last_visit
           # { 'col_name': 'date', 'fix_type': 'unixtime' }, # непонятно что это
           { 'col_name': 'terms_of_pay', 'fix_type': 'asbool' },
           { 'col_name': 'android_id', 'fix_type': 'asbool' },
    ]

    allowed_cols = [ i['col_name'] for i in fix_user_cnf ]
    # print('ALLOWED', allowed_cols)
    df = df[allowed_cols]

    all_cols = df.columns.tolist()

    for col_meta in fix_user_cnf:
        if col_meta['col_name'] in all_cols and 'fix_type' in col_meta:
            if col_meta['fix_type'] == 'asbool':
                old_col_name = col_meta['col_name']
                new_col_name = get_new_bool_col_name(old_col_name)
                print(old_col_name)
                df[new_col_name] = df[old_col_name].apply(lambda x: True if (pd.isna(x) | (x != '')) else False)
                df[new_col_name] = df[new_col_name].astype('bool')
                df = df.drop(old_col_name, axis=1)
                # df[new_field_name] = df[col_meta['field']].fillna(0)

            if col_meta['fix_type'] == 'unixtime': # values like 1686845940
                old_col_name = col_meta['col_name']
                new_col_name = 'days_from_' + str(col_meta['col_name'])
                df[new_col_name] = df[old_col_name].apply(lambda x: unixtime_to_days_from(x))
                df = df.drop(old_col_name, axis=1)

            if col_meta['fix_type'] == 'iso8601': # mask like 2021-11-15 10:00:25
                old_col_name = col_meta['col_name']
                new_col_name = 'days_from_' + str(col_meta['col_name'])
                df[new_col_name] = df[old_col_name].apply(lambda x: iso8601_to_days_from(x))
                df = df.drop(old_col_name, axis=1)

            if col_meta['fix_type'] == 'str2int':
                df[col_meta['col_name']] = df[col_meta['col_name']].apply(lambda x: leave_only_num(x))

            if col_meta['fix_type'] == 'drop':
                df = df.drop(col_meta['col_name'], axis=1)

    # drop all columns that are not in fix_user_cnf


    return df





# dry-run mode
# def autofix_for_ml
# Прописываем конфиг какие колонки по смыслу можно dropna, а какие fillna
# Алгоритм:
# if (can_fixint), config(dropna) и потери не более 10% даных - dropna sint


# get_user_* - для получения данных по одному пользователю (для реалтаймовой обработки, например по событию в RabbitMQ)
# get_users_* - для получения данных сразу по нескольким пользователям (обычно более эффективно при пакетной обработке)

def get_user_id_by_phone(phone_str):
    '''
    Возвращает id пользователя по его номеру телефона
    Учитывает ТОЛЬКО активных юзеров (WHERE status = 10)

    Примеры:
        get_user_id_by_phone(89260264488)
        get_user_id_by_phone('89260264488')

    Если в базе по этому номеру зарегистировано несколько пользователей - покажет warning и вернёт первое значение
    '''
    phone_str = str(phone_str)
    df = pd.read_sql('SELECT id FROM user WHERE status = 10 AND user_phone = '+ phone_str, CRM_DB_DSN)

    if len(df)==0:
        warnings.warn('User with phone '+ phone_str + ' not found')
        return None

    if len(df)>1:
        many_user_ids = df['id'].tolist()
        many_user_ids_str = str(', '.join(map(str, many_user_ids)))
        warn_msg = 'More than one user has phone ' + phone_str + ' listed: ' + many_user_ids_str + '. Return id of first founded user'
        warnings.warn(warn_msg)
    return df['id'][0]

def _get_user_orders_stat_by_id(id_user):
    '''
        Возвращает dict вида {'orders_via_app': 880, 'orders_via_viber': 33, 'orders': 1075, 'orders_other_channels': 162}
    '''
    id_user = str(id_user)
    query = 'SELECT count(*) orders_via_app FROM app_order WHERE status = 2 and id_user = ' + id_user + ' GROUP BY id_user'
    x1 = pd.read_sql(query, CRM_DB_DSN).to_dict(orient='records')[0] # {'orders_via_app': n}
    query = 'SELECT count(*) orders_via_viber FROM viber_order WHERE status = 2 and id_user = ' + id_user + ' GROUP BY id_user'
    x2 = pd.read_sql(query, CRM_DB_DSN).to_dict(orient='records')[0] # {'orders_via_viber': n}
    query = 'SELECT orders FROM users_statictic WHERE user_id = '+ id_user
    x3 = pd.read_sql(query, CRM_DB_DSN).to_dict(orient='records')[0]
    orders_other_channels = int(x3['orders']) - int(x2['orders_via_viber']) - int(x1['orders_via_app'])
    x4 = { "orders_other_channels": orders_other_channels }
    x = {**x1, **x2, **x3, **x4} # Python 3.5 or greater
    return x

def get_user_data_by_phone(phone_str):
    '''
    Возвращает данные пользователя по его номеру телефона
    Данные из табличек user и users_statictic

    TODO: более умная проверка на status
    '''
    phone_str = str(phone_str)
    query = 'SELECT * FROM user LEFT JOIN users_statictic ON user.id = users_statictic.user_id WHERE status = 10 AND user_phone = ' + phone_str
    query += ' LIMIT 1'
    df = pd.read_sql(query, CRM_DB_DSN)

    if len(df)==0:
        warnings.warn('User with phone '+ phone_str + ' not found')
        return None

    return df.to_dict(orient='records')[0]

def get_users_id_df(phones_arr, only_active_users=False):
    '''
    Возвращает pandas.DataFrame с id пользователей, соответствующих их номерам

    Пример:
        get_users_df(['89885851900', '89260264488'])

    Делает то же, что и get_user_id_by_phone только для нескольких номеров

    TODO: По умолчанию возвращает только поля user_phone и id, но можно опционально передать список полей таблички user, которые нужно вернуть
    TODO: Поиск по 7 вместо 8 в номере
    TODO: additional_fields=None
    '''

    if isinstance(phones_arr, pd.core.series.Series):
        phones_arr = phones_arr.astype('string')
        phones_arr = phones_arr.tolist()

    if isinstance(phones_arr, collections.abc.Sequence):
        phones_arr = [str(i) for i in phones_arr]

    query = 'SELECT user_phone, id user_id FROM user WHERE user_phone IN (' + ', '.join(phones_arr)  +')'
    if only_active_users:
        query += ' AND status = 10'
    df = pd.read_sql(query, CRM_DB_DSN)
    # df['user_id'] = df['user_id'].astype(int)

    # Если номеров нет в базе - для единообразия всё равно возвращаем pandas.Dataframe с заполненным user_phone
    if df.empty:
        df = pd.DataFrame(phones_arr, columns=['user_phone'])

    return df

def add_user_ids(df, only_active_users=False):
    '''
    Находит id пользователя по номеру телефона и добавляет колонку с id в конец DataFrame
    '''
    phone_column_name='user_phone'
    dfx = get_users_id_df(df[phone_column_name])

    # Fix 'ValueError: You are trying to merge on int64 and object columns. If you wish to proceed you should use pd.concat'
    df[phone_column_name] = df[phone_column_name].astype('string')
    dfx[phone_column_name] = dfx[phone_column_name].astype('string')

    # dfx['user_id'] = dfx['user_id'].astype(int)

    # df = df.drop(columns=['id_x', 'id_y'])
    return pd.merge(df, dfx, how='left', on=phone_column_name)

def get_users_stat(df, id_column='user_id'):
    ids_arr_str = df[id_column].dropna().astype(int).astype('string').tolist()
    ids_arr_str = ', '.join(ids_arr_str)
    query = 'SELECT * FROM users_statictic WHERE user_id IN (' + ids_arr_str +')'
    df = pd.read_sql(query, CRM_DB_DSN)
    return df

def add_user_stat(df, id_column='user_id'):
    dfs = get_users_stat(df)
    return pd.merge(df, dfs, how='left', on=id_column)

def get_users_data_df(df, id_column='user_id', autofix_for_ml=False):
    ids_arr_str = df[id_column].dropna().astype(int).astype('string').tolist()
    ids_arr_str = ', '.join(ids_arr_str)
    query = 'SELECT * FROM user WHERE id IN (' + ids_arr_str +')'
    df = pd.read_sql(query, CRM_DB_DSN)
    df = df.rename(columns={"id": id_column})
    if autofix_for_ml:
        df = do_autofix_for_ml(df)
    return df

def add_users_data_df(df, id_column='user_id', autofix_for_ml=False):
    dfs = get_users_data_df(df, autofix_for_ml=autofix_for_ml)
    return pd.merge(df, dfs, how='left', on=id_column)


def get_app_orders_df(user_ids_arr, new_user_id_column='user_id'):
    '''Подсчитывает сколько выполенных заказов из приложения у пользователя, на основе таблички app_order'''
    user_ids_arr_fixed = [str(i) for i in user_ids_arr]
    user_ids_arr_str = ', '.join(user_ids_arr_fixed)
    # If no row qualifies, then the result of COUNT is 0 (zero), and the result of any other aggregate function is the null value.
    query = 'SELECT id_user '+new_user_id_column+', count(*) orders_via_app FROM app_order WHERE status = 2 and id_user IN (' + user_ids_arr_str +') GROUP BY id_user'
    return pd.read_sql(query, CRM_DB_DSN)


def add_completed_orders_stat(df, id_column='user_id'):
    '''
    Добавляет в датасет статистику по выполенным заказам из различных каналов
    (поля orders_via_app, orders_via_viber и вычисляемый orders_other_channels)
    '''

    ids_arr_str = df[id_column].dropna().astype(int).astype('string').tolist()
    ids_arr_str = ', '.join(ids_arr_str)

    query = 'SELECT id_user '+id_column+', count(*) orders_via_app FROM app_order WHERE status = 2 and id_user IN (' + ids_arr_str +') GROUP BY id_user'
    df_app_orders = pd.read_sql(query, CRM_DB_DSN)

    query = 'SELECT id_user '+id_column+', count(*) orders_via_viber FROM viber_order WHERE status = 2 and id_user IN (' + ids_arr_str +') GROUP BY id_user'
    df_viber_orders = pd.read_sql(query, CRM_DB_DSN)

    # df[id_column] = df[id_column].astype('string')
    # df_app_orders[id_column] = df[id_column].astype('string')
    df = pd.merge(df, df_app_orders, how='left', on=id_column)
    df = pd.merge(df, df_viber_orders, how='left', on=id_column)
    df[['orders_via_app', 'orders_via_viber']] = df[['orders_via_app', 'orders_via_viber']].fillna(value=0).astype(int)

    df['orders_other_channels'] = df['orders'] - df['orders_via_viber'] - df['orders_via_app']
    return df

def get_id_name_pairs(table_name):
    '''
    Получает из таблички table_name все пары id-name

    Выход: массив dict

    Пример:
    SELECT id, name FROM order_works
    SELECT id, name FROM city
    '''
    df = pd.read_sql("SELECT id, name FROM "+table_name, get_dsn())
    return df.set_index('id').to_dict()['name']

# TODO: add experience
def get_potential_workers(params_dict, ignore_limit=True):
    '''
        Пример тестовых данных: order_works_id=7 (Сантехника), city_id=2 (Воронеж)
    '''
    sql_query = r"""SELECT user.id user_id, user.last_name, user.first_name, user.user_phone, us.useful orders_completed FROM user
                LEFT JOIN user_works on user_works.user_id=user.id
                LEFT JOIN users_statictic us on us.user_id=user.id
                WHERE user_works.order_works_id = {0} AND user.city_id = {1}
                """.format(params_dict['work_id'], params_dict['city_id'])
    df = pd.read_sql(sql_query, get_dsn())
    df = df.convert_dtypes()
    return df