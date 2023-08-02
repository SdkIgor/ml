from numpy import nan # fix NameError: name 'nan' is not defined

import pandas as pd
import myguru_crm as db
from myguru_utils import mark_confirm_and_result

'''Обертка над pd.read_json'''
def _tryread(filename, debug=True):
    try:
        if debug:
            print('Reading file '+filename)
        return pd.read_json(filename)
    except (FileNotFoundError) as e:
        print(e)
        return None

'''Обертка над pd.json_normalize'''
def _trynorm(dict, subprop_name):
    try:
        return pd.json_normalize(dict[subprop_name])
    except:
        return None

'''
Читает json, полученный из STT сервиса Tinkoff, вида

{ "full_transcript": "...",
  "detaled": [
    {"phrase": "здравствуйте", "negative_sentiment": 0.0, "gender": "male"},
    ...,
    ]
}

и из массива detaled получает средний и максимальный уровень негатива мастера

Внимание! Принадлежность реплики к мастеру определяется только оценкой пола говорящего

В тестовом опросе общались с мастерами всегда женщины.
Но иногда они говорили грубо и хабалисто, так что в отдельных случаях определялись как мужчины :(
Однако на общий скоринг это существенно не влияет, т.к. таких случаев немного, не более 10%
'''

def add_emotional_scores(df, audio_transcript_base_dir='audio', user_phone_col='user_phone'):
    df[user_phone_col] = df[user_phone_col].astype('string')
    df['audio_script_json']=df[user_phone_col].apply(lambda x: _tryread(audio_transcript_base_dir + '/' + x + '.mp3.json'))
    df['dialog_df'] = df['audio_script_json'].apply( lambda j: _trynorm(j, 'detaled') )
    print(df['dialog_df'])
    df['neg_sent_master_max'] = df['dialog_df'].apply( lambda d: d.loc[ d["gender"] == "male", ["negative_sentiment"] ].squeeze().max() if (d is not None) else None)
    df['neg_sent_master_avg'] = df['dialog_df'].apply( lambda d: d.loc[ d["gender"] == "male", ["negative_sentiment"] ].squeeze().mean() if (d is not None) else None)
    # df['neg_sent_operator_max'] = df['dialog_df'].apply( lambda d: d.loc[ d["gender"] == "female", ["negative_sentiment"] ].squeeze().max() )
    # df['neg_sent_operator_avg'] = df['dialog_df'].apply( lambda d: d.loc[ d["gender"] == "female", ["negative_sentiment"] ].squeeze().mean() )
    df = df.drop(columns=['audio_script_json', 'dialog_df'])
    return df

def preprocess_df(df,
    user_id_col='user_id',
    user_phone_col='user_phone',
    result_csv_filename='fixed.csv',
    save_csv=False,
    debug=False,
    only_active_users=False,    # select only active users
    rmu_no_stat=False,          # remove users with users_statictic = NaN
    convert_dtypes=False,
    head_log_n=3
    ):
    '''
        Подготавливает DataFrame, содержащий данные обзвона, для машинного обучения

        Единственное обязательное требование к исходному DataFrame - наличие колонки user_id или user_phone,
        по которым будет осуществлён поиск информации о пользователей в CRM

        Пример исходного DataFrame:

            df = pd.DataFrame({
             "user_id": { "0":test_ids[0], "1":test_ids[1],   "2":test_ids[2],  "3":test_ids[3] },
             "ab_group_id": { "0":1,    "1":2,   "2":3, "3":2 },
             "ab_question_id": { "0":1, "1":4, "2":1,  "3":7 },
             })
    '''
    if debug:
        print("\n")
        print("--> STAGE 0. Read original dataset:")
        df.info()
        print(df.head(head_log_n))

    if 'result_comment' in df.columns:
        df['result'] = df['result_comment'].apply(lambda x: mark_confirm_and_result(x)['result'])
        df = df[df['result'].notna()]
        df['result'] = df['result'].astype('bool')

    # fields_to_leave = ['user_phone', 'vertical', 'ab_group_id', 'ab_question_id', 'result']
    fields_to_leave = ['user_phone', 'ab_group_id', 'ab_question_id', 'result']
    if set(fields_to_leave).issubset(df.columns.to_list()):
        df = df[fields_to_leave]
        if debug:
            print('Leave only columns: '+str(','.join(fields_to_leave)))

    if debug:
        print("\n")
        print('--> STAGE 1/5. Original dataset autolabeled:')
        df.info()
        print(df.head(head_log_n))

    if user_phone_col in df.columns and user_id_col not in df.columns:
        df = db.add_user_ids(df, only_active_users=only_active_users)
        if only_active_users and debug:
            drop_count = df[ df[colx].isna() ].shape[0]
            log_msg = "Total numbers that ML engine can not match with ACTIVE guru user_id" + str(drop_count)
            print(log_msg)
        if only_active_users:
            df = df[df[user_id_col].notna()]

        if debug:
            print("\n")
            print('--> STAGE 2/5 (optional). Users ids added:')
            df.info()
            print(df.head(head_log_n))
        # df[user_id_col] = df[user_id_col].astype('int')

    if user_id_col in df.columns:

        df = db.add_user_stat(df, id_column=user_id_col)
        if rmu_no_stat:
            if debug:
                drop_count = df.shape[0] - df.dropna().shape[0]
                print("Items with no user statistic: " + str(drop_count))
            df.dropna(inplace=True)
        if debug:
            print("\n")
            print('--> STAGE 3/5. Users statistics added:')
            df.info()
            print(df.head(head_log_n))

        df = db.add_completed_orders_stat(df)
        if debug:
            print("\n")
            print('--> STAGE 4/5. Info about completed orders added:')
            df.info()
            print(df.head(head_log_n))

        df = db.add_users_data_df(df, autofix_for_ml=True)
        if debug:
            print("\n")
            print('--> STAGE 5/5. General user info added:')
            df.info()
            print(df.head(head_log_n))

    df = df.convert_dtypes()

    # Exclude useless columns
    # df = df.drop(columns=['user_phone', 'orders' ], errors='ignore')
    df = df.drop(columns=['orders'], errors='ignore')


    if debug:
        print("\n")
        print('--> RESULT DATASET:')
        df.info()
        print(df.head(head_log_n))

    if save_csv:
        df.to_csv(result_csv_filename, sep=',', encoding='utf-8', index=False)

    return df
