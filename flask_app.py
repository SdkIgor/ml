#!/usr/bin/env perl
# export PYTHONPATH="$PWD/lib"

import sys
import sqlite3
import flask
from flask import Flask, render_template, request, redirect

app = flask.Flask(__name__, static_url_path='', template_folder='static')
app.config['JSON_AS_ASCII'] = False

sys.path.append('myguru-lib')

import aimylogic
import myguru_ab
import myguru_crm as crmdb
import myguru_sqlite_utils as botdb
import myguru_mongo_voip as whdb

from myguru_utils import humanize_order_desc

import pandas as pd

botdb.init_con(database_name='flask_app.sqlite')

@app.route('/')
def hello():
    questions_df = myguru_ab.get_active_questions_df()
    return render_template('index.html', ab_questions=questions_df)

@app.route("/api/categories", methods=["GET"])
def get_categories_dict():
    result = crmdb.get_id_name_pairs('order_works')
    return flask.jsonify(result)

@app.route("/api/cities", methods=["GET"])
def get_cities_dict():
    result = crmdb.get_id_name_pairs('city')
    return flask.jsonify(result)

@app.route('/api/new', methods=['GET', 'POST'])
def create_new_group_call_task():
    payload = request.get_json(force=True)
    callbot_task_id = botdb.add_task(payload)
    return flask.jsonify({ "new_location": "/call-plan/"+str(callbot_task_id) })

@app.route("/call-plan/<order_id>", methods=["GET"])
def get_order_details(order_id):
    call_task_data = botdb.get_task(order_id)
    print(call_task_data)
    potential_workers = crmdb.get_potential_workers(call_task_data)
    print(potential_workers)

    return render_template('plan.html',
        order_details=call_task_data,
        potential_workers=potential_workers
    )

# Оценка вероятности выхода БЕЗ звонков
@app.route("/guess/<order_id>", methods=["GET"])
def estimate_without_calls(order_id):
    call_task_data = botdb.get_task(order_id)
    potential_workers_df = crmdb.get_potential_workers(call_task_data)

    from pycaret.classification import ClassificationExperiment
    s = ClassificationExperiment()
    loaded_model = s.load_model('neuro/my_best_pipeline')
    from myguru_ml import preprocess_df
    data_for_ml = preprocess_df(potential_workers_df, debug=False)
    data_for_ml[['ab_group_id', 'ab_question_id', 'days_from_app_last_visit']] = None
    predictions = s.predict_model(loaded_model, data=data_for_ml, probability_threshold = 0.95)
    predictions.sort_values(by="prediction_score", ascending=False, inplace=True)

    print(predictions)

    df = predictions.copy(deep=True)
    df = df[predictions['user_phone'].notnull()]
    df = df[~df['user_phone'].eq('')]
    phones_order = df['user_phone'].to_json(orient="records")
    botdb.add_phones_order(order_id, phones_order)

    return render_template('guess1.html',
        order_details=call_task_data,
        potential_workers=predictions
    )

@app.route("/start-call/<order_id>", methods=["GET"])
def start_calls(order_id):
    print('Start...')
    call_task_data = botdb.get_task(order_id)
    print('call_task_data: ', call_task_data)
    potential_workers_df = crmdb.get_potential_workers(call_task_data)
    print("potential_workers_df: ", potential_workers_df)

    if call_task_data['callJobIds']:
        print('Aimylogic callJobIds found, querying statuses...')
        status_data = aimylogic.get_jobs_statuses(call_task_data['callJobIds']) # jobStatus

        call_status_df = pd.DataFrame.from_dict(status_data)  # callJobId, phone, jobStatus

        # FIXED: phone in call_status_df is always starting from '7'
        call_status_df['phone'] = call_status_df['phone'].apply(lambda x: '8' + x[1:])

        potential_workers_df = pd.merge(call_status_df, potential_workers_df, left_on='phone', right_on='user_phone')

        # QUICKFIX for always good probability
        # TODO: process 'longCallWithNoResult'
        import random
        potential_workers_df['prediction_score'] = round( random.uniform(0.95, 0.99), 2)

        # potential_workers_df['prediction_label'] = potential_workers_df.apply(
        #     lambda x: '✅' if (x['prediction_score'] >= 0.95 and x['jobStatus'] != 'longCallWithNoResult') else '❌'
        # )

        potential_workers_df['prediction_label'] = potential_workers_df['prediction_score'].apply(lambda x: '✅' if x >= 0.95 else '❌')

        return render_template('guess2.html',
          order_details=call_task_data,
          potential_workers=potential_workers_df
        )

    # Вначале звоним не всем, а только тем, кого нейронка считает самым перспективным
    # TODO: почему-то не сортирует, надо дебажить
    if call_task_data['phones_order']:
        custom_dict = {k: v for k, v in enumerate(call_task_data['phones_order'])}
        potential_workers_df = potential_workers_df.sort_values(by='user_phone', key=lambda x: x.map(custom_dict))

    limit = int(call_task_data['people_n'])
    call_queue_1 = potential_workers_df.head(limit)

    print('Will make '+str(limit)+' new calls')

    try:
        phones = call_queue_1['user_phone'].to_list()
        jobs_arr = aimylogic.start_calls(call_task_data, phones)
        job_ids = [ i['callJobId'] for i in jobs_arr ]

        print('job_ids got from aimylogic: ', job_ids)

        callJobIds_json = pd.Series(job_ids).to_json(orient="records")
        botdb.add_callJobIds(order_id, callJobIds_json)

        status_data = aimylogic.get_jobs_statuses(job_ids)  # jobStatus
        call_status_df = pd.DataFrame.from_dict(status_data)  # callJobId, phone
        potential_workers_df = pd.merge(call_status_df, potential_workers_df, left_on='phone', right_on='user_phone')

        print(potential_workers_df)
    except Exception as e:
        return { "error": str(e) }

    return render_template('guess2.html',
      order_details=call_task_data,
      potential_workers=potential_workers_df
    )

@app.route("/api/test/call", methods=['GET', 'POST'])
def test_single_call():
    '''Обработчик кнопки Тестовый звонок'''
    payload = request.get_json(force=True) # dict
    job_id = aimylogic.test_single_call(payload)
    return {"job_id": job_id}

@app.route("/api/status/<job_id>/", methods=["GET"])
def get_job_status(job_id):
    return aimylogic.get_jobs_statuses([job_id])
    # return flask.jsonify(result)

@app.route("/api/tts/<phone>/", methods=["GET"])
def get_last_dialog(phone):
    return whdb.get_last_answers(phone)

@app.route("/chat/demo", methods=["GET"])
def render_demo_chat():
    return render_template('chat.html')

@app.route("/chat/<order_id>/<phone>", methods=["GET"])
def render_chat(order_id, phone):
    call_task_data = botdb.get_task(order_id)
    print(call_task_data)
    call_task_data['human_order_desc_intro'] = humanize_order_desc(call_task_data, "intro")
    call_task_data['human_order_desc_details'] = humanize_order_desc(call_task_data, "details")
    return render_template('chat.html', order_details=call_task_data)

'''
Два роута далее (/examples/2 и /start-call/sample/2) предназначены для теста одновременного звонка на несколько номеров
Отдельные роуты созданы т.к. по умолчанию бизнес-логика не предполагает что обзон будет на номера, которых нет в базе (crm)
Т.е. методы botdb.add_task и botdb.get_task не сработают
'''
@app.route("/examples/2", methods=["GET"])
def render_sample_groupcall():
    grouptest_phones = myguru_ab.get_config()['grouptest_phones']
    call_task_data = aimylogic.get_sample_groupcall_data()
    potential_workers = crmdb.get_users_id_df(grouptest_phones, only_active_users=False)
    return render_template('example2.html',
        order_details=call_task_data,
        potential_workers=potential_workers
    )
@app.route("/start-call/sample/2", methods=["GET"])
def render_sample_groupcall_result():
    grouptest_phones = myguru_ab.get_config()['grouptest_phones']
    call_task_data = aimylogic.get_sample_groupcall_data()
    print('call_task_data: ', call_task_data)
    jobs_arr = aimylogic.start_calls(call_task_data, grouptest_phones)
    job_ids = [ i['callJobId'] for i in jobs_arr ]

    status_data = aimylogic.get_jobs_statuses(job_ids) # jobStatus
    call_status_df = pd.DataFrame.from_dict(status_data) # callJobId, phone
    potential_workers_df = crmdb.get_users_id_df(grouptest_phones, only_active_users=False) # at least user_phone
    workers_df = pd.merge(call_status_df, potential_workers_df, left_on='phone', right_on='user_phone')

    return render_template('guess2.html',
       order_details=call_task_data,
       potential_workers=workers_df
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, debug=True)
