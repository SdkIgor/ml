import os
import requests

from myguru_utils import humanize_order_desc, humanize_time_aftertoday, tomorrow_midday

MYGURU_AIMYLOGIC_TOKEN = os.environ.get("MYGURU_AIMYLOGIC_TOKEN")
MYGURU_MONEY_FUSE = os.environ.get("MYGURU_MONEY_FUSE")

class AimylogicException(Exception):
    pass

def start_calls(payload, phones, token=MYGURU_AIMYLOGIC_TOKEN, add_human_order_desc=True):
    '''
    Запускает реальный обзвон

    Возвращает массив с callJobId вида

    [
      { "callJobId": "5665334669", "phone": "79885851900" },
      ...
    ]
    '''
    url = "https://app.aimylogic.com/api/calls/campaign/"+token+"/addPhones"

    if add_human_order_desc:
        payload['human_order_desc_intro'] = humanize_order_desc(payload, "intro")
        payload['human_order_desc_details'] = humanize_order_desc(payload, "details")
        payload['human_order_desc_full'] = humanize_order_desc(payload, "full")

    request_body = []

    for phone in phones:
        request_body.append({ "phone": phone, "payload": payload })

    if MYGURU_MONEY_FUSE:
        raise AimylogicException('App is in MYGURU_MONEY_FUSE mode. If you need real calls please set env MYGURU_MONEY_FUSE=0')

    resp = requests.post(url, json = request_body )
    # print('Response object: ', resp)
    if resp.status_code == 200:
        # print('Plain response:', resp.json())
        callJobIds = resp.json()
        # return int(resp.json()[0])  # first job id, resp.json always like ['5665334669', '5665334671']
        result = []
        # print(type(callJobIds))
        # print(type(phones))
        for x, y in zip(phones, callJobIds):
            result.append({"phone": x, "callJobId": int(y)})
        return result
    else:
        resp.raise_for_status()

def test_single_call(payload, token=MYGURU_AIMYLOGIC_TOKEN):
    '''
        Делает тестовый звонок с сообщением информации о заказе на один конкретный номер
    '''
    url = "https://app.aimylogic.com/api/calls/campaign/"+token+"/addPhones"
    payload['human_order_desc_intro'] = humanize_order_desc(payload, "intro")
    payload['human_order_desc_details'] = humanize_order_desc(payload, "details")
    payload['human_order_desc_full'] = humanize_order_desc(payload, "full")
    request_body = [{ "phone": payload['phone'], "payload": payload }]
    resp = requests.post(url, json = request_body )
    if resp.status_code == 200:
        job_id = int(resp.json()[0])
        return job_id
    else:
        resp.raise_for_status()


def get_jobs_statuses(job_ids, token=MYGURU_AIMYLOGIC_TOKEN):
    '''
        Выводит по каждому номеру вот такие подробности

        [
          {
            "attemptsCount": 0,
            "callDuration": 0,
            "callJobId": "5665336642",
            "createdAt": 1691006924166,
            "finishedAt": null,
            "jobStatus": "planned",
            "phone": "79885851900",
            "redialNumber": null,
            "reportData": null,
            "tagName": null,
            "tagPayload": null
          },
          {
            "attemptsCount": 0,
            "callDuration": 0,
            "callJobId": "5665336644",
            "createdAt": 1691006924166,
            "finishedAt": null,
            "jobStatus": "planned",
            "phone": "79956251900",
            "redialNumber": null,
            "reportData": null,
            "tagName": null,
            "tagPayload": null
          }
          ...
        ]


    '''
    url = "https://app.aimylogic.com/api/calls/campaign/"+token+"/callJob/getReport"
    resp = requests.post(url, json = { "ids": job_ids })
    return resp.json()

def get_sample_groupcall_data():
    '''Тестовые данные для проверки групповых звонков'''
    sample_datetime = tomorrow_midday()
    sample_datetime_humanized = humanize_time_aftertoday(sample_datetime)
    call_task_data = {
        "work_name": "Уборка территории",
        "people_n": 3,
        "dt_start": sample_datetime,
        "dt_start_human": sample_datetime_humanized,
        "price_rub": 2000,
        "city_name": "Ростов-на-Дону",
        "address": "Будённовский проспект 45",
        "comment": "Убрать строительный мусор"
    }
    return call_task_data



