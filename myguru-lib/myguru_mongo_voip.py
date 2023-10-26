import os
from pymongo import MongoClient

# Поменять флоу: добавлять в raw_request и логгировать только завершенные звонки

INSTANCE_URI = os.environ.get("MYGURU_MONGODB_URI") or "mongodb://root:20mongoroot23@webhook2mongo.linsec.dev:27017/"
DATABASE = os.environ.get("MYGURU_MONGODB_DEFAULT_DB") or "myguru"
COLLECTION = os.environ.get("MYGURU_MONGODB_DEFAULT_DB") or "aimylogic_webhooks"

client = MongoClient(INSTANCE_URI)
db = client[DATABASE]
collection = db[COLLECTION]

def get_all_dialogs_of_numbers(phones_arr):
    '''
    Получает ВСЕ диалоги по номерам телефона. Номер телефона должен начинаться с 7 (международный формат)

    Возвращает курсор

    Пример:
        cursor = get_all_dialogs_by_number([ 79885851900, 79956251900 ])
        for doc in cursor:
            print(doc)

        # or print(list(cursor))
    '''

    # rawRequest.caller, rawRequest.data.speech, rawRequest.data.text
    cursor = collection.find(
        { "rawRequest.caller": {"$in": phones_arr } },
        {
            "action": 1,
            "rawRequest.caller" : 1,
            # "rawRequest.data.speech": 1,
            # "rawRequest.data.speech[0].reply.text": 1,   # not working
            # "rawRequest.data.text": 1,
            "queryText" : 1
        }
    )
    return cursor


def get_last_dialog_raw(phone_str):
    '''
    Отбирает все ответы последнего дозвона до номера

    phone_str обязательно строка: не 79885851900, а "79885851900"

    Одним дозвоном считается всё что, между 'b-start' и 'b-end'

        [
            { 'action': 'b-start', ... },
            { 'action': 'q9_no', ... }, --> вот это вытаскивает
            ...
            { 'action': 'b-end', ... }
        ]

    Пример:
        x = get_last_dialog("79885851900")
    '''

    dialogs = list(get_all_dialogs_of_numbers([phone_str]))
    last_dialog_answers = []
    for i in reversed(dialogs):
        if i["action"] == 'b-end':
            continue
        if i["action"] == 'b-start':
            break
        del i["_id"]
        last_dialog_answers.append(i)

    print(last_dialog_answers)

    # TODO: process to ab_group_id, ab_question_id, result
    return last_dialog_answers

def parse_action(action_str, type=None):
    '''
        parse_action("q1_no"): { "question_id" : 1, "answer": False }
    '''

    x = action_str.split("_")

    question_id = x[0][-1]

    answer = False
    if x[1] == "yes":
        answer = True

    if type == "question_id":
        return question_id
    elif type == "answer":
        return answer
    else:
        return {
            "question_id" : question_id,
            "answer": answer
        }

def get_last_answers(phone_str, pretty=True):
    '''
    Получает последние ответы в человекопонятной форме
        [
            {
            "action": "q1_no",
            "queryText": "дело в том что я секретарь давайте я напишу что вы звонили и передам ваше сообщение",
            "rawRequest": {
                    "caller": "79885851900"
                }
            },
            ...
        ]
    '''
    dialog = get_last_dialog_raw(phone_str)

    if pretty == True:
        import myguru_ab
        qdf = myguru_ab.get_active_questions_df()

    result = []
    for x in dialog:
        y = {
            "question_id": parse_action(x["action"], "question_id"),
            "answer": parse_action(x["action"], "answer"),
        }

        if pretty == True:
            # y["question"] = next(item for item in q.to_numpy() if item["id"] == y["question_id"])
            # y["question"] = list(filter(lambda x: int(x["id"]) == int(y["question_id"]), q.to_numpy()))[0]["label"]
            y["question"] = qdf.loc[qdf['id'].astype('int64') == int(y["question_id"]) ]["label"].iloc[0]
            del y["question_id"]

            if y["answer"] == True:
                y["answer"] = "да"
            else:
                y["answer"] = "нет"

            y[ y["question"] ] = y["answer"]
            del y["question"]
            del y["answer"]

        result.append(y)


    return result
    # return { "status" : 1}

# print(x)
# cursor = collection.find({ "channelType": {"$in": [ "resterisk" ] } })
#     msg = ''
#
#     # if 'data' in doc['rawRequest'] and 'cause' in doc['rawRequest']['data']:
#     #     msg += doc['rawRequest']['data']['cause']
#
#     if 'data' in doc['rawRequest']:
#         msg += " -"
#         msg += doc['rawRequest']['data']['speech'][0]['reply']['text']
#         msg += " -"
#         msg += doc['queryText']
