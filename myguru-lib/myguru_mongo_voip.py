import os
from pymongo import MongoClient

# Поменять флоу: добавлять в raw_request и логгировать только завершенные звонки

INSTANCE_URI = os.environ.get("MYGURU_MONGODB_URI") or "mongodb://root:20mongoroot23@webhook2mongo.linsec.dev:27017/"
DATABASE = os.environ.get("MYGURU_MONGODB_DEFAULT_DB") or "myguru"
COLLECTION = os.environ.get("MYGURU_MONGODB_DEFAULT_DB") or "aimylogic_webhooks"

client = MongoClient(INSTANCE_URI)
db = client[DATABASE]
collection = db[COLLECTION]

number = "79885851900"
# rawRequest.caller, rawRequest.data.speech, rawRequest.data.text
cursor = collection.find(
    { "rawRequest.caller": {"$in": [ number ] } },
    {
        "action": 1,
        "rawRequest.caller" : 1,
        "rawRequest.data.speech": 1,
        # "rawRequest.data.speech[0].reply.text": 1,
        # "rawRequest.data.text": 1,
        "queryText" : 1
    }
)
# cursor = collection.find({ "channelType": {"$in": [ "resterisk" ] } })


import pprint
pp = pprint.PrettyPrinter(indent=2)

for doc in cursor:
    # print(doc)
    # pp.pprint(doc)


    # print(doc['action'])
    # print(type(doc['action']))
    msg = ''

    # if 'data' in doc['rawRequest'] and 'cause' in doc['rawRequest']['data']:
    #     msg += doc['rawRequest']['data']['cause']

    if 'data' in doc['rawRequest']:
        msg += " -"
        msg += doc['rawRequest']['data']['speech'][0]['reply']['text']
        msg += " -"
        msg += doc['queryText']

        # CAUSE не подходит, он везде ок
        # msg += ' '
        # msg += doc['rawRequest']['data']['cause']
        # msg += doc['rawRequest']['data']['speech'][0]['cause']

    # next loggeed action
    msg += ' -> next:'
    msg += doc['action']
    msg += ' '

    print(msg)




# print("Search finished")

# rawRequest.caller "79885851900"