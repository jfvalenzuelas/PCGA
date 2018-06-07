import pandas as pd
import pprint
import pymongo
from bson.objectid import ObjectId
import utils
import os

def getDocument(id):
    client = pymongo.MongoClient('localhost', 27654)
    #print('CLIENTE: SUCCESS')
    db = client['scrapper']
    #print('BD: SUCCESS')
    coll = db.PCGA
    #print('COLLECTION: SUCESS')

    cursor = coll.find({'_id':ObjectId(id)})
    return cursor

def run(doc_id):
    try:
        print('STEP 1')
        #First
        document = getDocument(doc_id)
        data = []

        for account in document[0]['data']:
            aux = []
            aux.append(account['name'])
            aux.append(account['assets'])
            aux.append(account['liabilities'])
            aux.append(account['lost'])
            aux.append(account['gain'])
            data.append(aux)

        df = pd.DataFrame(data[0])
        df.columns = ['text', 'val5', 'val6', 'val7', 'val8']
        df = utils.cleanData(df)
        print(df)
        print('--1 CHECK--')

    except:
        pass

if __name__ == '__main__':
    print('--HOLI :) --')
    run(os.sys.argv[1])