import pandas as pd
import pprint
import pymongo
from bson.objectid import ObjectId
import utils
import os
import spacy

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
    print('STEP 1')
    #First
    document = getDocument(doc_id)
    data = []
    clf = utils.loadData('/var/www/html/scrapper/PCGA/models/topmodel95.41.pickle')
    data_aux = []
    for account in document[0]['data']:
        aux = []
        aux.append(account['name'])
        print(account['assets'])
        aux.append(account['assets'])
        aux.append(account['liabilities'])
        aux.append(account['lost'])
        aux.append(account['gain'])
        data_aux.append(aux)
        
        df = pd.DataFrame(data_aux)
        df.columns = ['text', 'val5', 'val6', 'val7', 'val8']
        df = utils.cleanData(df)

        nlp = spacy.load('es')
        doc = nlp(df['text'][0].strip().lower())
        df['text'] = doc.vector_norm

        print(df)
        
        aux = []
        for row in df.iterrows():
            aux.append(row[1]['text'])
            aux.append(utils.numberToBinary(row[1]['val5']))
            aux.append(utils.numberToBinary(row[1]['val6']))
            aux.append(utils.numberToBinary(row[1]['val7']))
            aux.append(utils.numberToBinary(row[1]['val8']))

        predicted = clf.predict([aux])

        account['group'] = predicted[0]
    
    print(document[0]['data'])

    print('--1 CHECK--')

if __name__ == '__main__':
    print('--HOLI :) --')
    run(os.sys.argv[1])