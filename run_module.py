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
        
        df = pd.DataFrame(data)
        df.columns = ['text', 'val5', 'val6', 'val7', 'val8']
        df = utils.cleanData(df)
        
        wordvec = []
        nlp = spacy.load('es')
        
        for x in df['text']:
            doc = nlp(x.strip().lower())
            vector = doc.vector_norm
            wordvec.append(vector)

        df_copy = df[:]
        df_copy['text'] = wordvec

        data_copy = []
        for row in df_copy.iterrows():
            aux = []
            aux.append(row[1]['text'])
            aux.append(utils.numberToBinary(row[1]['val5']))
            aux.append(utils.numberToBinary(row[1]['val6']))
            aux.append(utils.numberToBinary(row[1]['val7']))
            aux.append(utils.numberToBinary(row[1]['val8']))
            data_copy.append(aux)
        
        print(data_copy)
        print('--1 CHECK--')

    except:
        pass

if __name__ == '__main__':
    print('--HOLI :) --')
    run(os.sys.argv[1])