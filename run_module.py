import pandas as pd
import pprint
import pymongo
from bson.objectid import ObjectId
import utils
import os
import spacy
import time

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
    t1 = time.time()
    print('STEP 1')
    #First
    document = getDocument(doc_id)
    clf = utils.loadData('/var/www/html/scrapper/PCGA/models/topmodel95.41.pickle')
    work_document = []
    for account in document[0]['data']:
        data_aux = []
        aux = []
        aux.append(account['name'])
        aux.append(account['assets'])
        aux.append(account['liabilities'])
        aux.append(account['lost'])
        aux.append(account['gain'])
        data_aux.append(aux)
        
        df = pd.DataFrame(data_aux)
        df.columns = ['text', 'val5', 'val6', 'val7', 'val8']
        df = utils.cleanData(df)
        account["clean_text"] = df['text'][0]

        nlp = spacy.load('es')
        doc = nlp(df['text'][0].strip().lower())
        df['text'] = doc.vector_norm
      
        aux = []
        for row in df.iterrows():
            aux.append(row[1]['text'])
            aux.append(utils.numberToBinary(row[1]['val5']))
            aux.append(utils.numberToBinary(row[1]['val6']))
            aux.append(utils.numberToBinary(row[1]['val7']))
            aux.append(utils.numberToBinary(row[1]['val8']))

        predicted = clf.predict([aux])

        account["group"] = predicted[0]
        work_document.append(account)
    t2 = time.time()
    print('PREDICTIONS ==> '+str(t2-t1/60)+' MINUTES')
    
    for account in work_document:
        group = int(float(account['group']))
        text = account['clean_text'].strip()
        doc = nlp(text)
        new_text = ''

        for token in doc:
            if (len(token.lemma_) <= 1):
                pass

            else:
                new_text = new_text+' '+str(token.lemma_)

        account['clean_text'] = new_text.strip()

        if (group == 1):
            df = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-act-tokens.csv')
            print(1)

        elif (group == 2):
            df = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-pas-tokens.csv')
            print(2)
        elif (group == 3):
            df = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-pat-tokens.csv')
            print(3)
        elif (group == 4):
            df = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-eerr-tokens.csv')
            print(4)

    print('--1 CHECK--')

if __name__ == '__main__':
    print('--HOLI :) --')
    run(os.sys.argv[1])