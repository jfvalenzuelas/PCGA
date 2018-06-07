import pandas as pd
import pprint
import pymongo
from bson.objectid import ObjectId
import utils
import os
import spacy
import time
import analysis
import threading

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
    print('RUNNING PCGA')
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
    print('PREDICTIONS ==> '+str(t2-t1)+' seconds')

    file_name = doc_id+'.xlsx'
    analysis.copy_rename(file_name)

    print('--TEMPLATE COPIED')
    print('--ANALYSIS BEGIN--')

    t1 = time.time()

    thread1 = threading.Thread( target=analysis.matchCellPCGA, args=(work_document, 1, file_name) )
    thread2 = threading.Thread( target=analysis.matchCellPCGA, args=(work_document, 2, file_name) )
    thread3 = threading.Thread( target=analysis.matchCellPCGA, args=(work_document, 3, file_name) )
    thread4 = threading.Thread( target=analysis.matchCellPCGA, args=(work_document, 4, file_name) )     

    thread1.setDaemon(True)
    thread2.setDaemon(True)
    thread3.setDaemon(True)
    thread4.setDaemon(True)
    
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()

    t2 = time.time()
    print('EXCEL CELLS ==> '+str(t2-t1)+' seconds')

    print('--ANALYSIS END--')
    print('--ALL DONE --')

if __name__ == '__main__':
    print('--HOL :) --')
    run(os.sys.argv[1])