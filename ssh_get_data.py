from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint
import configparser

def getData():
    config = configparser.ConfigParser()
    config.read('config.ini')

    MONGO_HOST = config['MONGODB']['MONGO_HOST']
    MONGO_DB = config['MONGODB']['MONGO_DB']
    MONGO_USER = config['MONGODB']['MONGO_USER']
    MONGO_PASS = config['MONGODB']['MONGO_PASS']

    REMOTE_HOST = config['REMOTE']['REMOTE_HOST']
    REMOTE_PORT = config['REMOTE']['REMOTE_PORT']

    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=MONGO_USER,
        ssh_password=MONGO_PASS,
        remote_bind_address=(REMOTE_HOST, int(REMOTE_PORT))
    )
    #print('-Iniciating Server-')
    server.start()
    #print('-SUCCESS-')
    client = pymongo.MongoClient(REMOTE_HOST, server.local_bind_port) # server.local_bind_port is assigned local port
    db = client[MONGO_DB]
    coll = db.PCGA

    cursor = coll.find({'processed':0})
    items = cursor.count()
    if (items > 0):
        documents = []
        for c in cursor:
            documents.append(c)
        #print('-Stopping Server-')
        server.stop()
        #print('SUCCESS-')
        return documents
    else:
        #print('-Stopping Server-')
        server.stop()
        #print('SUCCESS-')
        return False

def updateDocument(title):
    config = configparser.ConfigParser()
    config.read('config.ini')

    MONGO_HOST = config['MONGODB']['MONGO_HOST']
    MONGO_DB = config['MONGODB']['MONGO_DB']
    MONGO_USER = config['MONGODB']['MONGO_USER']
    MONGO_PASS = config['MONGODB']['MONGO_PASS']

    REMOTE_HOST = config['REMOTE']['REMOTE_HOST']
    REMOTE_PORT = config['REMOTE']['REMOTE_PORT']

    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=MONGO_USER,
        ssh_password=MONGO_PASS,
        remote_bind_address=(REMOTE_HOST, int(REMOTE_PORT))
    )
    #print('-Iniciating Server-')
    server.start()
    #print('-SUCCESS-')
    client = pymongo.MongoClient(REMOTE_HOST, server.local_bind_port) # server.local_bind_port is assigned local port
    db = client[MONGO_DB]
    coll = db.PCGA

    coll.update_one({'title': title}, {'$set': {'processed': 1}})
    server.stop()
    
def getDoc(title):
    config = configparser.ConfigParser()
    config.read('config.ini')

    MONGO_HOST = config['MONGODB']['MONGO_HOST']
    MONGO_DB = config['MONGODB']['MONGO_DB']
    MONGO_USER = config['MONGODB']['MONGO_USER']
    MONGO_PASS = config['MONGODB']['MONGO_PASS']

    REMOTE_HOST = config['REMOTE']['REMOTE_HOST']
    REMOTE_PORT = config['REMOTE']['REMOTE_PORT']

    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=MONGO_USER,
        ssh_password=MONGO_PASS,
        remote_bind_address=(REMOTE_HOST, int(REMOTE_PORT))
    )
    #print('-Iniciating Server-')
    server.start()
    #print('-SUCCESS-')
    client = pymongo.MongoClient(REMOTE_HOST, server.local_bind_port) # server.local_bind_port is assigned local port
    db = client[MONGO_DB]
    coll = db.PCGA

    cursor = coll.find({'title':title})
    items = cursor.count()
    if (items > 0):
        documents = []
        for c in cursor:
            documents.append(c)
        #print('-Stopping Server-')
        server.stop()
        #print('SUCCESS-')
        return documents
    else:
        #print('-Stopping Server-')
        server.stop()
        #print('SUCCESS-')
        return False