from config import *
from pymongo import MongoClient
import urllib
import datetime
import json
from message_queue import MessageQueue

_mongo_db = None

def get_mongo_client():
    '''
    Returns our mongo client singleton
    :return: Mongo Client
    '''
    global _mongo_db
    if _mongo_db is None:
        with MongoClient(host=MONGO_DB_HOST) as client:
            _mongo_db = client
    return _mongo_db

def get_mongo_collections():
    '''
    :return: our mongo collections for facilities and ingestions
    '''
    client = get_mongo_client().get_database(MONGO_DB_DB)
    return client.get_collection(MONGO_FACILITY_COLLECTION), client.get_collection(MONGO_INGESTION_COLLECTION)

def get_data_from_source():
    '''
    Returns the data from NYC's open data as well as the time it was last modified from it's header
    :return:
    '''
    with urllib.request.urlopen(NYC_DATA_URL) as url:
        #get the last modified time of the url
        json_mod_dt = datetime.datetime.strptime(url.headers['Last-Modified'], "%a, %d %b %Y %X %Z")
        data = json.loads(url.read().decode())
        return data, json_mod_dt


def get_message_queue(route=MESSAGE_QUEUE_ROUTE, host=MESSAGE_QUEUE_HOST, port=MESSAGE_QUEUE_PORT):
    '''
    Get our connection to RabbitMQ
    :return: Message Queue
    '''
    return MessageQueue(route, host, port)
