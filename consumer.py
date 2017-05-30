import json
from pymongo import MongoClient
from config import *
from queue import Queue


with MongoClient(host=MONGO_DB_HOST) as client:

    db = client.nyc_insurance_list

    def insert_record(ch, method, properties, body):
        '''
        Our callback function for received message,
        takes the body of the message and inserts it into the mongo database
        We could extend this to check for matches as well
        '''
        body = json.loads(body.decode("utf-8"), 'utf-8')
        db.institutions.insert_one(body)

if __name__ == '__main__':
    with Queue(MESSAGE_QUEUE_ROUTE, MESSAGE_QUEUE_HOST, MESSAGE_QUEUE_PORT) as q:
        q.consume(insert_record)












