import json
from pymongo import MongoClient
from config import *
import urllib.request
import datetime


def init_db(db):
    '''
    Initializes the collections in our new db
    :param db: Mongo Db
    '''
    print('Initializing Database')
    db.drop_collection('meta') #drop it first since it was created when we checked for it \_()_/ mongo life
    db.create_collection("meta", capped= True, size=client.max_bson_size, max=1)
    db.drop_collection('institutions')

def load_db(db, data, dt):
    '''
    Loads all our data into the database using mongo's batch insert
    :param db: Mongo Db
    :param data: All the json data we'll be loading in
    :param dt: A python datetime object
    '''
    print('Loading Database')
    bulk = db.institutions.initialize_ordered_bulk_op()
    for record in data:
        bulk.insert(record)
    result = bulk.execute()
    print(result)
    db.meta.insert_one({'last_modified': dt})

def roll_db(client, dt):
    '''
    Saves a snapshot of our database to a timestamped collection
    This may or may not be necessary depending on 1) backup strategy 2) Data usefulness
    :param client: Mongo Client
    :param dt: A python datetime object
    '''
    print('Archiving existing data')
    db_name = dt.strftime('nyc_insurance_list_%Y%m%d%X')
    client.drop_database(db_name)
    client.admin.command('copydb', fromdb='nyc_insurance_list', todb=db_name)



if __name__ == '__main__':
    with urllib.request.urlopen(NYC_DATA_URL) as url:

        #get the last modified time of the url
        json_mod_dt = datetime.datetime.strptime(url.headers['Last-Modified'], "%a, %d %b %Y %X %Z")

        with MongoClient(host=MONGO_DB_HOST) as client:

            db = client.nyc_insurance_list
            meta = db.meta.find_one()

            #check to see if we've already loaded this dataset, if so let's not reload
            if not meta:
                init_db(db)
                load_db(db, json.loads(url.read().decode()), json_mod_dt)

            #if our data is stale reload it
            elif meta['last_modified'] < json_mod_dt:
                roll_db(client, meta['last_modified'])
                init_db(db)
                load_db(db, json.loads(url.read().decode()), json_mod_dt)
            else:
                print('No new data to load')







