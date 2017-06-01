import datetime
from binascii import crc32
from pymongo import TEXT
from lib.services import *

def run_ingestion(facility_collection, ingestion_collection, get_data_fn=get_data_from_source):
    '''
    Runs a full ingestion if there has been an update do the data source
    :param facility_collection: Our Mongo Collection for the Facilities
    :param ingestion_collection: Our Mongo Collection for tracking injections
    :param get_data_fn: Our get data function, responsible for returning a list of data and a date time
    '''
    data, dt = get_data_fn()
    ingestion = get_last_ingestion(ingestion_collection)
    if not ingestion or ingestion['last_modified'] < dt:
        insert_ingestion(ingestion_collection, dt)
        ingestion = get_last_ingestion(ingestion_collection)
        ingest_data(facility_collection, data, ingestion['_id'])
    else:
        print('Endpoint has not been modified, no updates to process')

def ingest_data(collection, data, ingestion_id):
    '''
    Loads all our data into the database using mongo's batch insert
    :param db: Mongo Db
    :param data: All the json data we'll be loading in
    :param dt: A python datetime object
    '''
    print('Ingesting data to database')
    ensure_indexes(collection)
    bulk = collection.initialize_unordered_bulk_op()
    for record in data:
        record = prepare_for_ingestion(record, ingestion_id)
        bulk.find({'checksum':  record['checksum']}).upsert().update({"$set" : record})
    result = bulk.execute()
    print(result)
    result = collection.update({"ingestion_id": { "$ne": ingestion_id}}, {"$set": {"active": False}})
    print(result)

def ensure_indexes(collection):
    '''
    Make sure we have the proper indexes on our collection
    :param collection: Our Mongo Collection
    '''
    collection.create_index('checksum', unique=True)
    collection.create_index([('name_1',TEXT), ('name_2', TEXT), ('street_1', TEXT), ('street_2', TEXT), ('city', TEXT), ('zip', TEXT)],name="text_search_index")


def prepare_for_ingestion(record, ingestion_id):
    '''
    Append additional information to our data
    :param record: a dictionary containing our data
    :param ingestion_id: the ingestion id to append
    :return: record: a dictionary containing our data
    '''
    crc = generate_checksum(record)
    record['checksum'] = crc
    record['ingestion_id'] = ingestion_id
    record['active'] = True
    return record

def ingest_one(facility_collection, ingestion_collection, record):
    '''
    Ingests one record using the last ingestion id
    It must be present in the next full load or it wil be deactivate
    It's used for streaming updates
    :param facility_collection: the collection to update
    :param ingestion_collection: the collection to update
    :param record: our record to update the collection
    :return:
    '''
    ingestion = get_last_ingestion(ingestion_collection)
    if not ingestion:
        insert_ingestion(ingestion_collection, datetime.datetime.utcnow())
        ingestion = get_last_ingestion(ingestion_collection)
    record = prepare_for_ingestion(record, ingestion['_id'])
    facility_collection.update({'checksum':  record['checksum']},record, upsert=True)


def generate_checksum(d):
    '''
    Generates a checksum of our data. This may not work for nested data at this time
    :param d: dictionary
    :return: a crc32 checksum of the provided data
    '''
    items = bytes(str(sorted(d.items())), 'utf-8')
    return crc32(items)

def insert_ingestion(collection, dt):
    '''
    :param collection: Our mongo ingestion collection
    :param dt: The last modified date time from the server, we can choose whether to rely on this later
    :return: mongo result
    '''
    return collection.insert_one({"last_modified": dt, "inserted": datetime.datetime.utcnow()})

def get_last_ingestion(collection):
    '''
    :param collection: Our mongo ingestion collection
    :return: A mongo document if found
    '''
    return collection.find_one({}, sort=[("$natural", -1)])












