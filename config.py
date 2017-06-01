import os

'''
All Configs use os.environ.get with sensible default values specified.
The default values are meant for local development, while any production instance should
have an appropriately provisioned server.
'''

#MESSAGE QUEUE CONFIGS
MESSAGE_QUEUE_ROUTE = os.environ.get('MESSAGE_QUEUE_ROUTE', 'facility_info')
MESSAGE_QUEUE_HOST = os.environ.get('MESSAGE_QUEUE_HOST', 'localhost')
MESSAGE_QUEUE_PORT = int(os.environ.get('MESSAGE_QUEUE_PORT', 5672))

#MONGO CONFIGS
MONGO_DB_HOST = os.environ.get('MONGO_DB_HOST', 'localhost:27016')
MONGO_DB_DB   = os.environ.get('MONGO_DB_DB', 'nyc_facilities')
MONGO_FACILITY_COLLECTION = os.environ.get('MONGO_FACILITY_COLLECTION', 'facilities')
MONGO_INGESTION_COLLECTION = os.environ.get('MONGO_INGESTION_COLLECTION', 'ingestions')

#MISC CONFIGS
NYC_DATA_URL = os.environ.get('NYC_DATA_URL', 'https://data.cityofnewyork.us/resource/8nqg-ia7v.json')
