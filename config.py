import os

MESSAGE_QUEUE_ROUTE = os.environ.get('MESSAGE_QUEUE_ROUTE', 'insurance_info')
MESSAGE_QUEUE_HOST = os.environ.get('MESSAGE_QUEUE_HOST', 'localhost')
MESSAGE_QUEUE_PORT = int(os.environ.get('MESSAGE_QUEUE_PORT', 5672))
MONGO_DB_HOST = os.environ.get('MONGO_DB_HOST', 'localhost:27016')
NYC_DATA_URL = os.environ.get('NYC_DATA_URL', 'https://data.cityofnewyork.us/resource/8nqg-ia7v.json')
