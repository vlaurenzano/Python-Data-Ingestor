from queue import Queue
import urllib.request, json
import datetime
from config import *

if __name__ == '__main__':
    with urllib.request.urlopen(NYC_DATA_URL) as url:
        last_modified = url.headers['Last-Modified']
        dt = datetime.datetime.strptime(last_modified, "%a, %d %b %Y %X %Z")
        data = json.loads(url.read().decode())
        with Queue(MESSAGE_QUEUE_ROUTE, MESSAGE_QUEUE_HOST, MESSAGE_QUEUE_PORT) as q:
            for message in data:
                q.publish(json.dumps(message))





