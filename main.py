#!/usr/bin/env python
from lib.services import *
from lib.ingest_data import run_ingestion, ingest_one
import argparse

def full_ingestion():
    print('Running full ingestion')
    f, i = get_mongo_collections()
    run_ingestion(f, i)

def produce():
    data, _ = get_data_from_source()
    print('Publishing data from source...' )
    with get_message_queue() as q:
        for message in data:
            print('Publishing message...', message)
            q.publish(json.dumps(message))

def consume():
    f, i = get_mongo_collections()
    def cb(ch, method, properties, body):
        body = json.loads(body.decode("utf-8"), 'utf-8')
        print('Message received, ingesting...', body)
        ingest_one(f, i, body)
    with get_message_queue() as q:
        q.consume(cb)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest and stream data from NYC open data set")
    parser.add_argument('mode', help='the mode to run in: full, produce, consume')
    args = parser.parse_args()

    if args.mode == 'full':
        full_ingestion()
    elif args.mode == 'produce':
        produce()
    elif args.mode == 'consume':
        consume()
    else:
        print(parser.print_help())







