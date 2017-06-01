# NYC Insurance Open Data

Loads a NYC Public Record Database into a Mongo Database. Includes a full etl which archives and reloads data if changes are reported in the source.

Can also be used to stream data over RabbitMQ. Streamed messages are automatically inserted.

## Installation 

Fully installable with docker:

| Makefile  | docker |
| ------------- | ------------- |
|  `make launch-topology`  | `docker-compose up` |


Docker-compose will launch all the necessary databases and set the appropriate environment variables. If you would like to install locally, use python 3.5+ and run:

| Makefile | pip|
| ------------- | ------------- |
| `make install-local` | `pip install -r requirements.txt` |


This will install the required packages but you will still need to connect to mongodb and rabbitmq.

## Usage

To start running the topology call:

| Makefile | docker  | 
| ------------- | ------------- |
| `make launch-topology`  | `docker-compose up` |


Then to start a new ingestion call:

| Makefile | docker  | 
| ------------- | ------------- |
| `make full-data-load`  | `docker-compose run python ./main.py full` |

To stream messages set up a listener with:

| Makefile | docker  | 
| ------------- | ------------- |
| `make produce-stream`  | `docker-compose run python ./main.py consume` |

and then call the following to stream data to it:
| Makefile | docker  | 
| ------------- | ------------- |
| `make consume-stream`  | `docker-compose run python ./main.py produce` |


