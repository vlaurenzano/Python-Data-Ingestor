# NYC Insurance Open Data

Loads a NYC Public Record Database into A Mongo Database. Includes a full etl which archives and reloads data if changes are reported in the source.

Can also be used to stream data over RabbitMQ. Streamed messages are automatically inserted.

## Installation 

Fully installable with docker:

`docker-compose up`


## Usage

Once running call the following to perform the full etl:

`docker-compose run python python full_etl.py`

Or call the following to start a consumer:

`docker-compose run python python consumer.py`

and then call the following to stream data to it:

`docker-compose run python python consumer.py`



