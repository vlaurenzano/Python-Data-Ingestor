# NYC Insurance Open Data

Loads a NYC Public Record Database into A Mongo Database. Includes a full etl which archives and reloads data if changes are reported in the source.

Can also be used to stream data over RabbitMQ. Streamed messages are automatically inserted.

## Installation 

Fully installable with docker:

`docker-compose up`

Or use the make file (which wraps the docker command

`make launch-topology`

Docker-compose will launch all the necessary databases and set the appropriate environment variables. If you would like to install locally, use python 3.5+ and run:

`make install-local`

This will install the required packages but you will still need to have the mongodb and rabbitmq. 

## Usage

To start running the topology call:

`make launch-topology`

Then to start a new ingestion call:

`make full-data-load`

To stream messages set up a listener with:

`make consume-stream`

and then call the following to stream data to it:

`make produce-stream`



