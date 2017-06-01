.PHONY: tests requirements.txt

install-local: requirements.txt
	pip install -r requirements.txt

tests: tests.py
	 python -m unittest tests.py

launch-topology:
	docker-compose up

full-data-load:
	docker-compose run python ./main.py full

consume-stream:
	docker-compose run python ./main.py consume

produce-stream:
	docker-compose run python ./main.py produce


