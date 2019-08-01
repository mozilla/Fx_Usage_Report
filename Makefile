.PHONY: help clean test coverage release build

help:
	@echo "  lint -  check style with flake8"
	@echo "  test -  run tests quickly with the default Python"
	@echo "  build - Builds the docker images for the docker-compose setup"
	@echo "  clean - Stops and removes all docker containers"
	@echo "  run -   Run a command"
	@echo "  shell - Opens a Bash shell"

lint:
	docker-compose run app flake8 usage_report tests --max-line-length 100

test:
	docker-compose run app py.test

build:
	docker-compose build

clean: stop
	docker-compose rm -f

shell:
	docker-compose run app bash

run:
	docker-compose run app $(COMMAND)

stop:
	docker-compose down
	docker-compose stop

up:
	docker-compose up
