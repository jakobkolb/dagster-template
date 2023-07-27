#!make
-include .env.dev
export $(shell sed 's/=.*//' .env)

install:
	poetry install

dev:
	docker compose up -d
	poetry run dagster dev

test:
	poetry run pytest

watch:
	poetry run ptw

lint:
	poetry run flake8 && poetry run black . --check

build:
	docker build -f docker/Dockerfile . --progress=plain -t $(tag)

this:
	poetry run ptw -- -- -k $(test) -vvv