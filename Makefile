#!make
-include .env
export $(shell sed 's/=.*//' .env)

install:
	poetry install

dev:
	poetry run dagster dev

test:
	poetry run pytest

watch:
	poetry run pytest -f -ff

lint:
	poetry run flake8 && poetry run black . --check

build:
	docker build -f docker/Dockerfile . --progress=plain -t $(tag)