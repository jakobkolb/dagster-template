install:
	poetry install

dev:
	poetry run dagster dev

test:
	poetry run pytest

lint:
	poetry run flake8 && poetry run black . --check

build:
	docker build -f docker/Dockerfile . --progress=plain -t $(tag)