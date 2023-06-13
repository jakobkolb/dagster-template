FROM python:3.9

RUN pip install poetry &&\
    poetry config virtualenvs.create false &&\
    poetry config installer.max-workers 10


COPY poetry.lock pyproject.toml README.md ./

RUN poetry install --only main --no-interaction --no-ansi --no-root

# delete poetry cache
RUN rm -rf /root/.cache/pypoetry

# delete pip cache
RUN rm -rf /root/.cache/pip

# copy dagster assets
COPY dagster_poc/ /dagster_poc/