# Sync Change Data Capture with dagster

Use Dagster to sync Tables between Microsoft SQL Server instances using Change Data Capture.

## Usage

The configuration for this project consists of four environment variables:

- `SOURCE_DB_CONNECTION_STRING`: The connection string for the source SQL Server instance.
- `TARGET_DB_CONNECTION_STRING`: The connection string for the target SQL Server instance.
- `SOURCE_DB_NAME`: The name of the source database.
- `TARGET_DB_NAME`: The name of the target database.
- `DAGSTER_HOME`: The path to the directory where Dagster will store its metadata.

A sample configuration file for local development is provided in `.env.dev`.

## Development

This project uses [Poetry](https://python-poetry.org/) to manage Python dependencies and docker to setup a local development environment.
Dagster provides a local development environment that allows you to run your pipeline locally and iterate quickly.

### Setup

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
poetry install --editable .
```

Then, start the local development environment:

```bash
make dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dagster_poc/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

### Adding new Python dependencies

You can add new Python dependencies via Poetry:

```bash
poetry add <package-name>
```

### Unit testing

Tests are located next to the code with the naming convetion `<python_module_name>_test.py` run tests using `pytest`.
Run all tests via

```bash
make test
```

Run all tests interactively on file changes via

```bash
make watch
```

Or run a specific test interactively via

```bash
make this test=<test_name>
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
