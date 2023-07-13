import pytest
import sqlalchemy
from typing import TypedDict
from dagster_poc.utils.types import DatabaseConnection, Table, Resources, TestContext


class Tables(TypedDict):
    source: dict[str, str]
    target: dict[str, str]


@pytest.fixture()
def tables() -> Tables:
    return {
        "source": {"table_name": "source.common.change_data", "db_name": "source"},
        "target": {"table_name": "target.common.change_data", "db_name": "target"},
    }


@pytest.fixture()
def database(tables):

    # create a source_db for the tests
    engine = sqlalchemy.create_engine(
        "mssql+pymssql://sa:Password123@localhost:1433",
        connect_args={"autocommit": True},
    )
    try:
        for target, db in tables.items():
            prepare_database(engine, db["db_name"], db["table_name"])

        # Enable Change Data Capture on the source_db
        engine.execute(f'USE {tables["source"]["db_name"]}')
        engine.execute("EXEC sys.sp_cdc_enable_db")
        engine.execute(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'common', @source_name = N'change_data', @role_name = NULL, @supports_net_changes = 1"
        )
        yield engine
    finally:
        # Teardown: drop the databases.
        engine.execute("USE master")
        engine.execute("DROP DATABASE IF EXISTS source_db")
        engine.execute("DROP DATABASE IF EXISTS target_db")


def prepare_database(engine, db_name: str, table_name: str):
    # disable and cleanup change change_data capture
    engine.execute("EXEC sys.sp_cdc_disable_db")
    engine.execute("USE master")

    # Force kill all connections to source_db db_name
    # https://stackoverflow.com/a/20688603
    try:
        engine.execute(
            f"ALTER DATABASE {db_name} SET OFFLINE WITH ROLLBACK IMMEDIATE"
        )
        engine.execute(f"ALTER DATABASE {db_name} SET ONLINE")
    except sqlalchemy.exc.OperationalError:
        pass

    # Drop and recreate the source_db
    engine.execute(f"DROP DATABASE IF EXISTS {db_name}")
    engine.execute(f"CREATE DATABASE {db_name}")

    # Create the table in the source_db
    engine.execute(f"USE {db_name}")
    engine.execute("CREATE SCHEMA common")
    engine.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(50))"
    )

    engine.execute("USE master")


@pytest.fixture
def db_context(database, tables: Tables) -> TestContext:
    source_db = DatabaseConnection(
        connection_string=str(database.url), db_name=tables["source"]["db_name"]
    )
    target_db = DatabaseConnection(
        connection_string=str(database.url), db_name=tables["target"]["db_name"]
    )
    res_tables = {}
    for table in tables.values():
        res_tables[table["db_name"]] = Table(
            name=table["table_name"].split(".")[-1],
            db_schema=table["table_name"].split(".")[-2],
        )

    return TestContext(source_db=source_db, target_db=target_db, tables=res_tables)


@pytest.fixture
def source_table(tables: Tables) -> Table:
    return Table(
        name=tables["source"]["table_name"].split(".")[-1],
        db_schema=tables["source"]["table_name"].split(".")[-2],
    )


@pytest.fixture
def target_table(tables: Tables) -> Table:
    return Table(
        name=tables["target"]["table_name"].split(".")[-1],
        db_schema=tables["target"]["table_name"].split(".")[-2],
    )


@pytest.fixture
def resources(database, tables: Tables) -> Resources:
    connection_string = str(database.url)
    source_db_name = tables["source"]["db_name"]
    target_db_name = tables["target"]["db_name"]
    return {
        "source_db": DatabaseConnection(
            connection_string=connection_string, db_name=source_db_name
        ),
        "target_db": DatabaseConnection(
            connection_string=connection_string, db_name=target_db_name
        ),
    }