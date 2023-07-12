import pytest
import sqlalchemy
from typing import TypedDict


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
    try:
        # create a database for the tests
        engine = sqlalchemy.create_engine(
            "mssql+pymssql://sa:Password123@localhost:1433",
            connect_args={"autocommit": True},
        )

        def prepare_database(db_name: str, table_name: str):
            # disable and cleanup change change_data capture
            engine.execute("EXEC sys.sp_cdc_disable_db")
            engine.execute("USE master")

            # Force kill all connections to database db_name
            # https://stackoverflow.com/a/20688603
            try:
                engine.execute(
                    f"ALTER DATABASE {db_name} SET OFFLINE WITH ROLLBACK IMMEDIATE"
                )
                engine.execute(f"ALTER DATABASE {db_name} SET ONLINE")
            except sqlalchemy.exc.OperationalError:
                pass

            # Drop and recreate the database
            engine.execute(f"DROP DATABASE IF EXISTS {db_name}")
            engine.execute(f"CREATE DATABASE {db_name}")

            # Create the table in the database
            engine.execute(f"USE {db_name}")
            engine.execute("CREATE SCHEMA common")
            engine.execute(
                f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(50))"
            )

            engine.execute("USE master")

        for target, db in tables.items():
            prepare_database(db["db_name"], db["table_name"])

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
