from typing import List

from dagster_poc.utils.types import (
    Schema,
    Table,
    LsnRange,
    Changes,
    SQLChanges,
    DatabaseConnection,
    TrackedTableMetadata,
)
from enum import Enum
import ramda as R
import pandas as pd


# enum with DML operations
class DML(Enum):
    INSERT = 2
    UPDATE = 4
    DELETE = 1


def read_database_schema(
    database_connection: DatabaseConnection, table: Table
) -> Schema:
    """
    Reads the database schema for the given table.
    """
    engine = database_connection.get_engine()
    db_name = database_connection.get_db_name()

    with engine.connect() as connection:
        connection.execute(f"USE {db_name}")
        with connection.begin():
            res = connection.execute(
                f"""
                SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE
                TABLE_SCHEMA = '{table.db_schema}' AND TABLE_NAME = '{table.name}'
                """
            ).fetchall()
            columns = [r[0] for r in res]
            types = [r[1] for r in res]
            return Schema(columns=columns, types=types)


def read_primary_key(
    database_connection: DatabaseConnection, table: Table
) -> List[str]:
    """
    Reads the primary key for the given table.
    """
    res = database_connection.query(
        f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE
        OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
        AND TABLE_SCHEMA = '{table.db_schema}' AND TABLE_NAME = '{table.name}'
        """
    )
    return [r[0] for r in res]


def load_lsn_range_for_table(connection: DatabaseConnection, table: Table) -> LsnRange:
    min_lsn = connection.query(
        f"SELECT sys.fn_cdc_get_min_lsn ('{table.db_schema}_{table.name}')"
    )[0][0]
    max_lsn = connection.query(f"SELECT sys.fn_cdc_get_max_lsn ()")[0][0]
    return LsnRange(min_lsn=min_lsn, max_lsn=max_lsn)


def discover_tracked_tables(connection, db_name: str) -> List[Table]:
    connection.execute(f"USE {db_name}")

    res = connection.execute(
        f"""SELECT tables.name, schemas.name 
            FROM sys.tables tables, sys.schemas schemas 
            WHERE tables.is_tracked_by_cdc = 1 AND tables.schema_id = schemas.schema_id
            """
    )

    return [Table(name=r[0], db_schema=r[1]) for r in res]


def read_tracked_table_metadata(
    connection: DatabaseConnection, table: Table
) -> TrackedTableMetadata:
    lsn_range = load_lsn_range_for_table(connection, table)
    table_schema = read_database_schema(connection, table)
    primary_key = read_primary_key(connection, table)
    return TrackedTableMetadata(
        table=table,
        lsn_range=lsn_range,
        table_schema=table_schema,
        primary_key=primary_key,
    )


def get_next_lsn(connection: DatabaseConnection, lsn: bytes) -> bytes:
    return connection.query(
        f"""
            SELECT sys.fn_cdc_increment_lsn (:lsn)
        """,
        {"lsn": lsn},
    )[0][0]


def read_net_change_data_capture_for_table(
    connection: DatabaseConnection, tracked_table_metadata: TrackedTableMetadata
) -> Changes:
    # read net changes to the source table
    query = f"""
            SELECT * FROM cdc.fn_cdc_get_net_changes_{tracked_table_metadata.table.db_schema}_{tracked_table_metadata.table.name} (:min_lsn, :max_lsn, 'all with mask')
            """
    params = {
        "min_lsn": tracked_table_metadata.lsn_range.min_lsn,
        "max_lsn": tracked_table_metadata.lsn_range.max_lsn,
    }
    if (
        tracked_table_metadata.lsn_range.min_lsn
        >= tracked_table_metadata.lsn_range.max_lsn
    ):
        return Changes(
            table_metadata=tracked_table_metadata,
            changes=[],  # no changes to read
        )
    return Changes(
        table_metadata=tracked_table_metadata,
        changes=connection.query(query, params),
    )


def construct_sql_changes(changes: Changes) -> dict[str, SQLChanges]:
    return {
        "insert": SQLChanges(
            sql=construct_insert_statement(changes),
            change_data=filter_change_data_for_operation(DML.INSERT, changes),
        ),
        "delete": SQLChanges(
            sql=construct_delete_statement(changes),
            change_data=filter_change_data_for_operation(DML.DELETE, changes),
        ),
        "update": SQLChanges(
            sql=construct_update_statement(changes),
            change_data=filter_change_data_for_operation(DML.UPDATE, changes),
        ),
    }


def construct_update_statement(changes: Changes) -> str:
    table = changes.table_metadata.table
    schema = changes.table_metadata.table_schema
    primary_key = changes.table_metadata.primary_key

    # we can only update columns that are not part of the primary key
    modifiable_columns = [c for c in schema.columns if c not in primary_key]

    table_name = f"{table.db_schema}.{table.name}"
    update_statements = ", ".join([f"{c} = :{c}" for c in modifiable_columns])
    conditions = " AND ".join([f"{c} = :{c}" for c in primary_key])

    return f"UPDATE {table_name} SET {update_statements} WHERE {conditions}"


def construct_delete_statement(changes: Changes) -> str:
    table = changes.table_metadata.table
    schema = changes.table_metadata.table_schema

    table_name = f"{table.db_schema}.{table.name}"
    conditions = " AND ".join([f"{c} = :{c}" for c in schema.columns])

    return f"DELETE FROM {table_name} WHERE {conditions}"


def construct_insert_statement(changes: Changes) -> str:
    table = changes.table_metadata.table
    schema = changes.table_metadata.table_schema

    table_name = f"{table.db_schema}.{table.name}"
    columns = ", ".join(schema.columns)
    placeholders = ", ".join([f":{c}" for c in schema.columns])

    return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"


@R.curry
def filter_change_data_for_operation(operation: DML, changes: Changes) -> dict:
    """
    Filters the change data for the given operation.

    :param operation: The operation to filter for.
    :param changes: The changes to filter.
    """
    columns = [
        "start_lsn",
        "operation",
        "update_mask",
        *changes.table_metadata.table_schema.columns,
    ]
    data = pd.DataFrame(changes.changes, columns=columns)

    insert_data = data[data["operation"] == operation.value]
    print(insert_data)
    insert_data = insert_data.drop(columns=["start_lsn", "operation", "update_mask"])
    return insert_data.to_dict(orient="records")


def insert_sql_change_data(
    connection: DatabaseConnection, sql_changes: dict[str, SQLChanges]
):
    for operation, changes in sql_changes.items():
        if len(changes.change_data) > 0:
            connection.insert(changes.sql, changes.change_data)
            print(f"Found {len(changes.change_data)} {operation} changes")
            print(changes.change_data)
        else:
            print(f"No {operation} changes for {changes}")


def decode_lsn(lsn_bytes: bytes) -> int:
    return int.from_bytes(lsn_bytes, byteorder="big")


def encode_lsn(lsn: int | None) -> bytes | None:
    if lsn is None:
        return None
    return lsn.to_bytes(10, byteorder="big")
