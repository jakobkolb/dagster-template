from dagster import EnvVar, Definitions, asset, AssetIn, Output, AssetKey
from dagster_duckdb_pandas import DuckDBPandasIOManager
from typing import List
from dagster_poc.utils.database import (
    read_tracked_table_metadata,
    discover_tracked_tables,
    read_net_change_data_capture_for_table,
    construct_sql_changes,
    insert_sql_change_data,
    encode_lsn,
    decode_lsn,
    get_next_lsn,
)
from dagster_poc.utils.types import (
    Table,
    Changes,
    DatabaseConnection,
    TrackedTableMetadata,
    TableSyncStatus,
)


@asset()
def tracked_tables_at_origin_db(source_db: DatabaseConnection) -> List[Table]:
    """
    Change change_data table from origin.
    """
    engine = source_db.get_engine()
    db_name = source_db.get_db_name()

    with engine.connect() as connection:
        return discover_tracked_tables(connection, db_name)


@asset(ins={"tracked_tables": AssetIn(key="tracked_tables_at_origin_db")})
def metadata_of_tracked_tables(
    source_db: DatabaseConnection, tracked_tables: List[Table]
) -> List[TrackedTableMetadata]:
    """
    Log sequence numbers of tracked tables.
    """
    metadata = []
    for table in tracked_tables:
        metadata.append(read_tracked_table_metadata(source_db, table))
    return metadata


@asset(ins={"tables_metadata": AssetIn(key="metadata_of_tracked_tables")})
def change_data_records_of_tracked_tables(
    context,
    source_db: DatabaseConnection,
    tables_metadata: List[TrackedTableMetadata],
) -> List[Changes]:
    table_sync_status = load_table_sync_status(context)
    changes = []
    for metadata in tables_metadata:
        last_synced_lsn = get_last_synced_lsn_for_table(table_sync_status, metadata)
        if last_synced_lsn is not None:
            next_min_lsn = get_next_lsn(source_db, encode_lsn(last_synced_lsn))
            if next_min_lsn <= metadata.lsn_range.max_lsn:
                metadata.lsn_range.min_lsn = next_min_lsn
            else:
                print(f"No changes for table {metadata.table.name}")
                metadata.lsn_range.min_lsn = metadata.lsn_range.max_lsn
        changes.append(read_net_change_data_capture_for_table(source_db, metadata))
    return changes


@asset(ins={"changes": AssetIn(key="change_data_records_of_tracked_tables")})
def updated_sync_state(
    context, target_db: DatabaseConnection, changes: List[Changes]
) -> Output[None]:
    """
    Load changes to target database
    """
    table_sync_status = load_table_sync_status(context)
    for change in changes:
        last_synced_lsn = get_last_synced_lsn_for_table(
            table_sync_status, change.table_metadata
        )
        try:
            sql_changes = construct_sql_changes(change)
            insert_sql_change_data(target_db, sql_changes)
            table_sync_status[change.table_metadata.table.name] = TableSyncStatus(
                table=change.table_metadata.table.dict(),
                last_sync_success=True,
                last_sync_error=None,
                last_synced_lsn=decode_lsn(change.table_metadata.lsn_range.max_lsn),
            ).dict()
        except Exception as e:
            table_sync_status[change.table_metadata.table.name] = TableSyncStatus(
                table=change.table_metadata.table.dict(),
                last_sync_success=False,
                last_sync_error=str(e),
                last_synced_lsn=last_synced_lsn,
            ).dict()
    return Output(value=None, metadata={"table_sync_status": table_sync_status})


def get_last_synced_lsn_for_table(table_sync_status, table_metadata):
    return table_sync_status.get(table_metadata.table.name, {}).get(
        "last_synced_lsn", None
    )


def load_table_sync_status(context):
    instance = context.instance
    try:
        materialization = instance.get_latest_materialization_event(
            AssetKey(["updated_sync_state"])
        ).asset_materialization
        previous_table_sync_status = materialization.metadata["table_sync_status"]
        if previous_table_sync_status is None:
            return {}
        return previous_table_sync_status.value
    except AttributeError:
        return {}


resources = {
    "source_db": DatabaseConnection(
        connection_string=EnvVar("SOURCE_DB_CONNECTION_STRING"),
        db_name=EnvVar("SOURCE_DB_NAME"),
    ),
    "target_db": DatabaseConnection(
        connection_string=EnvVar("TARGET_DB_CONNECTION_STRING"),
        db_name=EnvVar("TARGET_DB_NAME"),
    ),
    "io_manager": DuckDBPandasIOManager(database="memory.db"),
}

defs = Definitions(resources=resources, assets=[tracked_tables_at_origin_db])
