from dagster import EnvVar, Definitions, asset, AssetIn, Output, AssetKey
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


# @asset
# def update_source_table(source_db: DatabaseConnection) -> Output[int]:
#
#     # get last id from source table
#     nint = source_db.query("SELECT MAX(id) FROM common.change_data")[0][0] + 1
#
#     # insert a row into the source table
#     source_db.insert(
#         f"""
#         INSERT INTO common.change_data (id, name) VALUES ({nint}, 'test_{nint}')
#         """
#     )
#     # trigger CDC
#     source_db.insert(f"EXEC sys.sp_cdc_scan")
#
#     return Output(nint, metadata={"source_table_update": nint})


@asset()
def tracked_tables_at_origin_db(
        source_db: DatabaseConnection,
        # update_source_table
) -> List[Table]:
    """
    Change change_data table from origin.
    """
    engine = source_db.get_engine()
    db_name = source_db.get_db_name()

    with engine.connect() as connection:
        return discover_tracked_tables(connection, db_name)


@asset()
def metadata_of_tracked_tables(
    source_db: DatabaseConnection, tracked_tables_at_origin_db: List[Table]
) -> List[TrackedTableMetadata]:
    """
    Log sequence numbers of tracked tables.
    """
    metadata = []
    for table in tracked_tables_at_origin_db:
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
                print(f"Changes for table {metadata.table.name}")
                print(f"between {decode_lsn(next_min_lsn)} and {decode_lsn(metadata.lsn_range.max_lsn)}")
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
            print('synchronizing changes done.')
            table_sync_status[change.table_metadata.table.name] = TableSyncStatus(
                table=change.table_metadata.table.dict(),
                last_sync_success=True,
                synchronized_inserts=len(sql_changes["insert"].change_data),
                synchronized_updates=len(sql_changes["update"].change_data),
                synchronized_deletes=len(sql_changes["delete"].change_data),
                last_sync_error=None,
                last_synced_lsn=decode_lsn(change.table_metadata.lsn_range.max_lsn),
            ).dict()
            print('creating sync status done.')
        except Exception as e:
            table_sync_status[change.table_metadata.table.name] = TableSyncStatus(
                table=change.table_metadata.table.dict(),
                last_sync_success=False,
                synchronized_inserts=0,
                synchronized_updates=0,
                synchronized_deletes=0,
                last_sync_error=str(e),
                last_synced_lsn=last_synced_lsn,
            ).dict()
        max_synced_lsn = decode_lsn(change.table_metadata.lsn_range.max_lsn)
    return Output(value=None, metadata={"table_sync_status": table_sync_status, "last_lsn": max_synced_lsn})


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
}

defs = Definitions(
    resources=resources,
    assets=[
        # update_source_table,
        tracked_tables_at_origin_db,
        metadata_of_tracked_tables,
        change_data_records_of_tracked_tables,
        updated_sync_state,
    ]
)
