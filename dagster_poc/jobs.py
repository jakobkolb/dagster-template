from dagster import EnvVar, Definitions, asset, AssetIn, Output, AssetKey
from typing import List
from dagster_poc.utils.database import (
    read_tracked_table_metadata,
    discover_tracked_tables,
    read_net_number_of_change_data_capture_for_table,
    read_net_change_data_capture_for_table,
    construct_sql_changes,
    insert_sql_change_data,
    encode_lsn,
    decode_lsn,
    get_next_lsn,
)
from dagster_poc.utils.types import (
    Table,
    DatabaseConnection,
    TrackedTableMetadata,
    TableSyncStatus,
)


BATCH_SIZE = 10000


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
def names_of_tables_with_cdc_enabled(
    source_db: DatabaseConnection,
    # update_source_table
) -> List[Table]:
    """
    Load all tables that have change data capture enabled.
    """
    engine = source_db.get_engine()
    db_name = source_db.get_db_name()

    with engine.connect() as connection:
        return discover_tracked_tables(connection, db_name)


@asset()
def metadata_of_tracked_tables(
    source_db: DatabaseConnection, names_of_tables_with_cdc_enabled: List[Table]
) -> List[TrackedTableMetadata]:
    """
    Metadata of tables that have change data tracking enabled including
    * valid log sequence numbers for change data capture (LSN)
    * primary key column names (if any)
    * table schema (column names and types)
    """
    metadata = []
    for table in names_of_tables_with_cdc_enabled:
        metadata.append(read_tracked_table_metadata(source_db, table))
    return metadata


@asset(ins={"tables_metadata": AssetIn(key="metadata_of_tracked_tables")})
def iterative_updated_sync_state(
        context,
        source_db: DatabaseConnection,
        target_db: DatabaseConnection,
        tables_metadata: List[TrackedTableMetadata],
) -> Output[None]:
    table_sync_status = load_table_sync_status(context)

    for metadata in tables_metadata:
        # get last synced lsn for table and update metadata
        last_synced_lsn = get_last_synced_lsn_for_table(table_sync_status, metadata)
        metadata.lsn_range.min_lsn = get_next_min_lsn_for_table(source_db, metadata, last_synced_lsn)

        # batch sync changes to table
        synced_inserts, synced_updates, synced_deletes = sync_changes(source_db, target_db, metadata, BATCH_SIZE)

        # update table sync status
        table_sync_status[metadata.table.name] = TableSyncStatus(
            table=metadata.table.dict(),
            last_sync_success=True,
            synchronized_inserts=synced_inserts,
            synchronized_updates=synced_updates,
            synchronized_deletes=synced_deletes,
            last_sync_error=None,
            last_synced_lsn=decode_lsn(metadata.lsn_range.max_lsn),
        ).dict()

    return Output(
        value=None,
        metadata={"table_sync_status": table_sync_status},
    )


def sync_changes(source_db, target_db, metadata, batch_size):

    # get number of changes for table
    number_of_changes = read_net_number_of_change_data_capture_for_table(
        source_db, metadata
    )

    # initialize counters
    synced_changes = 0
    synced_inserts = 0
    synced_updates = 0
    synced_deletes = 0

    # while there are still changes to sync:
    while synced_changes < number_of_changes:
        print(f'syncing between {synced_changes} and {synced_changes + batch_size}')
        # read a batch of changes
        changes = read_net_change_data_capture_for_table(
            source_db, metadata, synced_changes, batch_size
        )

        # construct sql changes
        sql_changes = construct_sql_changes(changes)

        # insert sql changes into target db
        insert_sql_change_data(target_db, sql_changes)

        # update counters
        synced_changes += batch_size
        synced_inserts += len(sql_changes["insert"].change_data)
        synced_updates += len(sql_changes["update"].change_data)
        synced_deletes += len(sql_changes["delete"].change_data)

    return synced_inserts, synced_updates, synced_deletes


def get_next_min_lsn_for_table(source_db: DatabaseConnection, metadata: TrackedTableMetadata, last_synced_lsn):
    """
    get the next min lsn for a table
    :param source_db:
    :param metadata:
    :param last_synced_lsn:
    :return: next_lsn
    """

    # if last_synced_lsn is None (this is the first sync):
    if last_synced_lsn is None:
        # use the min lsn from the metadata
        return metadata.lsn_range.min_lsn
    # else, if there was a previous sync:
    else:
        # increment the last synced lsn by 1
        next_min_lsn = get_next_lsn(source_db, encode_lsn(last_synced_lsn))
        # if this is greater than the max lsn from the metadata:
        if next_min_lsn > metadata.lsn_range.max_lsn:
            # set the min lsn to the max lsn
            print(f"No changes for table {metadata.table.name}")
            next_min_lsn = metadata.lsn_range.max_lsn
        return next_min_lsn


def get_last_synced_lsn_for_table(table_sync_status, table_metadata):
    return table_sync_status.get(table_metadata.table.name, {}).get(
        "last_synced_lsn", None
    )


def load_table_sync_status(context):
    """
    Load the table sync status from the previous run.
    If there is no previous run, return an empty dictionary.
    :param context: Dagster run context
    :return: Dict of Sync status for each table
    """
    instance = context.instance
    try:
        materialization = instance.get_latest_materialization_event(
            AssetKey(["iterative_updated_sync_state"])
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
        names_of_tables_with_cdc_enabled,
        metadata_of_tracked_tables,
        iterative_updated_sync_state,
    ],
)
