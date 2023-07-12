from dagster_poc.jobs import (
    tracked_tables_at_origin_db,
    metadata_of_tracked_tables,
    change_data_records_of_tracked_tables,
    updated_sync_state,
)
import os
from dagster_poc.utils.types import Table, DatabaseConnection
from dagster import materialize, DagsterInstance, AssetKey, JsonMetadataValue
from sqlalchemy.engine import Engine
import pytest


def test_tracked_tables_at_origin_returns_tracked_tables(
    database: Engine, tables: dict
):
    connection_string = str(database.url)
    db_name = tables["source"]["db_name"]
    resources = {
        "source_db": DatabaseConnection(
            connection_string=connection_string, db_name=db_name
        )
    }

    result = materialize([tracked_tables_at_origin_db], resources=resources)
    assert result.success
    assert result.output_for_node("tracked_tables_at_origin_db") == [
        Table(name="change_data", db_schema="common")
    ]


def test_reading_log_sequence_numbers_for_tracked_tables(
    database: Engine, tables: dict
):
    resources = prepare_db(database, tables)

    result = materialize(
        [tracked_tables_at_origin_db, metadata_of_tracked_tables],
        resources=resources,
    )
    assert result.success

    assert (
        result.output_for_node("metadata_of_tracked_tables")[0].lsn_range.min_lsn
        is not None
    )
    assert (
        result.output_for_node("metadata_of_tracked_tables")[0].lsn_range.max_lsn
        is not None
    )


def test_reading_change_data_records_for_tracked_tables(database: Engine, tables: dict):
    resources = prepare_db(database, tables)

    result = materialize(
        [
            tracked_tables_at_origin_db,
            metadata_of_tracked_tables,
            change_data_records_of_tracked_tables,
        ],
        resources=resources,
    )
    assert result.success

    change_records = result.output_for_node("change_data_records_of_tracked_tables")

    # we have change records for one table
    assert len(change_records) == 1
    assert change_records[0] is not None

    record = change_records[0]

    # we have change records for one row
    assert len(record.changes) == 1

    # changes are equal to the row we inserted
    assert record.changes[0][-2:] == (1, "test")


def test_db_sync(database: Engine, tables: dict, tmp_path):
    os.environ["DAGSTER_HOME"] = str(tmp_path)

    # prepare source database
    resources = prepare_db(database, tables)

    sync(resources, tables)

    # insert new row to source database
    resources["source_db"].insert(
        f"""
        INSERT INTO {tables["source"]["table_name"]} VALUES (2, 'test2'), (3, 'test3')
        """
    )
    # trigger CDC job
    resources["source_db"].insert(
        f"EXEC sys.sp_cdc_scan @maxtrans = 2, @maxscans = 1, @continuous = 0"
    )

    sync(resources, tables)


def sync(resources, tables):
    # run initial sync
    with DagsterInstance.get() as instance:
        result = materialize(
            [
                tracked_tables_at_origin_db,
                metadata_of_tracked_tables,
                change_data_records_of_tracked_tables,
                updated_sync_state,
            ],
            resources=resources,
            instance=instance,
        )

        # get metadata for updated_sync_state from instance
        materialization = instance.get_latest_materialization_event(
            AssetKey(["updated_sync_state"])
        ).asset_materialization
        sync_status = materialization.metadata["table_sync_status"].value

    assert result.success
    assert sync_status["change_data"]["last_sync_success"]
    assert sync_status["change_data"]["last_synced_lsn"] is not None

    # assert both databases are in sync
    source_values = resources["source_db"].query(
        f'SELECT * FROM {tables["source"]["table_name"]}'
    )
    target_values = resources["target_db"].query(
        f'SELECT * FROM {tables["target"]["table_name"]}'
    )

    assert source_values == target_values


def prepare_db(database: Engine, tables: dict):
    connection_string = str(database.url)
    source_db_name = tables["source"]["db_name"]
    target_db_name = tables["target"]["db_name"]
    resources = {
        "source_db": DatabaseConnection(
            connection_string=connection_string, db_name=source_db_name
        ),
        "target_db": DatabaseConnection(
            connection_string=connection_string, db_name=target_db_name
        ),
    }

    # Load change_data to source table
    with database.connect() as source_db:
        table = tables["source"]
        source_db.execute(f'USE {table["db_name"]}')
        source_db.execute(f'INSERT INTO {table["table_name"]} VALUES (1, \'test\')')
        assert source_db.execute(f'SELECT * FROM {table["table_name"]}').fetchall() == [
            (1, "test")
        ]

    # Trigger CDC job
    with database.connect() as source_db:
        source_db.execute(f"USE {source_db_name}")
        source_db.execute(
            f"EXEC sys.sp_cdc_scan @maxtrans = 2, @maxscans = 1, @continuous = 0"
        )

    return resources
