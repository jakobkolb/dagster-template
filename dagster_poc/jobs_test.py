from dagster_poc.jobs import (
    names_of_tables_with_cdc_enabled,
    metadata_of_tracked_tables,
    change_data_records_of_tracked_tables,
    updated_sync_state,
)
import os
from dagster_poc.utils.types import Table, DatabaseConnection, Resources
from dagster import materialize, DagsterInstance, AssetKey
from sqlalchemy.engine import Engine
from time import sleep


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

    result = materialize([names_of_tables_with_cdc_enabled], resources=resources)
    assert result.success
    assert result.output_for_node("tracked_tables_at_origin_db") == [
        Table(name="change_data", db_schema="common")
    ]


def test_reading_log_sequence_numbers_for_tracked_tables(
    resources: Resources, source_table: Table, target_table: Table
):
    prepare_db(resources, source_table)

    result = materialize(
        [names_of_tables_with_cdc_enabled, metadata_of_tracked_tables],
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


def test_reading_change_data_records_for_tracked_tables(
    resources: Resources, source_table: Table
):
    prepare_db(resources, source_table)

    result = materialize(
        [
            names_of_tables_with_cdc_enabled,
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


def test_db_sync(
    resources: Resources, target_table: Table, source_table: Table, tmp_path
):
    os.environ["DAGSTER_HOME"] = str(tmp_path)

    # prepare source database
    prepare_db(resources, source_table)

    # run sync
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)


def test_repeated_db_sync(
    resources: Resources, source_table: Table, target_table: Table, tmp_path
):
    os.environ["DAGSTER_HOME"] = str(tmp_path)

    prepare_db(resources, source_table)

    # run sync
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)

    # insert new row to source database
    resources["source_db"].insert(
        f"""
        INSERT INTO {source_table.db_schema}.{source_table.name} VALUES (3, 'test3')
        """
    )

    # trigger cdc scan
    resources["source_db"].insert(f"EXEC sys.sp_cdc_scan")

    # run sync
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)


def test_db_sync_with_deleted_rows(
    resources: Resources, source_table: Table, target_table: Table, tmp_path
):
    os.environ["DAGSTER_HOME"] = str(tmp_path)

    resources["source_db"].insert(
        f"INSERT INTO {source_table.db_schema}.{source_table.name} VALUES (1, 'test'), (2, 'test2')"
    )

    # trigger cdc scan
    resources["source_db"].insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    # run sync
    print("sync first round of changes")
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)

    # delete row from source database
    resources["source_db"].insert(
        f"""
        DELETE FROM {source_table.db_schema}.{source_table.name} WHERE id = 1
        """
    )

    # trigger cdc scan
    resources["source_db"].insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    print("sync second round of changes")
    # run sync
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)


def test_db_sync_with_updated_rows(
    resources: Resources, source_table: Table, target_table: Table, tmp_path
):
    os.environ["DAGSTER_HOME"] = str(tmp_path)

    # insert new rows to source database
    resources["source_db"].insert(
        f"""
        INSERT INTO {source_table.db_schema}.{source_table.name} VALUES (1, 'test'), (2, 'test2')
        """
    )

    # trigger cdc scan
    resources["source_db"].insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    # run sync
    print("sync first round of changes")
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)

    # update row in source database
    resources["source_db"].insert(
        f"""
        UPDATE {source_table.db_schema}.{source_table.name} SET name = 'test3' WHERE id = 1
        """
    )

    # trigger cdc scan
    resources["source_db"].insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    print("sync second round of changes")
    # run sync
    sync(resources, source_table, target_table)

    # assert that both databases are in sync
    assert_databases_are_in_sync(resources, source_table, target_table)


def assert_databases_are_in_sync(
    resources: Resources, source_table: Table, target_table: Table
):
    source_values = resources["source_db"].query(
        f"SELECT * FROM {source_table.db_schema}.{source_table.name}"
    )
    target_values = resources["target_db"].query(
        f"SELECT * FROM {target_table.db_schema}.{target_table.name}"
    )

    assert source_values == target_values


def sync(resources, source_table, target_table):
    # run initial sync
    with DagsterInstance.get() as instance:
        result = materialize(
            [
                names_of_tables_with_cdc_enabled,
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
    print(sync_status["change_data"])
    assert sync_status["change_data"]["last_sync_success"]
    assert sync_status["change_data"]["last_synced_lsn"] is not None


def prepare_db(resources: Resources, source_table: Table):
    database = resources["source_db"]

    # insert data into source table
    database.insert(
        f"""
        INSERT INTO {source_table.db_schema}.{source_table.name} VALUES (1, 'test')
        """
    )

    # assert data is inserted
    assert database.query(
        f"SELECT * FROM {source_table.db_schema}.{source_table.name}"
    ) == [(1, "test")]

    # trigger cdc scan
    database.insert(f"EXEC sys.sp_cdc_scan")
    sleep(2)
