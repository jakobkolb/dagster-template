from dagster_poc.utils.database import (
    read_database_schema,
    load_lsn_range_for_table,
    discover_tracked_tables,
    read_net_change_data_capture_for_table,
    read_tracked_table_metadata,
    construct_insert_statement,
    filter_change_data_for_operation,
    construct_sql_changes,
    insert_sql_change_data,
    get_next_lsn,
    DML,
    encode_lsn,
    decode_lsn,
)
from dagster_poc.utils.types import TrackedTableMetadata, Changes, SQLChanges
from dagster_poc.utils.types import Schema, Table, DatabaseConnection, LsnRange

from time import sleep

def test_database_fixture_has_tables_and_is_writable(database, tables):
    """
    Asserts that the given database fixture has the given tables.
    """
    with database.connect() as connection:
        for target, table in tables.items():
            connection.execute(f'USE {table["db_name"]}')
            connection.execute(
                f'INSERT INTO {table["table_name"]} VALUES (1, \'test\')'
            )
            assert connection.execute(
                f'SELECT * FROM {table["table_name"]}'
            ).fetchall() == [(1, "test")]


def test_source_table_has_change_data_capture_enabled(database, tables):
    """
    Asserts that the given database fixture has change change_data capture enabled on the source table.
    """
    with database.connect() as connection:
        res = discover_tracked_tables(connection, tables["source"]["db_name"])
        print(res)
        assert res == [Table(name="change_data", db_schema="common")]


def test_read_lsn_range_returns_none_for_max_lsn_when_no_changes_have_been_made(
    db_context,
):
    """
    Asserts that the lsn range for a table is None when no changes have been made.
    """
    table: Table = db_context.tables["source"]
    connection = db_context.source_db

    lsn_range = load_lsn_range_for_table(connection, table)

    assert lsn_range.min_lsn is not None
    assert lsn_range.max_lsn is None


def test_read_lsn_range_returns_lsn_range_when_changes_have_been_made(db_context):
    """
    Asserts that the lsn range for a table is None when no changes have been made.
    """
    table: Table = db_context.tables["source"]
    connection = db_context.source_db

    # add some change_data to the source table
    connection.insert(f"INSERT INTO {table.db_schema}.{table.name} VALUES (1, 'test')")

    # trigger change change_data capture job manually
    connection.insert(f"EXEC sys.sp_cdc_scan")

    lsn_range = load_lsn_range_for_table(connection, table)

    assert lsn_range.min_lsn is not None
    assert lsn_range.max_lsn is not None


def test_read_database_schema_returns_column_names_and_types(db_context):
    schema = read_database_schema(db_context.source_db, db_context.tables["source"])

    assert schema == Schema(columns=["id", "name"], types=["int", "varchar"])


def test_read_tracked_table_metadata_returns_correct_metadata(db_context):
    metadata = read_tracked_table_metadata(
        db_context.source_db, db_context.tables["source"]
    )

    assert metadata.table == Table(name="change_data", db_schema="common")
    assert metadata.table_schema == Schema(
        columns=["id", "name"], types=["int", "varchar"]
    )
    assert metadata.lsn_range.min_lsn is not None
    assert metadata.lsn_range.max_lsn is None


def test_get_next_lsn_returns_correct_lsn(db_context):
    """
    Asserts that the next lsn is the max lsn + 1
    """
    table = db_context.tables["source"]
    connection = db_context.source_db

    # add some change_data to the source table
    connection.insert(f"INSERT INTO {table.db_schema}.{table.name} VALUES (1, 'test')")
    connection.insert(f"EXEC sys.sp_cdc_scan")

    # load lsn range
    lsn_range = load_lsn_range_for_table(connection, table)

    # get next lsn
    next_lsn = get_next_lsn(connection, lsn_range.min_lsn)

    assert next_lsn > lsn_range.min_lsn


def test_read_change_data_capture_from_source_db(db_context):
    """
    Reads change change_data capture from the source database.
    """
    print(db_context.tables)
    table: Table = db_context.tables["source"]
    connection = db_context.source_db

    # add some change_data to the source table
    connection.insert(f"INSERT INTO {table.db_schema}.{table.name} VALUES (1, 'test')")

    # trigger change change_data capture job manually
    connection.insert(f"EXEC sys.sp_cdc_scan")

    tracked_table_metadata = TrackedTableMetadata(
        table=table,
        lsn_range=load_lsn_range_for_table(connection, table),
        table_schema=read_database_schema(connection, table),
    )

    change_data = read_net_change_data_capture_for_table(
        connection, tracked_table_metadata
    )

    changed_rows = change_data.changes

    print(changed_rows)

    assert len(changed_rows) == 1

    # the last two entries in the row are the inserted change_data
    assert changed_rows[0][-2:] == (1, "test")

    # the first entry in the row is the max lsn (since we only made one change)
    assert changed_rows[0][0] == tracked_table_metadata.lsn_range.max_lsn


def test_construct_sql_statement_from_change_data_returns_correct_sql_statement_for_insert():
    change_data = Changes(
        changes=[(b"\x00\x00\x00'\x00\x00\x02\xef\x00\x1c", 2, None, 1, "test")],
        table_metadata=TrackedTableMetadata(
            table=Table(name="change_data", db_schema="common"),
            lsn_range=LsnRange(
                min_lsn=b"\x00\x00\x00\x00\x00\x00\x00\x00",
                max_lsn=b"\x00\x00\x00'\x00\x00\x02\xef\x00\x1c",
            ),
            table_schema=Schema(columns=["id", "name"], types=["int", "varchar"]),
        ),
    )
    sql_statement = construct_insert_statement(change_data)

    assert (
        sql_statement == "INSERT INTO common.change_data (id, name) VALUES (:id, :name)"
    )


def test_constructed_sql_statement_has_correct_parameters_for_insert():
    change_data = Changes(
        changes=[(b"\x00\x00\x00'\x00\x00\x02\xef\x00\x1c", 2, None, 1, "test")],
        table_metadata=TrackedTableMetadata(
            table=Table(name="change_data", db_schema="common"),
            lsn_range=LsnRange(
                min_lsn=b"\x00\x00\x00\x00\x00\x00\x00\x00",
                max_lsn=b"\x00\x00\x00'\x00\x00\x02\xef\x00\x1c",
            ),
            table_schema=Schema(columns=["id", "name"], types=["int", "varchar"]),
        ),
    )
    sql_data = filter_change_data_for_operation(DML.INSERT, change_data)

    assert sql_data == [{"id": 1, "name": "test"}]


def test_insert_sql_change_data(db_context):
    """
    Asserts that the constructed sql statement can be executed.
    """
    sql_change_data = {
        "insert": SQLChanges(
            sql="INSERT INTO common.change_data (id, name) VALUES (:id, :name)",
            change_data=[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
        )
    }

    connection = db_context.source_db

    insert_sql_change_data(connection, sql_change_data)

    assert connection.query("SELECT * FROM common.change_data") == [
        (1, "test"),
        (2, "test2"),
    ]


def test_construct_and_execute_sql_change_data_syncs_db(db_context):
    source = db_context.tables["source"]
    target = db_context.tables["target"]

    source_connection = db_context.source_db
    target_connection = DatabaseConnection(
        connection_string=source_connection.connection_string, db_name="target"
    )

    # insert data into source table
    source_connection.insert(
        f"INSERT INTO {source.db_schema}.{source.name} VALUES (1, 'test'), (2, 'test2')"
    )

    # trigger change change_data capture job manually
    source_connection.insert(f"EXEC sys.sp_cdc_scan")

    # read change change_data capture from source db
    tracked_table_metadata = read_tracked_table_metadata(source_connection, source)

    change_data = read_net_change_data_capture_for_table(
        source_connection, tracked_table_metadata
    )

    sql_change_data = construct_sql_changes(change_data)

    # insert change change_data into target db
    insert_sql_change_data(target_connection, sql_change_data)

    # assert that the target table has the same data as the source table
    source_data = source_connection.query(
        f"SELECT * FROM {source.db_schema}.{source.name}"
    )
    target_data = target_connection.query(
        f"SELECT * FROM {target.db_schema}.{target.name}"
    )

    assert source_data == target_data


def test_increment_lsn_range_returns_lsn_range_that_includes_only_new_changes(
    db_context,
):
    # insert data into source table
    source = db_context.tables["source"]
    source_connection = db_context.source_db

    source_connection.insert(
        f"INSERT INTO {source.db_schema}.{source.name} VALUES (1, 'test'), (2, 'test2')"
    )

    # trigger change change_data capture job manually
    source_connection.insert(f"EXEC sys.sp_cdc_scan")

    # read metadata from source db
    tracked_table_metadata = read_tracked_table_metadata(source_connection, source)

    # add more data to source table
    source_connection.insert(
        f"INSERT INTO {source.db_schema}.{source.name} VALUES (3, 'test3'), (4, 'test4')"
    )

    # trigger change change_data capture job manually
    source_connection.insert(f"EXEC sys.sp_cdc_scan")

    # increment the max_lsn of the tracked_table_metadata by one
    new_min_lsn = get_next_lsn(
        source_connection, tracked_table_metadata.lsn_range.max_lsn
    )

    # read metadata from source db
    tracked_table_metadata = read_tracked_table_metadata(source_connection, source)

    # set the new min_lsn
    tracked_table_metadata.lsn_range.min_lsn = new_min_lsn

    # read change change_data capture from source db
    change_data = read_net_change_data_capture_for_table(
        source_connection, tracked_table_metadata
    )

    # assert that the change change_data only contains the new changes
    print(change_data.changes)
    assert change_data.changes[0][-2:] == (3, "test3")
    assert change_data.changes[1][-2:] == (4, "test4")


def test_construct_sql_change_data_for_delete(db_context):
    # insert data into source table
    source = db_context.tables["source"]
    source_connection = db_context.source_db

    source_connection.insert(
        f"INSERT INTO {source.db_schema}.{source.name} VALUES (1, 'test'), (2, 'test2')"
    )

    # trigger change change_data capture job manually
    source_connection.insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    old_tracked_table_metadata = read_tracked_table_metadata(source_connection, source)

    # delete data from source table
    source_connection.insert(
        f"DELETE FROM {source.db_schema}.{source.name} WHERE id = 1"
    )

    # trigger change change_data capture job manually
    source_connection.insert(f"EXEC sys.sp_cdc_scan")
    sleep(1)

    # read metadata from source db
    tracked_table_metadata = read_tracked_table_metadata(source_connection, source)

    # set the new min_lsn to only select new changes. (otherwise we would get only one insert)
    tracked_table_metadata.lsn_range.min_lsn = get_next_lsn(source_connection, old_tracked_table_metadata.lsn_range.max_lsn)

    # read change change_data capture from source db
    change_data = read_net_change_data_capture_for_table(
        source_connection, tracked_table_metadata
    )

    sql_change_data = construct_sql_changes(change_data)

    assert sql_change_data['delete'] == SQLChanges(
            sql="DELETE FROM common.change_data WHERE id = :id AND name = :name",
            change_data=[{"id": 1, "name": "test"}],
        )


def test_bytes_conversion(db_context):

    connection = db_context.source_db
    table = db_context.tables["source"]

    lsn = connection.query(
        f"SELECT sys.fn_cdc_get_min_lsn('{table.db_schema}_{table.name}')"
    )[0][0]

    lsn_int = decode_lsn(lsn)

    next_lsn = get_next_lsn(connection, lsn)

    next_lsn_int = decode_lsn(next_lsn)

    assert next_lsn > lsn
    assert next_lsn == encode_lsn(next_lsn_int)
    assert lsn == encode_lsn(lsn_int)
