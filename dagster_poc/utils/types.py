from typing import List, Any, Union, TypedDict

import sqlalchemy

from sqlalchemy.exc import ResourceClosedError
from dagster import ConfigurableResource

from pydantic import BaseModel


class Schema(BaseModel):
    columns: List[str]
    types: List[str]


class Table(BaseModel):
    name: str
    db_schema: str


class TableSyncStatus(BaseModel):
    table: Table
    last_sync_success: bool
    synchronized_inserts: int
    synchronized_updates: int
    synchronized_deletes: int
    last_sync_error: str | None
    last_synced_lsn: int | None


class LsnRange(BaseModel):
    min_lsn: bytes
    max_lsn: Union[bytes, None]


class TrackedTableMetadata(BaseModel):
    table: Table
    table_schema: Schema
    lsn_range: LsnRange


class Changes(BaseModel):
    table_metadata: TrackedTableMetadata
    changes: list


class SQLChanges(BaseModel):
    sql: str
    change_data: list[dict[str, Any]]


class DatabaseConnection(ConfigurableResource):
    connection_string: str
    db_name: str

    def get_engine(self):
        return sqlalchemy.create_engine(self.connection_string)

    def get_db_name(self):
        return self.db_name

    def query(self, sql: str, data: dict = None):
        engine = sqlalchemy.create_engine(self.connection_string)
        with engine.connect() as connection:
            connection.execute(f"USE {self.db_name}")
            with connection.begin():
                return connection.execute(sqlalchemy.text(sql), data).fetchall()

    def insert(self, sql: str, data: list[dict] = None):
        engine = sqlalchemy.create_engine(self.connection_string)
        with engine.connect() as connection:
            connection.execute(f"USE {self.db_name}")
            with connection.begin():
                res = connection.execute(sqlalchemy.text(sql), data)
                try:
                    return res.fetchall()
                except ResourceClosedError:
                    return None


class Resources(TypedDict):
    source_db: DatabaseConnection
    target_db: DatabaseConnection


class TestContext(BaseModel):
    source_db: DatabaseConnection
    target_db: DatabaseConnection
    tables: dict[str, Table]
