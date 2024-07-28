import duckdb
from dagduck.config import Config, DEFAULT_CONFIG, DEFAULT_CONNECTION
from dagduck.operations import add_column, replace_column, load, join

def create_metadata_table(conn, table_name: str) -> None:
    query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  dst_table VARCHAR PRIMARY KEY,
  source_tables VARCHAR[],
  materialized BOOLEAN,
  meta JSON
)
"""
    conn.execute(query)


def _init(config: Config) -> None:
    create_metadata_table(DEFAULT_CONNECTION, config.metadata_table)


_init(DEFAULT_CONFIG)
