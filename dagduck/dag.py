import json
import os
from dataclasses import asdict, replace
from typing import Any

from dagduck.config import Config, DEFAULT_CONFIG
from dagduck.struct import Node, Nodes


def create_cache_query_if_required(node: Node, nodes: Nodes, config: Config = DEFAULT_CONFIG) -> tuple[
    str | None, str | None]:
    if len(node.src_tables) > 1 and config.cache_on_join:
        return _full_cache_query(node, config)

    ancestor, n_cascade = _get_materialized_ancestor(node, nodes)
    if not ancestor or n_cascade <= config.max_cascade:
        return None, None

    ancestor_columns = [c.column_name for c in nodes.list_columns(ancestor)]

    all_columns = [c.column_name for c in nodes.list_columns(node) if c.column_name != "_row_id"]

    not_materialized_columns = [c for c in all_columns if c not in ancestor_columns]

    if not not_materialized_columns:
        return None, None

    filename = _cache_path(node, config)
    cache_write_query = f"""
COPY (SELECT {config.row_id_column}, {', '.join(not_materialized_columns)} FROM {node.dst_table}) TO '{filename}'
    """
    cache_read_query = f"""
SELECT
    l.{config.row_id_column},
    {', '.join(all_columns)}
FROM '{filename}' l
LEFT JOIN {ancestor.dst_table} r ON l._row_id = r._row_id
    """
    return cache_write_query, cache_read_query


def construct_node(conn, node: Node, config: Config = DEFAULT_CONFIG):
    conn.sql(f"CREATE OR REPLACE VIEW {node.dst_table} AS ({node.query});")

    cache_write_query, cache_read_query = create_cache_query_if_required(node, Nodes.from_metadata(conn))
    if cache_write_query and cache_read_query:
        conn.sql(cache_write_query)
        conn.sql(f"CREATE OR REPLACE VIEW {node.dst_table} AS ({cache_read_query});")
        node = replace(node, materialized=True)

    conn.sql(f"DELETE FROM {config.metadata_table} WHERE dst_table = '{node.dst_table}';")
    conn.sql(f"""
INSERT INTO {config.metadata_table}
VALUES
  ({_to_value(node.dst_table)},
  {_to_value(node.src_tables)},
  {node.materialized},
  {_to_value(json.dumps(asdict(node)))});
"""
             )

    return node


def _get_materialized_ancestor(node: Node, nodes: Nodes, n_cascade: int = 0) -> tuple[Node | None, int]:
    if node.materialized:
        return node, n_cascade

    if len(node.src_tables) == 1:
        return _get_materialized_ancestor(nodes[node.src_tables[0]], nodes, n_cascade + 1)

    return None, 0


def _cache_path(node: Node, config: Config) -> str:
    return os.path.join(config.cache_directory, node.dst_table + "_cache.parquet")


def _full_cache_query(node: Node, config: Config) -> tuple[str, str]:
    filename = _cache_path(node, config)
    return f"COPY (SELECT * FROM {node.dst_table}) TO '{filename}'", f"SELECT * FROM '{filename}'"


def _escape(src: Any) -> Any:
    if isinstance(src, str):
        return src.replace("'", "''")
    if isinstance(src, list):
        return [_escape(item) for item in src]
    if isinstance(src, dict):
        return {k: _escape(v) for k, v in src.items()}
    return src


def _to_value(v: Any) -> str:
    v = _escape(v)
    if v is None:
        return "NULL"

    if isinstance(v, str):
        return f"'{v}'"
    return str(v)
