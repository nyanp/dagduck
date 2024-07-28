from typing import Literal

from dagduck.config import Config, DEFAULT_CONFIG, DEFAULT_CONNECTION
from dagduck.dag import construct_node
from dagduck.struct import Node, Operation


def load(dst_table: str, source_file: str, conn=DEFAULT_CONNECTION, config: Config = DEFAULT_CONFIG) -> Node:
    op = Operation("load", {"source": source_file}, [])

    query = f"SELECT row_number() OVER () as {config.row_id_column}, * FROM '{{{{op.source}}}}'"

    return _construct_if_eager(Node(dst_table, [], query, True, op), conn, config)


def add_column(dst_table: str, source_table: str, query: str, dst_column: str, conn=DEFAULT_CONNECTION,
               config: Config = DEFAULT_CONFIG) -> Node:
    op = Operation("add_column", {"query": query, "dst_column": dst_column}, [dst_column])

    query = "SELECT *, ({{op.query}}) AS {{op.dst_column}} FROM {{source[0]}}"

    return _construct_if_eager(Node(dst_table, [source_table], query, False, op), conn, config)


def replace_column(dst_table: str, source_table: str, query: str, target_column: str, conn=DEFAULT_CONNECTION,
                   config: Config = DEFAULT_CONFIG) -> Node:
    op = Operation("replace_column", {"query": query, "target_column": target_column}, [target_column])

    query = "SELECT * EXCLUDE({{op.target_column}}), ({{op.query}}) AS {{op.target_column}} FROM {{source[0]}}"

    return _construct_if_eager(Node(dst_table, [source_table], query, False, op), conn, config)


def join(dst_table: str, left_table: str, right_table: str, on: str,
         how: Literal["LEFT", "INNER", "OUTER"], conn=DEFAULT_CONNECTION, config: Config = DEFAULT_CONFIG) -> Node:
    op = Operation("join", {"on": on, "how": how}, ["*"])

    query = f"""
SELECT
  l.*,
  r.* EXCLUDE({config.row_id_column}, {{{{op.on}}}})
FROM
""" + "{{source[0]}} l {{op.how}} JOIN {{source[1]}} r ON (l.{{op.on}} = r.{{op.on}})"

    return _construct_if_eager(Node(dst_table, [left_table, right_table], query, False, op), conn, config)


def _construct_if_eager(node: Node, conn=DEFAULT_CONNECTION, config: Config = DEFAULT_CONFIG):
    if config.eager:
        return construct_node(conn, node, config)
    else:
        return node
