import json
from typing import Literal

from jinja2 import Template
from pydantic.dataclasses import dataclass
from dagduck.config import Config, DEFAULT_CONFIG


@dataclass(frozen=True)
class Operation:
    operation_type: Literal["add_column", "replace_column", "remove_columns", "filter", "join", "load"]
    operation_arg: dict[str, str]
    update_columns: list[str]


@dataclass(frozen=True)
class Column:
    column_name: str
    column_index: int
    data_type: str
    is_nullable: bool


@dataclass(frozen=True)
class Node:
    dst_table: str
    src_tables: list[str]
    query_template: str  # ふつうにテーブルを生成するためのクエリ
    materialized: bool
    operation: Operation

    def list_columns(self, con):
        return [
            Column(*arg) for arg in
            con.sql(
                f"""
SELECT
    column_name,
    column_index,
    data_type,
    is_nullable
FROM duckdb_columns WHERE table_name = '{self.dst_table}'""").fetchall()
        ]

    @property
    def query(self) -> str:
        parameters = {
            "source": self.src_tables,
            "op": self.operation.operation_arg
        }
        return Template(self.query_template).render(parameters)


class Nodes:
    def __init__(self, con, nodes: list[Node]):
        self._nodes = nodes
        self._name2node = {n.dst_table: n for n in nodes}
        self._con = con

    def __getitem__(self, key: str) -> Node:
        return self._name2node[key]

    @property
    def nodes(self) -> list[Node]:
        return self._nodes

    @classmethod
    def from_metadata(cls, con, config: Config = DEFAULT_CONFIG) -> "Nodes":
        nodes = [Node(**json.loads(meta[0])) for meta in con.sql(f"SELECT meta FROM {config.metadata_table}").fetchall()]
        return Nodes(con, nodes)

    def find_parent(self, node: Node | str) -> list[Node]:
        if isinstance(node, str):
            node = self[node]
        return [self[parent] for parent in node.src_tables]

    def list_columns(self, node: Node | str) -> list[Column]:
        if isinstance(node, str):
            node = self[node]
        return node.list_columns(self._con)

    def has_branch(self, node: Node | str) -> bool:
        if isinstance(node, str):
            node = self[node]
        if len(node.src_tables) == 1:
            return self.has_branch(node.src_tables[0])
        return False
