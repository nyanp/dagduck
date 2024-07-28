import duckdb
from pydantic.dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    database_file: str = ":memory:"
    metadata_table: str = "metadata"
    row_id_column: str = "_row_id"
    cache_directory: str = "./"
    cache_on_join: bool = True
    max_cascade: int = 1
    eager: bool = True


DEFAULT_CONFIG = Config()

DEFAULT_CONNECTION = duckdb.connect(DEFAULT_CONFIG.database_file)
