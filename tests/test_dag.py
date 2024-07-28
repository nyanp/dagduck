import os
import tempfile
import pandas as pd
import numpy as np

from dagduck import load, add_column, join, DEFAULT_CONNECTION


def test_dag_from_parquet_single_node():
    with tempfile.TemporaryDirectory() as dname:
        conn = DEFAULT_CONNECTION
        parquet_path = os.path.join(dname, "test.parquet")

        n_rows = 100000
        df = pd.DataFrame({
            "idx": np.arange(n_rows),
            "rnd": np.random.randint(0, 100, size=n_rows)
        })
        df.to_parquet(parquet_path)

        node = load("pq", parquet_path, conn)
        assert node.dst_table == "pq"
        assert node.src_tables == []
        assert node.operation.operation_type == "load"
        assert node.materialized

        df_dst = conn.sql(f"SELECT * FROM {node.dst_table}").df()
        assert len(df_dst) == len(df)


def test_dag_from_parquet_chain():
    with tempfile.TemporaryDirectory() as dname:
        conn = DEFAULT_CONNECTION
        parquet_path = os.path.join(dname, "test.parquet")

        n_rows = 100000
        df = pd.DataFrame({
            "idx": np.arange(n_rows),
            "rnd": np.random.randint(0, 100, size=n_rows)
        })
        df.to_parquet(parquet_path)

        load("src", parquet_path, conn)
        t1 = add_column("t1", "src", "2 * rnd", "rnd2", conn)
        t2 = add_column("t2", "t1", "row_number() over()", "rownum", conn)

        df_dst = conn.sql(f"SELECT * FROM {t2.dst_table}").df()
        assert df_dst.columns.tolist() == ["_row_id", "idx", "rnd", "rnd2", "rownum"]

        assert not t1.materialized
        assert t2.materialized

        assert pd.read_parquet(t2.dst_table + "_cache.parquet").columns.tolist() == ["_row_id", "rnd2", "rownum"]


def test_dag_from_parquet_join():
    with tempfile.TemporaryDirectory() as dname:
        conn = DEFAULT_CONNECTION

        n_rows = 100
        pd.DataFrame({
            "idx": np.arange(n_rows),
            "x": np.random.choice([False, True], size=n_rows)
        }).to_parquet(os.path.join(dname, "master.parquet"))

        pd.DataFrame({
            "idx": np.random.randint(0, n_rows, 10000),
            "y": np.random.randint(0, 1000, 10000)
        }).to_parquet(os.path.join(dname, "trn.parquet"))

        load("master", os.path.join(dname, "master.parquet"), conn)
        load("trn", os.path.join(dname, "trn.parquet"), conn)

        joined = join("joined", "trn", "master", on="idx", how="LEFT", conn=conn)

        df_dst = conn.sql(f"SELECT * FROM {joined.dst_table}").df()

        assert len(df_dst) == 10000
        assert list(df_dst.columns) == ["_row_id", "idx", "y", "x"]
