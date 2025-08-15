import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb

    from pyspark.sql import SparkSession
    from pyspark.sql import types as T
    from pyspark.sql import functions as F

    from pyspark.ml.feature import PCA
    from pyspark.ml.feature import OneHotEncoder


    return duckdb, mo


@app.cell
def _(duckdb):
    conn = duckdb.connect()
    return (conn,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT *, 
        FROM read_parquet('data/devset_tmp_3/**/*.parquet',
            hive_partitioning=true,
            union_by_name=true);
        """,
        engine=conn
    )
    return


@app.cell
def _(conn):
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    return


@app.cell
def _(conn):
    conn.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return


@app.cell
def _(conn):
    path = "s3a://spark-data/fraud-data/**/*.parquet"
    df = conn.execute(f"select * from read_parquet('{path}')").fetch_df()
    return (df,)


@app.cell
def _(df):
    df
    return


if __name__ == "__main__":
    app.run()
