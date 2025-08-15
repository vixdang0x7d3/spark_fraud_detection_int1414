import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pyspark
    from pyspark.sql import SparkSession
    return SparkSession, mo, pyspark


@app.cell
def _(pyspark):
    pyspark.__version__
    return


@app.cell
def _(SparkSession):
    sc = (
        SparkSession.builder
        .appName("Test")
        .master("local[*]")
        .getOrCreate()
    )
    return (sc,)


@app.cell
def _(sc):
    datasrc = "/home/saladass/crafts/int-1414-final/data/dev_set/chunk_1/adults_2550_female_rural_000-199.csv"
    df = (
        sc.read
        .option("sep", "|")
        .option("header", "true")
        .option("inferSchema", "true")
    ).csv(datasrc)
    return (df,)


@app.cell
def _(df):
    df.createOrReplaceTempView('fraud_sample')
    df.show()
    return


@app.cell
def _(sc):
    import ibis
    con = ibis.pyspark.connect(sc)
    return (con,)


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        select * from fraud_sample
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        WITH histogram_stats AS (
          SELECT 
            MIN(amt) as min_amt,
            MAX(amt) as max_amt,
            PERCENTILE_APPROX(amt, 0.5) as median_amt
          FROM fraud_sample
          WHERE amt IS NOT NULL
        ),
        binned_data AS (
          SELECT 
            amt,
            FLOOR((amt - h.min_amt) / ((h.max_amt - h.min_amt) / 20)) as bin_id,
            h.min_amt + FLOOR((amt - h.min_amt) / ((h.max_amt - h.min_amt) / 20)) * ((h.max_amt - h.min_amt) / 20) as bin_start,
            (h.max_amt - h.min_amt) / 20 as bin_width
          FROM fraud_sample f
          CROSS JOIN histogram_stats h
          WHERE amt IS NOT NULL
        )
        SELECT 
          bin_id,
          ROUND(bin_start, 2) as range_start,
          ROUND(bin_start + bin_width, 2) as range_end,
          COUNT(*) as frequency,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM binned_data
        GROUP BY bin_id, bin_start, bin_width
        ORDER BY bin_id;
        """,
        engine=con
    )
    return


app._unparsable_cell(
    r"""
    full_df = df.toPandas()
    full_df.head()

    features = 
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
