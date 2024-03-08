import azure.functions as func
import logging
import io
import polars as pl
import pandas as pd
import datetime

app = func.FunctionApp()

logger = logging.getLogger("blob_trigger")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
import os


@app.blob_trigger(
    arg_name="myblob", path="dailydataingestor", connection="AzureWebJobsStorage"
)
def daily_data_ingestor(myblob: func.InputStream):
    logger.info(
        f"Python blob trigger function processed blob"
        f"Name: {myblob.name}"
        f"Blob Size: {myblob.length} bytes"
    )
    
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_table = os.environ["DB_TABLE"]
    db_name = os.environ["DB_NAME"]
    
    data_file_csv = io.StringIO(myblob.read().decode("utf-8"))
    daily_results_df = pl.read_csv(data_file_csv, has_header=True)
    daily_results_df = daily_results_df.filter(
        pl.any_horizontal(pl.col("Symbol").is_not_null())
    )
    managed_daily_results_df = fix_date_columns_new_format(daily_results_df, "Managed")
    raw_daily_results_df = fix_date_columns_new_format(daily_results_df, "Raw")
    raw_daily_results_df = raw_daily_results_df.unique()
    logger.info(raw_daily_results_df)
    # raw_daily_results_df.write_database(
    #     table_name=db_table,
    #     connection=f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
    #     if_table_exists="append",
    # )



def fix_date_columns_new_format(df: pl.DataFrame, profit_column) -> pl.DataFrame:
    df = df.with_columns(
        (pl.col("Day") + " " + pl.col("Hour") + ":00").alias("Datetime")
    )
    df = df.with_columns(pl.col("Datetime").str.to_datetime("%m-%d-%Y %H:%M:%S"))
    df = df.with_columns(pl.col("Datetime").dt.weekday().alias("WeekdayNumber"))
    df = df.with_columns(pl.col("Datetime").dt.weekday().alias("Weekday"))
    df = df.with_columns(pl.col("Weekday").replace(1, "Monday"))
    df = df.with_columns(pl.col("Weekday").replace(2, "Tuesday"))
    df = df.with_columns(pl.col("Weekday").replace(3, "Wednesday"))
    df = df.with_columns(pl.col("Weekday").replace(4, "Thursday"))
    df = df.with_columns(pl.col("Weekday").replace(5, "Friday"))
    df = df.with_columns(pl.col("Datetime").dt.time().alias("Time"))
    df = df.with_columns((pl.col(profit_column)).alias("Profit"))
    df = df.with_columns((pl.col("Reward") / pl.col("Risk")).alias("Ratio"))
    df = df.with_columns((pl.col("Profit") > 0).alias("Expired"))
    df = df.rename({"Time": "time_of_day"})
    df = df.rename({"Datetime": "datetime"})
    df = df.rename({"Weekday": "weekday"})
    df = df.rename({"WeekdayNumber": "weekdaynumber"})
    df = df.rename({"Symbol": "symbol"})
    df = df.rename({"Price": "price"})
    df = df.rename({"Name": "strategy"})
    df = df.rename({"Premium": "premium"})
    df = df.rename({"Predicted": "predicted"})
    df = df.rename({"Closed": "closed"})
    df = df.rename({"Trade": "trade"})
    df = df.rename({"Risk": "risk"})
    df = df.rename({"Reward": "reward"})
    df = df.rename({"Ratio": "ratio"})
    df = df.rename({"Profit": "profit"})
    df = df.rename({"Expired": "expired"})

    df = df.select(
        [
            "datetime",
            "weekdaynumber",
            "weekday",
            "time_of_day",
            "symbol",
            "price",
            "strategy",
            "premium",
            "predicted",
            "closed",
            "expired",
            "trade",
            "risk",
            "reward",
            "ratio",
            "profit",
        ]
    )
    return df


def fix_date_columns(df: pl.DataFrame) -> pl.DataFrame:
    day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    df = df.with_columns(
        (pl.col("Day") + " " + pl.col("Hour") + ":00").alias("Datetime")
    )
    df = df.with_columns(pl.col("Datetime").str.to_datetime("%m-%d-%Y %H:%M:%S"))
    df = df.with_columns(pl.col("Datetime").dt.weekday().alias("WeekdayNumber"))
    df = df.with_columns(pl.col("Datetime").dt.weekday().alias("Weekday"))
    df = df.with_columns(pl.col("Weekday").replace(1, "Monday"))
    df = df.with_columns(pl.col("Weekday").replace(2, "Tuesday"))
    df = df.with_columns(pl.col("Weekday").replace(3, "Wednesday"))
    df = df.with_columns(pl.col("Weekday").replace(4, "Thursday"))
    df = df.with_columns(pl.col("Weekday").replace(5, "Friday"))

    df = df.with_columns(pl.col("Datetime").dt.time().alias("Time"))

    df = df.rename({"Time": "time_of_day"})
    df = df.rename({"Datetime": "datetime"})
    df = df.rename({"Weekday": "weekday"})
    df = df.rename({"WeekdayNumber": "weekdaynumber"})
    df = df.rename({"Symbol": "symbol"})
    df = df.rename({"Price": "price"})
    df = df.rename({"Name": "strategy"})
    df = df.rename({"Premium": "premium"})
    df = df.rename({"Predicted": "predicted"})
    df = df.rename({"Closed": "closed"})
    df = df.rename({"Expired": "expired"})
    df = df.rename({"Trade": "trade"})
    df = df.rename({"Risk": "risk"})
    df = df.rename({"Reward": "reward"})
    df = df.rename({"Ratio": "ratio"})
    df = df.rename({"Profit": "profit"})

    df = df.select(
        [
            "datetime",
            "weekdaynumber",
            "weekday",
            "time_of_day",
            "symbol",
            "price",
            "strategy",
            "premium",
            "predicted",
            "closed",
            "expired",
            "trade",
            "risk",
            "reward",
            "ratio",
            "profit",
        ]
    )
    return df


def sumemerise_profit_by_wekday_time(df: pl.DataFrame) -> pl.DataFrame:
    df = df.groupby(["weekday", "time_of_day", "symbol", "strategy"]).agg(
        [
            pl.col("profit").sum().alias("TotalProfit"),
            pl.col("profit").mean().alias("AverageProfit"),
            pl.col("profit").count().alias("TotalTrades"),
        ]
    )
    return df

