from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, unix_timestamp, current_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--s3-path",  required=True, help="s3a://bucket/...")
    p.add_argument("--ch-table", required=True, help="database.table")
    return p.parse_args()

def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nasa_asteroids_s3_to_ch")
        .config("spark.hadoop.fs.s3a.endpoint",               os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key",             os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key",             os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload",            "true")
        .config("spark.sql.shuffle.partitions",               "4")
        .getOrCreate()
    )

def transform(df: DataFrame) -> DataFrame:
    """
    Приводим типы под схему ClickHouse.
    close_approach_date оставляем строкой — CH сам приведёт к Date.
    Добавляем updated_at и _version для ReplacingMergeTree.
    """
    return (
        df
        .dropDuplicates(["neo_id", "close_approach_date"])
        .na.drop(subset=["neo_id", "close_approach_date"])
        .withColumn("updated_at", unix_timestamp(current_timestamp()).cast("long"))
        .withColumn("_version",   unix_timestamp(current_timestamp()).cast("long"))
    )

def write_to_clickhouse(df: DataFrame, table: str) -> None:
    ch_url = (
        f"jdbc:clickhouse://{os.environ['CH_HOST']}:"
        f"{os.environ.get('CH_PORT', '8123')}/"
        f"{os.environ.get('CH_DATABASE', 'nasa')}"
    )

    count = df.count()
    log.info("Запись %d строк → ClickHouse[%s]", count, table)

    (
        df
        .repartition(4)
        .write
        .format("jdbc")
        .option("url",            ch_url)
        .option("dbtable",        table)
        .option("user",           os.environ.get("CH_USER", "default"))
        .option("password",       os.environ.get("CH_PASSWORD", ""))
        .option("driver",         "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize",      "100000")
        .option("numPartitions",  "4")
        .option("socket_timeout", "300000")
        .mode("append")
        .save()
    )

    log.info("Записано: %d строк", count)


def main():
    args = parse_args()
    log.info("s3_path=%s  ch_table=%s", args.s3_path, args.ch_table)

    spark = create_session()
    try:
        df_raw = spark.read.parquet(args.s3_path)
        df_raw.printSchema()
        log.info("Строк прочитано: %d", df_raw.count())

        df_clean = transform(df_raw)
        write_to_clickhouse(df_clean, args.ch_table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()