#!/usr/bin/env python3
# ingest_postgres_to_polaris.py
"""
Generic Postgres -> Polaris (Iceberg REST) ingest.

Required env (source):
  JDBC_URL                    e.g. jdbc:postgresql://host:5432/dbname
  JDBC_USERNAME
  JDBC_PASSWORD
  (choose one)
    JDBC_TABLE                e.g. public.orders
    JDBC_QUERY                e.g. SELECT * FROM public.orders WHERE ...

Optional incremental window (adds WHERE when using JDBC_TABLE):
  INCR_COLUMN                 e.g. updated_at
  INCR_START                  ISO8601 (e.g. 2025-08-24T00:00:00Z)
  INCR_END                    ISO8601 (e.g. 2025-08-25T00:00:00Z)

Parallel read (optional, improves throughput):
  JDBC_PARTITION_COLUMN       numeric or timestamp column for partitioning
  JDBC_LOWER_BOUND            inclusive lower bound (as integer or epoch sec)
  JDBC_UPPER_BOUND            inclusive upper bound (as integer or epoch sec)
  JDBC_NUM_PARTITIONS         e.g. 8
  JDBC_FETCHSIZE              e.g. 10000
  JDBC_EXTRAS_JSON            JSON object of extra jdbc() .option(...) pairs

Destination (Polaris / Iceberg REST):
  POLARIS_ALIAS               Spark catalog alias (default: polaris)
  POLARIS_URI                 REST endpoint, e.g. https://polaris.example.com/api/catalog
  POLARIS_WAREHOUSE           Optional warehouse name/path if your catalog needs it

Auth (pick what your Polaris/REST catalog supports; all optional):
  # OAuth2 client-credentials (Iceberg REST supports these keys)
  POLARIS_OAUTH2_TOKEN_URL
  POLARIS_OAUTH2_CLIENT_ID
  POLARIS_OAUTH2_CLIENT_SECRET
  POLARIS_OAUTH2_SCOPE        (optional)

Target table:
  TARGET_NAMESPACE            e.g. bronze
  TARGET_TABLE                e.g. orders
  WRITE_MODE                  append | overwrite   (default: append)

General Spark tuning (optional):
  SPARK_SQL_SHUFFLE_PARTITIONS   e.g. 200
  REPARTITION                     integer; if set, df.repartition(N) before write

JARs you’ll need at submit time (examples for Spark 3.5 / Scala 2.12):
  --packages org.postgresql:postgresql:42.7.4,\
             org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1
"""

import json
import os
import sys
import logging
from typing import Dict, Any

from pyspark.sql import SparkSession

# -------- helpers --------
log = logging.getLogger()
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing required env: {name}")
    return v


# -------- read config from env --------
# Source
JDBC_URL = env("JDBC_URL", required=True)
JDBC_USER = env("JDBC_USERNAME", required=True)
JDBC_PASSWORD = env("JDBC_PASSWORD", required=True)
JDBC_DRIVER = env("JDBC_DRIVER", "org.postgresql.Driver")

JDBC_TABLE = env("JDBC_TABLE", required=True)
JDBC_QUERY = env("JDBC_QUERY", required=True)
APP_NAME = env("APP_NAME", "postgres-to-polaris")


spark = SparkSession.builder.appName("APP_NAME").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# -------- print env variables --------
log.info("Environment configuration:")
log.info(f"APP_NAME: {APP_NAME}")
log.info(f"JDBC_URL: {JDBC_URL}")
log.info(f"JDBC_USERNAME: {JDBC_USER}")
log.info(f"JDBC_PASSWORD: {'***' if JDBC_PASSWORD else '(not set)'}")
log.info(f"JDBC_DRIVER: {JDBC_DRIVER}")
log.info(f"JDBC_TABLE: {JDBC_TABLE}")
log.info(f"JDBC_QUERY: {JDBC_QUERY}")


log.info(f"Start extracting infomation from postgres...")
# todo

log.info(f"Updating Polaris catalog & write parquet file to data lake...")
# todo

log.info(f"Done.")
