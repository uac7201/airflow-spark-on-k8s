#!/usr/bin/env python3
import os, sys, logging
from pyspark.sql import SparkSession

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("write-to-polaris")

def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

# ==== Inputs (env) ====
INPUT_PATH         = env("INPUT_PATH", required=True)            # e.g. /shared/encore/tmp/widgets/{{ ts_nodash }}
POLARIS_ALIAS      = env("POLARIS_ALIAS", "polaris")
POLARIS_URI        = env("POLARIS_URI", required=True)

OAUTH_TOKEN_URL     = env("POLARIS_OAUTH2_TOKEN_URL")
OAUTH_CLIENT_ID     = env("POLARIS_OAUTH2_CLIENT_ID")
OAUTH_CLIENT_SECRET = env("POLARIS_OAUTH2_CLIENT_SECRET")
OAUTH_SCOPE         = env("POLARIS_OAUTH2_SCOPE")

TARGET_NAMESPACE   = env("TARGET_NAMESPACE", "spark_maik")
TARGET_TABLE       = env("TARGET_TABLE", "maikspark_demo")
WRITE_MODE         = env("WRITE_MODE", "append").lower()         # append | overwrite
APP_NAME           = env("APP_NAME", "write-to-polaris")

# ==== Spark session ====
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# Configure Polaris (Iceberg REST catalog)
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.uri", POLARIS_URI)

if OAUTH_TOKEN_URL and OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET:
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.token.url", OAUTH_TOKEN_URL)
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.client.id", OAUTH_CLIENT_ID)
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.client.secret", OAUTH_CLIENT_SECRET)
    if OAUTH_SCOPE:
        spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.scope", OAUTH_SCOPE)

full_table = f"{POLARIS_ALIAS}.{TARGET_NAMESPACE}.{TARGET_TABLE}"

# Read the parquet produced by job 1
log.info(f"Reading input parquet from: {INPUT_PATH}")
df = spark.read.parquet(INPUT_PATH)
cnt = df.count()
log.info(f"Read {cnt} rows from parquet")

# Ensure namespace exists
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {POLARIS_ALIAS}.{TARGET_NAMESPACE}")

# Decide create/append/overwrite
table_exists = spark.catalog.tableExists(full_table)

if not table_exists:
    # Create table with the schema of df
    # Use Iceberg v2 by default (optional)
    writer = df.writeTo(full_table).using("iceberg").tableProperty("format-version", "2")
    if WRITE_MODE == "overwrite":
        # First creation replaces if left from prior partial runs
        writer.createOrReplace()
        log.info(f"Created (or replaced) table {full_table} with {cnt} rows")
    else:
        writer.create()
        # Then append the initial batch
        df.writeTo(full_table).append()
        log.info(f"Created table {full_table} and appended {cnt} rows")
else:
    if WRITE_MODE == "overwrite":
        # Dynamic partition overwrite (safe with partitioned/unpartitioned tables)
        (df.writeTo(full_table)
           .option("overwrite-mode", "dynamic")
           .overwritePartitions())
        log.info(f"Overwrote partitions in {full_table} with {cnt} rows")
    else:
        df.writeTo(full_table).append()
        log.info(f"Appended {cnt} rows to {full_table}")

spark.stop()
