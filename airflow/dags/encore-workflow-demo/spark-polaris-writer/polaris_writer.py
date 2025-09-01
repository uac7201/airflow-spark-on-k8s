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
INPUT_PATH         = env("INPUT_PATH", required=True)     # e.g. /shared/encore/tmp/widgets
POLARIS_ALIAS      = env("POLARIS_ALIAS", "polaris")
POLARIS_URI        = env("POLARIS_URI", required=True)

OAUTH_TOKEN_URL     = env("POLARIS_OAUTH2_TOKEN_URL")
OAUTH_CLIENT_ID     = env("POLARIS_OAUTH2_CLIENT_ID")
OAUTH_CLIENT_SECRET = env("POLARIS_OAUTH2_CLIENT_SECRET")
OAUTH_SCOPE         = env("POLARIS_OAUTH2_SCOPE")

TARGET_NAMESPACE   = env("TARGET_NAMESPACE", "spark_maik")
TARGET_TABLE       = env("TARGET_TABLE", "maikspark_demo")
WRITE_MODE         = env("WRITE_MODE", "append").lower()  # append | overwrite
APP_NAME           = env("APP_NAME", "write-to-polaris")

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# Polaris (Iceberg REST catalog)# Spark catalog config (keep your alias)
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.type", "rest")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.uri", POLARIS_URI)

# ✅ use the REST auth keys Iceberg expects
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.rest.auth.type", "oauth2")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2-server-uri", OAUTH_TOKEN_URL)

# client credentials are passed via the single 'credential' key as "<client_id>:<client_secret>"
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.credential", f"{OAUTH_CLIENT_ID}:{OAUTH_CLIENT_SECRET}")

# optional but common with Polaris
if OAUTH_SCOPE:
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.scope", OAUTH_SCOPE)
aud = os.getenv("POLARIS_OAUTH2_AUDIENCE")
if aud:
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.audience", aud)

# --- OR: static bearer token instead of client creds ---
TOKEN = os.getenv("POLARIS_BEARER_TOKEN")
if TOKEN:
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.rest.auth.type", "token")
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.token", TOKEN)

full_table = f"{POLARIS_ALIAS}.{TARGET_NAMESPACE}.{TARGET_TABLE}"

# Read parquet from job 1
log.info(f"Reading input parquet from: {INPUT_PATH}")
df = spark.read.parquet(INPUT_PATH)
cnt = df.count()
log.info(f"Read {cnt} rows from parquet")

# Ensure namespace
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {POLARIS_ALIAS}.{TARGET_NAMESPACE}")

# Robust table existence check
exists = spark.sql(
    f"SHOW TABLES IN {POLARIS_ALIAS}.{TARGET_NAMESPACE} LIKE '{TARGET_TABLE}'"
).count() > 0

if not exists:
    writer = df.writeTo(full_table).using("iceberg").tableProperty("format-version", "2")
    if WRITE_MODE == "overwrite":
        writer.createOrReplace()    # creates table and writes df
        log.info(f"Created/replaced table {full_table} with {cnt} rows")
    else:
        writer.create()             # creates table and writes df
        log.info(f"Created table {full_table} with {cnt} rows")
else:
    if WRITE_MODE == "overwrite":
        # dynamic partition overwrite (works for partitioned tables; for unpartitioned it replaces contents)
        df.writeTo(full_table).overwritePartitions()
        log.info(f"Overwrote partitions in {full_table} with {cnt} rows")
    else:
        df.writeTo(full_table).append()
        log.info(f"Appended {cnt} rows to {full_table}")

spark.stop()
