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
INPUT_PATH   = env("INPUT_PATH", required=True)                 # e.g. /shared/encore/tmp/widgets
POLARIS_ALIAS= env("POLARIS_ALIAS", "polaris")

# Accept either a full URI or an account identifier
POLARIS_URI  = env("POLARIS_URI")                               # e.g. https://<acct>.snowflakecomputing.com/polaris/api/catalog
CATALOG_ID   = env("POLARIS_CATALOG_ACCOUNT_IDENTIFIER", "rngtjdl-polaris")        # e.g. rngtjdl-polaris
if not POLARIS_URI and CATALOG_ID:
    POLARIS_URI = f"https://{CATALOG_ID}.snowflakecomputing.com/polaris/api/catalog"
if not POLARIS_URI:
    raise RuntimeError("Provide POLARIS_URI or POLARIS_CATALOG_ACCOUNT_IDENTIFIER")

# OAuth2 client-credentials (names match your YAML)
OAUTH_CLIENT_ID     = env("POLARIS_OAUTH2_CLIENT_ID", required=True)
OAUTH_CLIENT_SECRET = env("POLARIS_OAUTH2_CLIENT_SECRET", required=True)
OAUTH_SCOPE         = env("POLARIS_OAUTH2_SCOPE", "PRINCIPAL_ROLE:ALL")

# Optional extras
POLARIS_WAREHOUSE   = env("POLARIS_WAREHOUSE")                  # only if your catalog needs it

TARGET_NAMESPACE = env("TARGET_NAMESPACE", "spark_maik")
TARGET_TABLE     = env("TARGET_TABLE", "maikspark_demo")
WRITE_MODE       = env("WRITE_MODE", "append").lower()          # append | overwrite
APP_NAME         = env("APP_NAME", "write-to-polaris")

# ===== Spark session (packages are better set via sparkConf in the YAML) =====
builder = (SparkSession.builder
    .appName(APP_NAME)
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", POLARIS_ALIAS)
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}.type", "rest")
    # Polaris expects this header when it will vend cloud credentials to Iceberg
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}.header.X-Iceberg-Access-Delegation", "vended-credentials")
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}.uri", POLARIS_URI)
    # Iceberg OAuth2: pass client creds and scope. (No token URL needed for Polaris.)
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}.credential", f"{OAUTH_CLIENT_ID}:{OAUTH_CLIENT_SECRET}")
    .config(f"spark.sql.catalog.{POLARIS_ALIAS}.scope", OAUTH_SCOPE)
)

if POLARIS_WAREHOUSE:
    builder = builder.config(f"spark.sql.catalog.{POLARIS_ALIAS}.warehouse", POLARIS_WAREHOUSE)

spark = builder.getOrCreate()

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
        writer.createOrReplace()
        log.info(f"Created/replaced table {full_table} with {cnt} rows")
    else:
        writer.create()
        log.info(f"Created table {full_table} with {cnt} rows")
else:
    if WRITE_MODE == "overwrite":
        df.writeTo(full_table).overwritePartitions()
        log.info(f"Overwrote partitions in {full_table} with {cnt} rows")
    else:
        df.writeTo(full_table).append()
        log.info(f"Appended {cnt} rows to {full_table}")

spark.stop()
