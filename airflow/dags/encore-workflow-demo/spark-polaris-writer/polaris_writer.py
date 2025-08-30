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

# Polaris / Iceberg REST
POLARIS_ALIAS     = env("POLARIS_ALIAS", "polaris")
POLARIS_URI       = env("POLARIS_URI", required=True)  # e.g. https://<host>/api/catalog         # optional; depends on your setup

# OAuth2 (recommended for Snowflake-managed Polaris)
OAUTH_TOKEN_URL     = env("POLARIS_OAUTH2_TOKEN_URL")
OAUTH_CLIENT_ID     = env("POLARIS_OAUTH2_CLIENT_ID")
OAUTH_CLIENT_SECRET = env("POLARIS_OAUTH2_CLIENT_SECRET")
OAUTH_SCOPE         = env("POLARIS_OAUTH2_SCOPE")

TARGET_NAMESPACE  = env("TARGET_NAMESPACE", "spark_maik")
TARGET_TABLE      = env("TARGET_TABLE", "maik_spark_demo")
WRITE_MODE        = env("WRITE_MODE", "append")  # append|overwrite
APP_NAME          = env("APP_NAME", "write-to-polaris")

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# Configure Iceberg REST catalog
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.uri", POLARIS_URI)

# OAuth2 client-credentials (preferred)
if OAUTH_TOKEN_URL and OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET:
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.token.url", OAUTH_TOKEN_URL)
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.client.id", OAUTH_CLIENT_ID)
    spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.client.secret", OAUTH_CLIENT_SECRET)
    if OAUTH_SCOPE:
        spark.conf.set(f"spark.sql.catalog.{POLARIS_ALIAS}.oauth2.scope", OAUTH_SCOPE)

# simple demo dataframe to write
rows = [(1, "maik"), (2, "alex")]
df = spark.createDataFrame(rows, "id INT, name STRING")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {POLARIS_ALIAS}.{TARGET_NAMESPACE}")
full_name = f"{POLARIS_ALIAS}.{TARGET_NAMESPACE}.{TARGET_TABLE}"

if WRITE_MODE.lower() == "overwrite":
    df.writeTo(full_name).overwritePartitions()
else:
    df.writeTo(full_name).append()

log.info(f"Wrote {df.count()} rows to {full_name}")
spark.stop()
