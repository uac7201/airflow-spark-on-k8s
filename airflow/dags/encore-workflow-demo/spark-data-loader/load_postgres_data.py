# load_postgres_data.py
from pyspark.sql import SparkSession
import os, sys, builtins, functools
print = functools.partial(builtins.print, flush=True)

pg_user = os.environ["USERNAME"]
pg_password = os.environ["PASSWORD"]
pg_table_name = os.environ["POSTGRES_TABLE_NAME"]

url = "jdbc:postgresql://pg-postgresql.db.svc.cluster.local:5432/appdb"
props = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver", "ssl": "false"}

# path passed via env (a *directory* for Parquet)
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/shared/encore/tmp/widgets")

spark = SparkSession.builder.appName("ReadPostgresWidgets").getOrCreate()

df = spark.read.jdbc(url=url, table=pg_table_name, properties=props)
print(f"Row count: {df.count()}")
df.show(truncate=False); sys.stdout.flush()

out = df.selectExpr("id", "upper(name) as name_upper", "created_at")
out.show(truncate=False); sys.stdout.flush()

# write a partitioned Parquet *directory*
out.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Wrote parquet to {OUTPUT_PATH}")
spark.stop()
