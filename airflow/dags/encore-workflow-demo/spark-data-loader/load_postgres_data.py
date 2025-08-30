from pyspark.sql import SparkSession
import os

# Pull credentials from environment
pg_user = os.environ["USERNAME"]      # New: user from env var
pg_password = os.environ["PASSWORD"]

# JDBC URL to Postgres service in AKS
url = "jdbc:postgresql://postgres.db.svc.cluster.local:5432/db"

# Connection properties
props = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver",
    "ssl": "false"
}

spark = SparkSession.builder \
    .appName("ReadPostgresWidgets") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.jdbc(url=url, table="widgets", properties=props)
print("Row count:", df.count())
df.show(truncate=False)   # print the rows to stdout

# Example: do something minor and write back as a demo
out = df.selectExpr("id", "upper(name) as name_upper", "created_at")
out.show(truncate=False)

spark.stop()
