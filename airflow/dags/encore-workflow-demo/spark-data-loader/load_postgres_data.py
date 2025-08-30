from pyspark.sql import SparkSession
import os, sys, builtins, functools

# make all prints flush
print = functools.partial(builtins.print, flush=True)

pg_user = os.environ["USERNAME"]
pg_password = os.environ["PASSWORD"]
url = "jdbc:postgresql://pg-postgresql.db.svc.cluster.local:5432/appdb"

props = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver", "ssl": "false"}

spark = SparkSession.builder.appName("ReadPostgresWidgets").getOrCreate()   

df = spark.read.jdbc(url=url, table="widgets", properties=props)
print(f"Row count: {df.count()}")

df.show(truncate=False)
sys.stdout.flush() 

out = df.selectExpr("id", "upper(name) as name_upper", "created_at")
out.show(truncate=False)
sys.stdout.flush()

spark.stop()
