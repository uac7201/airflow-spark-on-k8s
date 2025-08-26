# simple_pyspark_etl.py
# Run with: spark-submit simple_pyspark_etl.py --output /tmp/sales_by_product
# Or with a CSV: spark-submit simple_pyspark_etl.py --input /path/to/sales.csv --output /tmp/out

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

def main():
    parser = argparse.ArgumentParser(description="Simple PySpark ETL: revenue per product")
    parser.add_argument("--input", help="Path to input CSV with columns: date,product,price,quantity")
    parser.add_argument("--output", required=False, default="output/sales_by_product", help="Output folder (Parquet)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("SimplePySparkETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN") 

    if args.input:
        # Expecting a CSV with header and columns: date,product,price,quantity
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(args.input)
        )
    else:
        # Demo data if no input provided
        demo = [
            ("2025-01-01", "Widget", 10.0, 3),
            ("2025-01-02", "Gadget", 12.5, 2),
            ("2025-01-03", "Widget", 10.0, 5),
            ("2025-01-04", "Doodad", 7.0, 10),
        ]
        df = spark.createDataFrame(demo, ["date", "product", "price", "quantity"])

    # Basic sanity filter and calculation
    clean = df.dropna(subset=["product", "price", "quantity"]) \
              .filter((col("price") >= 0) & (col("quantity") >= 0)) \
              .withColumn("revenue", col("price") * col("quantity"))

    by_product = (
        clean.groupBy("product")
        .agg(_sum("revenue").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
    )

    # Show a few rows in the driver logs
    print("Revenue by product:", flush=True)
    print(by_product._jdf.showString(20, 0, False), flush=True)  # (

    # Write results as Parquet
    by_product.write.mode("overwrite").parquet(args.output)
    print(f"Wrote results to: {args.output}")

    spark.stop()

if __name__ == "__main__":
    main()
