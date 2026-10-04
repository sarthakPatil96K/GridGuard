from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


spark = (
    SparkSession.builder
    .appName("GridGuard-Data-Ingestion")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/sample/smart_meter_sample.csv"
output_path = "data/parquet/smart_meter"


df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)


df = df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)


print("\n===== SCHEMA =====")
df.printSchema()


print("\n===== SAMPLE DATA =====")
df.show(10, truncate=False)


print("\n===== RECORD COUNT =====")
print(df.count())


print("\n===== METER COUNT =====")
print(df.select("meter_id").distinct().count())


df.write.mode("overwrite").parquet(output_path)


print("\nParquet data written to:")
print(output_path)


spark.stop()