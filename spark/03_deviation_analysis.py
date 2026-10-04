from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    abs,
    col,
    lit,
    when
)


spark = (
    SparkSession.builder
    .appName("GridGuard-Deviation-Analysis")
    .master("local[2]")
    .getOrCreate()
)


input_path = "data/processed/meter_features"
output_path = "data/processed/meter_deviation"


# --------------------------------------------------
# 1. Read feature data
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("\n===== INPUT FEATURES =====")
df.show(truncate=False)


# --------------------------------------------------
# 2. Calculate overall average consumption
# --------------------------------------------------

overall_average = (
    df.select(
        avg("avg_consumption").alias(
            "overall_avg_consumption"
        )
    )
    .collect()[0]["overall_avg_consumption"]
)


print(
    "\nOverall average consumption:",
    overall_average
)


# --------------------------------------------------
# 3. Historical deviation
#
# For the development dataset we use the
# overall baseline.
#
# Later this will be replaced with each
# meter's historical baseline.
# --------------------------------------------------

df = df.withColumn(
    "historical_deviation",
    when(
        lit(overall_average) != 0,
        abs(
            col("avg_consumption")
            - lit(overall_average)
        ) / lit(overall_average)
    ).otherwise(0)
)


# --------------------------------------------------
# 4. Peer baseline
#
# Current prototype:
# peer baseline = overall average.
#
# Later we will calculate peers based on:
# - zone
# - consumer type
# - consumption profile
# --------------------------------------------------

df = df.withColumn(
    "peer_average_consumption",
    lit(overall_average)
)


df = df.withColumn(
    "peer_deviation",
    when(
        col("peer_average_consumption") != 0,
        abs(
            col("avg_consumption")
            - col("peer_average_consumption")
        )
        / col("peer_average_consumption")
    ).otherwise(0)
)


# --------------------------------------------------
# 5. Display results
# --------------------------------------------------

result = df.select(
    "meter_id",
    "avg_consumption",
    "historical_deviation",
    "peer_average_consumption",
    "peer_deviation"
)


print("\n===== DEVIATION ANALYSIS =====")

result.show(
    truncate=False
)


# --------------------------------------------------
# 6. Save results
# --------------------------------------------------

result.write \
    .mode("overwrite") \
    .parquet(output_path)


print(
    "\nDeviation data written to:",
    output_path
)


spark.stop()