import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("GridGuard")
        .getOrCreate()
    )

    print("=" * 50)
    print("GridGuard Spark Test")
    print("=" * 50)

    print("Spark Version:", spark.version)
    print("Application:", spark.sparkContext.appName)

    data = [
        ("M001", 10.5),
        ("M002", 20.3),
        ("M003", 15.7),
        ("M004", 31.2),
    ]

    df = spark.createDataFrame(
        data,
        ["meter_id", "consumption"]
    )

    print("\nSample meter data:")
    df.show()

    print("Average consumption:")

    df.groupBy().avg("consumption").show()

    spark.stop()


if __name__ == "__main__":
    main()