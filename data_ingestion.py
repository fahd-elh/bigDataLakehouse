# # 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from delta import configure_spark_with_delta_pip

def main():
    # Configure Spark with Delta Lake
    builder = SparkSession.builder \
        .appName("IngestToLakehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Initialize Spark with Delta Lake support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Define paths
    RAW_BASE = "./data_lake/raw"
    WAREHOUSE_BASE = "./data_lake/warehouse"

    sensor_types = ["temperature", "vibration", "pressure"]

    for sensor in sensor_types:
        raw_path = os.path.join(RAW_BASE, sensor)
        
        # Check if directory exists
        if not os.path.exists(raw_path):
            print(f"[WARN] Directory not found for {sensor}: {raw_path}")
            continue
            
        files = sorted([f for f in os.listdir(raw_path) if f.endswith(".json")])

        if not files:
            print(f"[INFO] No files found for {sensor}")
            continue

        print(f"[INFO] Loading files for {sensor}...")

        try:
            # Read JSON files
            df = spark.read.json([os.path.join(raw_path, f) for f in files])

            # Basic validation
            required_cols = ["sensor_id", "type", "value", "unit", "site", "machine", "timestamp"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"[ERROR] Missing columns in {sensor}: {', '.join(missing_cols)}")
                continue

            # Convert timestamp
            df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

            # Add partition columns
            df = df.withColumn("year", year("timestamp")) \
                   .withColumn("month", month("timestamp")) \
                   .withColumn("day", dayofmonth("timestamp"))

            # Define Delta output path
            delta_path = os.path.join(WAREHOUSE_BASE, sensor)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(delta_path), exist_ok=True)

            # Write as Delta with partitioning
            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("site", "year", "month", "day") \
                .save(delta_path)

            print(f"[SUCCESS] {sensor} integrated into Lakehouse ✅")

        except Exception as e:
            print(f"[ERROR] Failed to process {sensor}: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()

