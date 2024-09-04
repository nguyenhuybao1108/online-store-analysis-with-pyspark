from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Read the CSV file
csv_path = "/Users/baonguyen/Downloads/Project for CV/data/2019-Oct.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Convert 'event_time' to timestamp if it's not already
df = df.withColumn("event_time", to_timestamp("event_time"))

# Write as Parquet without partitioning
parquet_path = "/Users/baonguyen/Downloads/Project for CV/data/2019-Oct.parquet"
df.write.mode("overwrite").parquet(parquet_path)

# Stop the Spark session
spark.stop()
