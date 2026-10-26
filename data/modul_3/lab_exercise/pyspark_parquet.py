from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StorageExample").getOrCreate()

# Load CSV (menggunakan dataset EHR pasien yang diupload ke HDFS)
df = spark.read.csv("hdfs:///user/student/data.csv", header=True, inferSchema=True)

# Save as Parquet (columnar storage)
df.write.mode("overwrite").parquet("hdfs:///user/student/data_parquet")

# Read back Parquet file
df_parquet = spark.read.parquet("hdfs:///user/student/data_parquet")
df_parquet.show(5)

spark.stop()
