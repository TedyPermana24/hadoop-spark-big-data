"""
================================================================================
SPEED LAYER - Spark Structured Streaming + Apache Kafka
================================================================================
Tujuan:
1. Mengonsumsi stream data tanda vital monitor ICU dari Kafka Topic 'icu-vitals'.
2. Menghitung Early Warning Score (MEWS) untuk indikasi dini Sepsis & Hipoksia.
3. Menghasilkan Immediate Alerts real-time untuk klinisi/dokter ICU.
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    print("=" * 75)
    print(" [SPEED LAYER] SPARK STRUCTURED STREAMING + KAFKA ICU ALERT ENGINE")
    print("=" * 75)

    spark = SparkSession.builder \
        .appName("Hospital-ICU-Kafka-Alerts") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Skema JSON data sensor ICU dari Kafka
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("bed_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("patient_name", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("spo2", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("respiratory_rate", IntegerType(), True),
        StructField("systolic_bp", IntegerType(), True),
        StructField("condition", StringType(), True)
    ])

    print("[+] Menghubungkan Spark Structured Streaming ke Kafka broker 'kafka:9092'...")
    print("[+] Berlangganan topic 'icu-vitals'...")

    # Membaca stream dari Apache Kafka
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "icu-vitals") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse data JSON dari kolom 'value'
    vitals_stream = kafka_raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    # Evaluasi Early Warning Score (MEWS) untuk deteksi Sepsis
    evaluated_stream = vitals_stream.withColumn(
        "sepsis_score",
        (when(col("heart_rate") > 110, 2).otherwise(0) +
         when(col("spo2") < 90, 3).otherwise(when(col("spo2") < 94, 1).otherwise(0)) +
         when(col("temperature") > 38.5, 2).otherwise(0) +
         when(col("systolic_bp") < 90, 2).otherwise(0) +
         when(col("respiratory_rate") > 24, 2).otherwise(0))
    ).withColumn(
        "alert_status",
        when(col("sepsis_score") >= 6, "CRITICAL SEPSIS ALERT (IMMEDIATE ACTION)")
        .when(col("sepsis_score") >= 4, "HIGH RISK WARNING")
        .otherwise("STABLE")
    )

    # Filter hanya kondisi kritis/peringatan untuk Immediate Alerts ke dokter
    urgent_alerts = evaluated_stream.filter(col("sepsis_score") >= 4) \
        .select(
            "timestamp", "bed_id", "patient_name", 
            "heart_rate", "spo2", "temperature", "systolic_bp",
            "sepsis_score", "alert_status"
        )

    # Sink stream alert ke terminal console
    query = urgent_alerts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("[✓] Spark Structured Streaming aktif membaca stream dari Kafka!")
    print("[✓] Memantau tanda vital pasien ICU secara real-time...\n")

    query.awaitTermination()

if __name__ == "__main__":
    main()
