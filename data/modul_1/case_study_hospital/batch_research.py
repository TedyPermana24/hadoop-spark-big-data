"""
================================================================================
BATCH LAYER: Retrospective Clinical Research & AI Prep (Hadoop HDFS + Spark)
================================================================================
Tujuan:
1. Membaca data riwayat rekam medis (EHR) jangka panjang yang tersimpan di HDFS.
2. Analisis retrospektif mortalitas, demografi, dan tren kasus kritis (Sepsis & ICU).
3. Mengekstrak korelasi tanda vital untuk persiapan data training model AI klinis.
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, round as spark_round, desc

def main():
    print("=" * 75)
    print(" [BATCH LAYER] UNIVERSITY HOSPITAL - RETROSPECTIVE RESEARCH ENGINE")
    print("=" * 75)

    # 1. Inisialisasi Spark Session
    spark = SparkSession.builder \
        .appName("Hospital-Batch-Research") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Baca data EHR dari Hadoop HDFS
    hdfs_path = "hdfs://namenode:8020/hospital/ehr/ehr_patients.json"
    print(f"\n[+] Memuat data riwayat pasien dari HDFS: {hdfs_path}")
    
    df = spark.read.json(hdfs_path)
    total_records = df.count()
    print(f"[✓] Berhasil memuat {total_records} rekam medis pasien.\n")

    # 3. Analisis 1: Distribusi Departemen & Angka Keselamatan Pasien
    print("-" * 75)
    print(" ANALISIS 1: Distribusi Departemen & Angka Keselamatan (Mortality vs Recovered)")
    print("-" * 75)
    df.groupBy("department", "outcome") \
        .agg(
            count("*").alias("jumlah_pasien"),
            spark_round(avg("age"), 1).alias("rerata_usia"),
            spark_round(avg("heart_rate_avg"), 1).alias("rerata_detak_jantung"),
            spark_round(avg("spo2_avg"), 1).alias("rerata_SpO2")
        ) \
        .orderBy("department", "outcome") \
        .show()

    # 4. Analisis 2: Studi Retrospektif Pasien Sepsis (Penyebab Kematian vs Selamat)
    print("-" * 75)
    print(" ANALISIS 2: Studi Retrospektif Pasien SEPSIS di ICU")
    print("-" * 75)
    sepsis_df = df.filter(col("diagnosis") == "Sepsis")
    sepsis_df.groupBy("outcome") \
        .agg(
            count("*").alias("pasien_sepsis"),
            spark_round(avg("heart_rate_avg"), 1).alias("HR_avg"),
            spark_round(avg("spo2_avg"), 1).alias("SpO2_avg"),
            spark_round(avg("temperature_avg"), 1).alias("Temp_avg"),
            spark_round(avg("systolic_bp_avg"), 1).alias("BP_Sistolik_avg"),
            spark_round(avg("lab_lactate"), 2).alias("Lactate_Laktat_avg")
        ) \
        .show()

    # 5. Analisis 3: Ekstraksi Data Korelasi untuk AI Model Training (Sepsis Prediction)
    print("-" * 75)
    print(" ANALISIS 3: Ekstraksi Fitur Tanda Vital untuk Persiapan Training AI")
    print("-" * 75)
    ai_features_df = df.withColumn(
        "label_critical_risk",
        when(
            (col("heart_rate_avg") > 110) & 
            (col("spo2_avg") < 90) & 
            (col("lab_lactate") > 4.0), 
            1
        ).otherwise(0)
    ).select(
        "patient_id", "name", "diagnosis", 
        "heart_rate_avg", "spo2_avg", "temperature_avg", 
        "lab_lactate", "label_critical_risk", "outcome"
    )

    ai_features_df.orderBy(desc("label_critical_risk")).show()

    print("=" * 75)
    print(" [✓] BATCH RESEARCH SELESAI: Data siap digunakan oleh peneliti & tim AI")
    print("=" * 75)

    spark.stop()

if __name__ == "__main__":
    main()
