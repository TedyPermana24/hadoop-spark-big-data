"""
================================================================================
CASE STUDY: STORAGE & ARCHITECTURE BENCHMARK (Row vs Columnar Storage)
================================================================================
Mengevaluasi efisiensi penyimpanan dan kecepatan kueri analitik klinis:
1. Membaca 10.000+ data rekam medis pasien (full_dataset_ehr.csv) dari HDFS.
2. Mengonversi ke Apache Parquet (Columnar Storage) terkompresi Snappy.
3. Membandingkan footprint penyimpanan HDFS (CSV vs Parquet) dan % kompresi.
4. Melakukan benchmark kecepatan kueri analitik (Query Latency) pada CSV vs Parquet.
5. Menyimpan hasil metrik untuk disajikan pada Web Dashboard (/storage).
================================================================================
"""

import time
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round, desc

def main():
    print("=" * 80)
    print(" [STORAGE BENCHMARK] HOSPITAL BIG DATA - ROW (CSV) VS COLUMNAR (PARQUET)")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("Hospital-Storage-Benchmark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Path data di HDFS
    csv_path = "hdfs://namenode:8020/hospital/storage/full_dataset_ehr.csv"
    parquet_path = "hdfs://namenode:8020/hospital/storage/ehr_parquet"

    print(f"\n[+] Memuat data CSV dari HDFS: {csv_path}")
    t0_load = time.time()
    df_csv = spark.read.csv(csv_path, header=True, inferSchema=True)
    total_records = df_csv.count()
    t_load_csv = round(time.time() - t0_load, 3)
    print(f"[✓] Berhasil memuat {total_records:,} rekam medis pasien dalam {t_load_csv} detik.")

    # 1. Konversi ke Parquet (Columnar Storage)
    print(f"\n[+] Mengonversi dan menyimpan ke Parquet: {parquet_path}")
    t0_write = time.time()
    df_csv.write.mode("overwrite").parquet(parquet_path)
    t_write_parquet = round(time.time() - t0_write, 3)
    print(f"[✓] Konversi Parquet selesai dalam {t_write_parquet} detik.")

    # 2. Ukur Ukuran Penyimpanan di HDFS menggunakan Hadoop FileSystem API
    conf = sc._jsc.hadoopConfiguration()
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem

    fs = FileSystem.get(conf)
    csv_bytes = fs.getContentSummary(Path(csv_path)).getLength()
    parquet_bytes = fs.getContentSummary(Path(parquet_path)).getLength()

    csv_mb = round(csv_bytes / (1024 * 1024), 2)
    parquet_mb = round(parquet_bytes / (1024 * 1024), 2)
    saving_pct = round(((csv_bytes - parquet_bytes) / csv_bytes) * 100, 1)

    print("\n" + "-" * 80)
    print(" 1. HASIL ANALISIS UKURAN PENYIMPANAN (STORAGE FOOTPRINT)")
    print("-" * 80)
    print(f" • Format Row-Based (CSV)      : {csv_bytes:,} bytes (~{csv_mb} MB)")
    print(f" • Format Columnar (Parquet)   : {parquet_bytes:,} bytes (~{parquet_mb} MB)")
    print(f" • Efisiensi Penghematan Disk  : {saving_pct}% lebih hemat ruang penyimpanan!")
    print("-" * 80)

    # 3. Benchmark Kueri Analitik 1: Agregasi Komorbiditas Klinis Berat
    # Menghitung rata-rata tekanan darah, kolesterol, dan mortalitas per kondisi medis
    print("\n" + "-" * 80)
    print(" 2. BENCHMARK KUERI ANALITIK 1: Agregasi Komorbiditas & Risiko Klinis")
    print("-" * 80)

    # Kueri pada CSV
    t0_q1_csv = time.time()
    res_csv = df_csv.groupBy("condition", "severity") \
        .agg(
            count("*").alias("total_pasien"),
            spark_round(avg("systolic_bp"), 1).alias("avg_systolic"),
            spark_round(avg("cholesterol_mg_dl"), 1).alias("avg_cholesterol"),
            spark_round(avg("adverse_outcome"), 3).alias("mortality_rate")
        ) \
        .filter(col("total_pasien") > 50) \
        .orderBy(desc("mortality_rate"))
    res_csv.collect()
    latency_q1_csv = round((time.time() - t0_q1_csv) * 1000, 2)

    # Kueri pada Parquet
    df_parquet = spark.read.parquet(parquet_path)
    t0_q1_parquet = time.time()
    res_parquet = df_parquet.groupBy("condition", "severity") \
        .agg(
            count("*").alias("total_pasien"),
            spark_round(avg("systolic_bp"), 1).alias("avg_systolic"),
            spark_round(avg("cholesterol_mg_dl"), 1).alias("avg_cholesterol"),
            spark_round(avg("adverse_outcome"), 3).alias("mortality_rate")
        ) \
        .filter(col("total_pasien") > 50) \
        .orderBy(desc("mortality_rate"))
    res_parquet.collect()
    latency_q1_parquet = round((time.time() - t0_q1_parquet) * 1000, 2)

    speedup_q1 = round(latency_q1_csv / max(1, latency_q1_parquet), 2)
    print(f" • Waktu Eksekusi Kueri pada CSV     : {latency_q1_csv} ms")
    print(f" • Waktu Eksekusi Kueri pada Parquet : {latency_q1_parquet} ms")
    print(f" • Peningkatan Kecepatan (Speedup)   : {speedup_q1}x lebih cepat")
    print("\n Cuplikan Hasil Analitik Klinis:")
    res_parquet.show(5)

    # 4. Benchmark Kueri Analitik 2: Column Pruning & Filter Predikat
    print("-" * 80)
    print(" 3. BENCHMARK KUERI ANALITIK 2: Filter Predikat (Type 2 Diabetes) & Kolom Tertentu")
    print("-" * 80)

    # Kueri pada CSV
    t0_q2_csv = time.time()
    df_csv.filter(col("condition") == "Type 2 Diabetes") \
        .select("patient_id", "systolic_bp", "cholesterol_mg_dl") \
        .collect()
    latency_q2_csv = round((time.time() - t0_q2_csv) * 1000, 2)

    # Kueri pada Parquet
    t0_q2_parquet = time.time()
    df_parquet.filter(col("condition") == "Type 2 Diabetes") \
        .select("patient_id", "systolic_bp", "cholesterol_mg_dl") \
        .collect()
    latency_q2_parquet = round((time.time() - t0_q2_parquet) * 1000, 2)

    speedup_q2 = round(latency_q2_csv / max(1, latency_q2_parquet), 2)
    print(f" • Waktu Filter pada CSV             : {latency_q2_csv} ms")
    print(f" • Waktu Filter pada Parquet         : {latency_q2_parquet} ms")
    print(f" • Peningkatan Kecepatan (Speedup)   : {speedup_q2}x lebih cepat berkat Column Pruning!")

    # 5. Simpan Hasil Benchmark ke JSON untuk Web Dashboard
    metrics = {
        "total_records": total_records,
        "csv_bytes": csv_bytes,
        "csv_mb": csv_mb,
        "parquet_bytes": parquet_bytes,
        "parquet_mb": parquet_mb,
        "saving_pct": saving_pct,
        "latency_q1_csv_ms": latency_q1_csv,
        "latency_q1_parquet_ms": latency_q1_parquet,
        "speedup_q1": speedup_q1,
        "latency_q2_csv_ms": latency_q2_csv,
        "latency_q2_parquet_ms": latency_q2_parquet,
        "speedup_q2": speedup_q2,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    out_file = "/data/modul_3/case_study/benchmark_results.json"
    try:
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"\n[✓] Metrik benchmark berhasil disimpan ke: {out_file}")
    except Exception as e:
        print(f"[!] Gagal menulis JSON metrik: {e}")

    print("=" * 80)
    print(" [✓] STORAGE BENCHMARK COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()
