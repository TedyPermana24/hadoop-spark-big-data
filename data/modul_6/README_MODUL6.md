================================================================================
MODUL 6 - README: CARA MENJALANKAN APACHE SPARK & ANALITIK KANKER
================================================================================

STRUKTUR FILE:
  data/modul_6/
  ├── lab_exercise/
  │   ├── data.csv                 # Dataset sample individu (name, age)
  │   ├── sample.txt               # Dataset teks Word Count RDD
  │   └── spark_basics.py          # Lab: RDD Basics, DataFrame CSV, Spark SQL & WordCount
  ├── case_study/
  │   ├── pathology_notes.txt      # Teks tidak terstruktur catatan patologi biopsi
  │   ├── cancer_analytics_spark.py # Analitik RDD, DataFrame SQL, & Benchmark Latensi
  │   └── output/                  # Hasil keluaran TXT dan JSON
  │       ├── pathology_patterns_rdd.txt
  │       ├── cancer_cohort_sql.txt
  │       ├── spark_vs_mapreduce_benchmark.txt
  │       └── cancer_benchmark_results.json
  ├── Laporan_Modul6_BigData.txt   # Laporan resmi (Format standar Modul 3 & 5)
  └── README_MODUL6.md             # Panduan eksekusi ini

================================================================================
A. MENJALANKAN SECARA LOKAL / STANDALONE (Windows PowerShell)
================================================================================

1. Jalankan Studi Kasus Analitik Kanker & Benchmark:
   cd d:\Src\BigData\hadoop-spark-big-data
   python data\modul_6\case_study\cancer_analytics_spark.py

2. Jalankan Serving Layer Dashboard:
   python data/modul_1/case_study_hospital/serving_dashboard.py
   Buka di browser:
   👉 Spark Cancer Analytics Dashboard : http://localhost:5000/spark (atau /modul6)
   👉 REST API Spark Analytics        : http://localhost:5000/api/spark_cancer_analytics

================================================================================
B. MENJALANKAN DI APACHE SPARK & HADOOP DOCKER CLUSTER
================================================================================

STEP 1: Pastikan seluruh kontainer kluster aktif
   docker compose up -d
   docker ps

STEP 2: Unggah dataset ke HDFS
   docker exec namenode hdfs dfs -mkdir -p /user/student /hospital/spark
   docker exec namenode hdfs dfs -put /data/modul_6/lab_exercise/data.csv /user/student/
   docker exec namenode hdfs dfs -put /data/modul_6/lab_exercise/sample.txt /user/student/
   docker exec namenode hdfs dfs -put /data/modul_6/case_study/pathology_notes.txt /hospital/spark/
   docker exec namenode hdfs dfs -put /data/full_dataset_ehr.csv /hospital/spark/

STEP 3: Jalankan Lab Exercise (spark_basics.py) di Spark Master
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /data/modul_6/lab_exercise/spark_basics.py

STEP 4: Jalankan Studi Kasus Analitik Kanker di Spark Master
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /data/modul_6/case_study/cancer_analytics_spark.py

STEP 5: Akses Web UI Kluster di Browser
   👉 Apache Spark Master Web UI : http://localhost:8080/
   👉 Apache Spark Worker Web UI : http://localhost:8081/
   👉 Hadoop NameNode HDFS Web UI: http://localhost:9870/
   👉 Serving Dashboard Platform : http://localhost:5000/spark

================================================================================
C. PANDUAN TANGKAPAN LAYAR (12 GAMBAR LAPORAN MODUL 6)
================================================================================

1. [Gambar 1. memperlihatkan docker container]
   -> Screenshot terminal: docker ps (terlihat spark-master, spark-worker, namenode).
2. [Gambar 2. memperlihatkan eksekusi perintah hdfs dfs -mkdir, -put, dan -ls pada terminal]
   -> Screenshot terminal upload data ke HDFS.
3. [Gambar 3. memperlihatkan browse directory HDFS /user/student pada Web UI]
   -> Browser: http://localhost:9870/explorer.html#/user/student
4. [Gambar 4. memperlihatkan diagram alur arsitektur Apache Spark]
   -> Crop diagram arsitektur dari modul halaman 42 (Driver -> Cluster Manager -> Executors, DAG).
5. [Gambar 5. kode PySpark spark_basics.py]
   -> Buka data/modul_6/lab_exercise/spark_basics.py di VS Code (satu-satunya screenshot kode).
6. [Gambar 6. output hasil eksekusi RDD dan DataFrame pada terminal]
   -> Screenshot terminal eksekusi spark_basics.py (Sum: 15, DataFrame show, SQL show, WordCount take 10).
7. [Gambar 7. antarmuka web Apache Spark Master Web UI pada port 8080]
   -> Browser: http://localhost:8080/ (Spark Master Web UI).
8. [Gambar 8. browse directory HDFS /hospital/spark pada Web UI]
   -> Browser: http://localhost:9870/explorer.html#/hospital/spark
9. [Gambar 9. output hasil eksekusi ekstraksi pola patologi RDD pada terminal]
   -> Screenshot terminal: cat data/modul_6/case_study/output/pathology_patterns_rdd.txt
10. [Gambar 10. output hasil kueri analitik kohort kanker pada terminal]
   -> Screenshot terminal: cat data/modul_6/case_study/output/cancer_cohort_sql.txt
11. [Gambar 11. output perbandingan performa latensi eksekusi Spark vs MapReduce pada terminal]
   -> Screenshot terminal: cat data/modul_6/case_study/output/spark_vs_mapreduce_benchmark.txt
12. [Gambar 12. antarmuka web Spark Cancer Analytics pada Serving Dashboard]
   -> Browser: http://localhost:5000/spark
================================================================================
