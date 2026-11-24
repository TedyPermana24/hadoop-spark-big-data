================================================================================
MODUL 7 - README: PANDUAN EKSEKUSI SPARK SQL, DATAFRAMES & FHIR INTEGRATION
================================================================================

STRUKTUR FILE:
  data/modul_7/
  ├── lab_exercise/
  │   ├── employees.csv              # Dataset CSV karyawan (id, name, department, salary)
  │   ├── people.json                # Dataset JSON semi-terstruktur (name, age)
  │   ├── spark_sql_basics.py        # Lab: SparkSession, DataFrame API, SQL view, Parquet
  │   └── output/
  │       └── avg_salary_by_dept/    # Output format Apache Parquet (_SUCCESS & part file)
  ├── case_study/
  │   ├── generate_fhir_data.py      # Generator 801 data admisi berstandar FHIR JSON
  │   ├── fhir_admissions.json       # Dataset semi-terstruktur admisi rumah sakit
  │   ├── hospital_sql_analytics.py  # Spark SQL kueri multi-format (EHR CSV + FHIR JSON)
  │   └── output/                    # Hasil keluaran analitik
  │       ├── admissions_trend_by_day.txt
  │       ├── readmission_rates_dept.txt
  │       ├── cross_format_unified_join.txt
  │       ├── schema_evolution_analysis.txt
  │       └── spark_sql_benchmark_results.json
  ├── Laporan_Modul7_BigData.txt     # Naskah laporan resmi (Format standar Modul 3 & 5)
  └── README_MODUL7.md               # Panduan eksekusi ini

================================================================================
A. MENJALANKAN SECARA LOKAL / STANDALONE (Windows PowerShell)
================================================================================

1. Jalankan Lab Exercise Spark SQL (Pemuatan CSV, Filter, GroupBy, SQL & Parquet):
   cd d:\Src\BigData\hadoop-spark-big-data
   python data\modul_7\lab_exercise\spark_sql_basics.py

2. Jalankan Studi Kasus Analitik Integrasi Multi-Format Hospital EHR + FHIR JSON:
   python data\modul_7\case_study\hospital_sql_analytics.py

3. Jalankan Serving Layer Web Dashboard:
   python data\modul_1\case_study_hospital\serving_dashboard.py
   Buka di web browser:
   👉 Spark SQL & FHIR Dashboard : http://localhost:5000/spark_sql (atau /modul7)
   👉 REST API Spark SQL         : http://localhost:5000/api/spark_sql_analytics

================================================================================
B. MENJALANKAN DI APACHE SPARK & HADOOP DOCKER CLUSTER
================================================================================

STEP 1: Pastikan seluruh kontainer kluster aktif
   docker compose up -d
   docker ps

STEP 2: Unggah dataset Lab & Studi Kasus ke HDFS
   docker exec namenode hdfs dfs -mkdir -p /user/student /hospital/spark_sql
   docker exec namenode hdfs dfs -put /data/modul_7/lab_exercise/employees.csv /user/student/
   docker exec namenode hdfs dfs -put /data/modul_7/lab_exercise/people.json /user/student/
   docker exec namenode hdfs dfs -put /data/modul_7/case_study/fhir_admissions.json /hospital/spark_sql/
   docker exec namenode hdfs dfs -put /data/full_dataset_ehr.csv /hospital/spark_sql/
   docker exec namenode hdfs dfs -ls /user/student
   docker exec namenode hdfs dfs -ls /hospital/spark_sql

STEP 3: Eksekusi PySpark Script pada Kluster
   # Lab Exercise:
   docker exec spark-master /spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /data/modul_7/lab_exercise/spark_sql_basics.py

   # Case Study:
   docker exec spark-master /spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /data/modul_7/case_study/hospital_sql_analytics.py

================================================================================
C. PANDUAN PENGAMBILAN 12 GAMBAR SCREENSHOT UNTUK LAPORAN
================================================================================

Bagian 2 - Latihan Laboratorium (Lab Exercise):
- Gambar 1: [Gambar memperlihatkan docker container]
  -> Screenshot terminal eksekusi: `docker ps` menampilkan container aktif (namenode, datanode, spark-master, spark-worker, dll).
- Gambar 2: [Gambar memperlihatkan eksekusi perintah hdfs dfs -mkdir, -put, dan -ls pada terminal]
  -> Screenshot terminal perintah `hdfs dfs -mkdir -p /user/student`, `hdfs dfs -put employees.csv people.json /user/student/`, dan `hdfs dfs -ls /user/student`.
- Gambar 3: [Gambar memperlihatkan browse directory HDFS /user/student pada Web UI]
  -> Screenshot browser Hadoop NameNode Web UI (http://localhost:9870) menu Utilities -> Browse the file system pada path `/user/student`.
- Gambar 4: [Gambar memperlihatkan diagram alur kerja Spark SQL dan Catalyst Optimizer]
  -> Diagram alur resmi Spark SQL dari modul (Input Data -> DataFrame -> Catalyst Optimizer [Logical Plan -> Optimized Logical Plan -> Physical Plan] -> Execution).
- Gambar 5: [Gambar kode PySpark spark_sql_basics.py]
  -> Screenshot editor teks/IDE menampilkan kode `spark_sql_basics.py` (SATU-SATUNYA screenshot kode pada seluruh naskah laporan).
- Gambar 6: [Gambar output hasil eksekusi DataFrame dan SQL pada terminal]
  -> Screenshot terminal hasil eksekusi `python spark_sql_basics.py` menampilkan skema DataFrame, tabel karyawan gaji >50.000, tabel rata-rata gaji per departemen, dan kueri SQL people.json.
- Gambar 7: [Gambar browse directory HDFS /user/student/output/avg_salary_by_dept pada Web UI]
  -> Screenshot browser Hadoop Web UI (http://localhost:9870) menampilkan berkas `_SUCCESS` dan `part-00000.snappy.parquet` pada direktori output.

Bagian 3 - Studi Kasus Kesehatan (Health Informatics Case Study):
- Gambar 8: [Gambar browse directory HDFS /hospital/spark_sql pada Web UI]
  -> Screenshot browser Hadoop Web UI (http://localhost:9870) browse directory pada path `/hospital/spark_sql` menampilkan `fhir_admissions.json` dan `full_dataset_ehr.csv`.
- Gambar 9: [Gambar output hasil kueri tren admisi IGD per hari pada terminal]
  -> Screenshot terminal kueri Spark SQL `admissions_trend_by_day` (tabel Day_of_Week, Total_Admissions, ER_Admissions, ER_Percentage).
- Gambar 10: [Gambar output hasil kueri tingkat readmisi per departemen pada terminal]
  -> Screenshot terminal kueri Spark SQL `readmission_rates_dept` (tabel Department, Total_Encounters, Readmission_Count, Readmission_Rate, Avg_Stay_Days).
- Gambar 11: [Gambar output kueri cross-format join EHR dan FHIR JSON pada terminal]
  -> Screenshot terminal kueri Spark SQL `cross_format_unified_join` (tabel Condition, Severity, Cohort_Size, Mean_Systolic_BP, Readmission_Rate, Mean_LOS_Days).
- Gambar 12: [Gambar antarmuka web Spark SQL & FHIR Analytics pada Serving Dashboard]
  -> Screenshot browser antarmuka web Serving Dashboard pada URL `http://localhost:5000/spark_sql` menampilkan KPI cards, alur Catalyst, tabel tren admisi, tabel readmisi departemen, dan tabel korelasi unified join.
