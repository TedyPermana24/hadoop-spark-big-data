================================================================================
MODUL 5 - README: CARA MENJALANKAN MAPREDUCE
================================================================================

STRUKTUR FILE:
  data/modul_5/
  ├── lab_exercise/
  │   ├── mapper_wordcount.py        # Lab: Mapper Word Count (Python)
  │   ├── reducer_wordcount.py       # Lab: Reducer Word Count (Python)
  │   ├── combiner_wordcount.py      # Lab: Combiner (Local Reducer)
  │   ├── WordCount.java             # Lab: Native Java MapReduce API
  │   ├── server_logs.txt            # Lab: Dataset Log Server
  │   ├── mapper_log_analysis.py     # Lab: Mapper Log Analysis
  │   ├── reducer_log_analysis.py    # Lab: Reducer Log Analysis
  │   ├── patients_info.txt          # Lab: Dataset Demografi Relasi A
  │   ├── patients_diagnosis.txt     # Lab: Dataset Diagnosa Relasi B
  │   ├── mapper_join.py             # Lab: Mapper Reduce-Side Join
  │   └── reducer_join.py            # Lab: Reducer Reduce-Side Join
  └── case_study/
      ├── mapper_highrisk_region.py  # Job 1: High-Risk by Region
      ├── reducer_highrisk_region.py
      ├── mapper_mortality_condition.py # Job 2: Mortality by Condition
      ├── reducer_mortality_condition.py
      ├── mapper_bp_profile.py       # Job 3: BP Profile
      ├── reducer_bp_profile.py
      ├── run_mapreduce_local.py     # Runner lokal (tanpa Hadoop cluster)
      └── output/                    # Output TXT ketiga job

================================================================================
A. MENJALANKAN SECARA LOKAL (Windows PowerShell)
================================================================================

1. WORD COUNT (Lab Exercise Python):
   cat data\modul_1\lab_exercise\sample.txt | python data\modul_5\lab_exercise\mapper_wordcount.py | sort | python data\modul_5\lab_exercise\reducer_wordcount.py

2. LOG ANALYSIS (Lab Exercise):
   cat data\modul_5\lab_exercise\server_logs.txt | python data\modul_5\lab_exercise\mapper_log_analysis.py | sort | python data\modul_5\lab_exercise\reducer_log_analysis.py

3. REDUCE-SIDE JOIN (Lab Exercise):
   cat data\modul_5\lab_exercise\patients_info.txt, data\modul_5\lab_exercise\patients_diagnosis.txt | python data\modul_5\lab_exercise\mapper_join.py | sort | python data\modul_5\lab_exercise\reducer_join.py

4. CASE STUDY (Semua 3 Job sekaligus):
   cd d:\Src\BigData\hadoop-spark-big-data
   python data\modul_5\case_study\run_mapreduce_local.py

================================================================================
B. MENJALANKAN DI HADOOP CLUSTER (via Hadoop Streaming)
================================================================================

STEP 1: Pastikan cluster Hadoop berjalan
   docker compose up -d

STEP 2: Upload dataset ke HDFS
   docker exec namenode hdfs dfs -mkdir -p /hospital/mapreduce
   docker exec namenode hdfs dfs -put /data/full_dataset_ehr.csv /hospital/mapreduce/
   docker exec namenode hdfs dfs -put /data/modul_1/lab_exercise/sample.txt /user/student/

STEP 3a: Lab Exercise - Word Count (Python Hadoop Streaming)
   docker exec namenode hadoop jar \
     /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
     -input /user/student/sample.txt \
     -output /user/student/wordcount_output \
     -mapper "python3 /data/modul_5/lab_exercise/mapper_wordcount.py" \
     -reducer "python3 /data/modul_5/lab_exercise/reducer_wordcount.py" \
     -file /data/modul_5/lab_exercise/mapper_wordcount.py \
     -file /data/modul_5/lab_exercise/reducer_wordcount.py

STEP 3b: Lab Exercise - Word Count (Native Java MapReduce)
   docker exec -it namenode bash -c "
     cd /data/modul_5/lab_exercise
     javac -classpath \$(hadoop classpath) -d . WordCount.java
     jar -cvf wordcount.jar WordCount*.class
     hadoop jar wordcount.jar WordCount /user/student/sample.txt /user/student/java_wordcount_output
   "

STEP 4a: Case Study Job 1 - High-Risk by Region
   docker exec namenode hadoop jar \
     /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
     -input /hospital/mapreduce/full_dataset_ehr.csv \
     -output /hospital/mapreduce/job1_highrisk_by_region \
     -mapper "python3 /data/modul_5/case_study/mapper_highrisk_region.py" \
     -combiner "python3 /data/modul_5/case_study/reducer_highrisk_region.py" \
     -reducer "python3 /data/modul_5/case_study/reducer_highrisk_region.py" \
     -file /data/modul_5/case_study/mapper_highrisk_region.py \
     -file /data/modul_5/case_study/reducer_highrisk_region.py

STEP 4b: Case Study Job 2 - Mortality by Condition
   docker exec namenode hadoop jar \
     /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
     -input /hospital/mapreduce/full_dataset_ehr.csv \
     -output /hospital/mapreduce/job2_mortality_by_condition \
     -mapper "python3 /data/modul_5/case_study/mapper_mortality_condition.py" \
     -reducer "python3 /data/modul_5/case_study/reducer_mortality_condition.py" \
     -file /data/modul_5/case_study/mapper_mortality_condition.py \
     -file /data/modul_5/case_study/reducer_mortality_condition.py

STEP 4c: Case Study Job 3 - BP Profile
   docker exec namenode hadoop jar \
     /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
     -input /hospital/mapreduce/full_dataset_ehr.csv \
     -output /hospital/mapreduce/job3_bp_profile \
     -mapper "python3 /data/modul_5/case_study/mapper_bp_profile.py" \
     -reducer "python3 /data/modul_5/case_study/reducer_bp_profile.py" \
     -file /data/modul_5/case_study/mapper_bp_profile.py \
     -file /data/modul_5/case_study/reducer_bp_profile.py

STEP 5: Lihat hasil output
   docker exec namenode hdfs dfs -ls /hospital/mapreduce/
   docker exec namenode hdfs dfs -cat /hospital/mapreduce/job1_highrisk_by_region/part-00000
   docker exec namenode hdfs dfs -cat /hospital/mapreduce/job2_mortality_by_condition/part-00000
   docker exec namenode hdfs dfs -cat /hospital/mapreduce/job3_bp_profile/part-00000

================================================================================
C. MELIHAT HASIL DI DASHBOARD WEB (Serving Layer)
================================================================================

Setelah menjalankan runner lokal atau MapReduce di Hadoop, hasil studi kasus
dapat dilihat secara visual melalui web dashboard:

1. Jalankan aplikasi Serving Layer:
   python data/modul_1/case_study_hospital/serving_dashboard.py

2. Buka di browser:
   👉 MapReduce Analytics Dashboard : http://localhost:5000/mapreduce (atau /modul5)
   👉 REST API MapReduce Results    : http://localhost:5000/api/mapreduce_results

Fitur Dashboard MapReduce:
- Visualisasi ringkasan KPI (Total Pasien Risiko Tinggi, Mortalitas Tertinggi, Puncak Sistolik)
- Diagram alur 4 tahap MapReduce (Map, Combiner, Shuffle & Sort, Reduce)
- Job 1: Kartu dan grafik batang distribusi risiko per region
- Job 2: Tabel tingkat mortalitas & komplikasi per kondisi penyakit
- Job 3: Tabel profil tekanan darah sistolik per kondisi dan tingkat keparahan

================================================================================
