# 🏥 Case Study: University Hospital Hybrid Big Data Architecture

## 1. Konsep Arsitektur (Lambda Architecture)

Studi kasus ini mengimplementasikan arsitektur hybrid Big Data untuk rumah sakit universitas:

```text
                           ┌─────────────────────────────────────────────────────────┐
                           │                      SUMBER DATA                        │
                           └────────────────────────────┬────────────────────────────┘
                                                        │
                        ┌───────────────────────────────┴───────────────────────────────┐
                        │                                                               │
                        ▼                                                               ▼
            [ BATCH LAYER (Hadoop) ]                                        [ SPEED LAYER (Spark Streaming) ]
        • Data Rekam Medis (EHR) & Lab Historis                         • Sensor Monitor ICU Pasien (Real-time)
        • Disimpan di HDFS: /hospital/ehr/                              • Stream tanda vital per beberapa detik
        • Spark Batch Processing                                        • Evaluasi Early Warning Score (MEWS) Sepsis
                        │                                                               │
                        ▼                                                               ▼
          Analisis Retrospektif Klinis &                                  Immediate Critical Alert untuk
         Dataset Siap Latih Model AI/ML                                   Dokter / Perawat Jaga di ICU
                        │                                                               │
                        └───────────────────────────────┬───────────────────────────────┘
                                                        │
                                                        ▼
                                            [ SERVING LAYER (Klinisi) ]
                                      Dashboard Klinisi & Sistem Notifikasi
```

---

## 2. Struktur File

- [`ehr_patients.json`](file:///d:/TEDY/hadoop-spark/data/case_study_hospital/ehr_patients.json): Data rekam medis pasien untuk disimpan di HDFS.
- [`batch_research.py`](file:///d:/TEDY/hadoop-spark/data/case_study_hospital/batch_research.py): Job Spark Batch untuk analisis retrospektif & persiapan AI model.
- [`icu_vitals_stream.py`](file:///d:/TEDY/hadoop-spark/data/case_study_hospital/icu_vitals_stream.py): Simulator sensor monitor ICU streaming.
- [`stream_alerts.py`](file:///d:/TEDY/hadoop-spark/data/case_study_hospital/stream_alerts.py): Engine Spark Streaming pendeteksi sepsis & kondisi kritis real-time.

---

## 3. Cara Menjalankan

Pastikan cluster Docker Hadoop-Spark kamu sedang menyala:
```powershell
docker compose up -d
```

### A. Menjalankan BATCH LAYER (Hadoop HDFS + Spark Batch)
1. **Upload dataset EHR ke HDFS:**
   ```powershell
   docker exec namenode hdfs dfs -mkdir -p /hospital/ehr
   docker exec namenode hdfs dfs -put /data/modul_1/case_study_hospital/ehr_patients.json /hospital/ehr/
   docker exec namenode hdfs dfs -ls /hospital/ehr
   ```

2. **Jalankan Batch Analytics Spark:**
   ```powershell
   docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /data/modul_1/case_study_hospital/batch_research.py
   ```
   *Hasil:* Menampilkan analisis demografi pasien, studi kasus Sepsis, dan ekstraksi data untuk training AI.

---

### B. Menjalankan SPEED LAYER (Spark + Apache Kafka)
Speed layer menggunakan Kafka sebagai message broker untuk tanda vital ICU secara real-time.

1. **Nyalakan Kafka & Zookeeper (Hanya untuk modul ini):**
   ```powershell
   docker compose -f docker-compose.kafka.yml up -d
   ```
   *(File ini modular dan terhubung ke network `hadoop-net` yang sama)*

2. **Terminal 1 — Jalankan Monitor ICU (Kirim Stream ke Kafka):**
   ```powershell
   docker exec -it spark-master python3 /data/modul_1/case_study_hospital/icu_vitals_stream.py
   ```
   *(Monitor akan otomatis mengirim data tanda vital ke Kafka topic `icu-vitals` setiap 3 detik)*

3. **Terminal 2 — Jalankan Spark Structured Streaming (Consumer Kafka & Alert Engine):**
   ```powershell
   docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /data/modul_1/case_study_hospital/stream_alerts.py
   ```
   *Hasil:* Spark Streaming langsung membaca stream dari Kafka, menghitung skor Sepsis (MEWS), dan mencetak alert darurat langsung di layar jika pasien mengalami kondisi kritis!

4. **Matikan Kafka jika sudah selesai praktikum (Hemat RAM):**
   ```powershell
   docker compose -f docker-compose.kafka.yml down
   ```

---

### C. Menjalankan SERVING LAYER (REST API & Dashboard Klinisi)
Serving Layer dapat dijalankan langsung di PowerShell/terminal Windows laptop kamu:

1. **Jalankan API & Dashboard Server:**
   ```powershell
   python d:\TEDY\hadoop-spark\data\modul_1\case_study_hospital\serving_dashboard.py
   ```
2. **Buka Browser:**
   👉 **`http://localhost:5000`**

3. **Fitur yang Tersedia:**
   - **Live ICU Monitor Cards:** Tanda vital pasien auto-refresh tiap 2 detik dengan indikator skor Sepsis (MEWS).
   - **Tabel Retrospective Research:** Menampilkan data rekam medis jangka panjang dari Batch Layer HDFS.
   - **REST API Endpoints:**
     - `http://localhost:5000/api/vitals` (Data stream monitor terbaru)
     - `http://localhost:5000/api/alerts` (Daftar pasien darurat/sepsis)
     - `http://localhost:5000/api/patients` (Data EHR historis)
