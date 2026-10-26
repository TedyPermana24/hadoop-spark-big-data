-- ============================================================================
-- CASE STUDY: CASSANDRA NOSQL SCHEMA & VARIED ICU TIME-SERIES TELEMETRY
-- ============================================================================

-- 1. Buat Keyspace untuk Telemetri Rumah Sakit
CREATE KEYSPACE IF NOT EXISTS hospital_telemetry 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE hospital_telemetry;

-- 2. Buat Tabel Time-Series Telemetri Pasien ICU
CREATE TABLE IF NOT EXISTS icu_patient_vitals (
    patient_id text,
    timestamp timestamp,
    bed_id text,
    patient_name text,
    heart_rate int,
    spo2 int,
    temperature double,
    respiratory_rate int,
    systolic_bp int,
    diastolic_bp int,
    condition text,
    PRIMARY KEY ((patient_id), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Bersihkan data lama untuk tampilan rapi
TRUNCATE icu_patient_vitals;

-- 3. Masukkan Data Telemetri Bervariasi Pasien ICU (Real-Time Time-Series Fluktuasi Alami)
-- Pasien 1: Ahmad Rizki (Kondisi Memburuk - Suspect Sepsis)
INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-101', '2026-10-26 17:00:00+0000', 'BED-01', 'Ahmad Rizki', 110, 91, 38.6, 22, 92, 60, 'Suspect Sepsis');

INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-101', '2026-10-26 17:05:00+0000', 'BED-01', 'Ahmad Rizki', 118, 88, 39.2, 26, 85, 55, 'Sepsis Severe Warning');

-- Pasien 2: Dewi Lestari (Kondisi Stabil - Pemulihan Pasca Operasi)
INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-102', '2026-10-26 17:00:00+0000', 'BED-02', 'Dewi Lestari', 74, 98, 36.6, 16, 118, 76, 'Post-Op Stable');

INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-102', '2026-10-26 17:05:00+0000', 'BED-02', 'Dewi Lestari', 78, 99, 36.8, 17, 115, 74, 'Post-Op Recovery');

-- Pasien 3: Budi Santoso (Kondisi Kritis - Syok Septik)
INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-103', '2026-10-26 17:00:00+0000', 'BED-03', 'Budi Santoso', 126, 84, 36.4, 28, 78, 48, 'Septic Shock Alert');

INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-103', '2026-10-26 17:05:00+0000', 'BED-03', 'Budi Santoso', 135, 80, 36.1, 32, 70, 42, 'Septic Shock CRITICAL');

-- Pasien 4: Siti Nurhaliza (Pneumonia Akut)
INSERT INTO icu_patient_vitals (patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('P-104', '2026-10-26 17:05:00+0000', 'BED-04', 'Siti Nurhaliza', 96, 93, 38.7, 24, 98, 64, 'Pneumonia Acute');

-- 4. Kueri Menampilkan Seluruh Pasien ICU dengan Data Telemetri Bervariasi
SELECT patient_id, bed_id, patient_name, heart_rate, spo2, temperature, systolic_bp, condition 
FROM icu_patient_vitals;
