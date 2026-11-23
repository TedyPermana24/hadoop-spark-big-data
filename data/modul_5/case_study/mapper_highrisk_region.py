#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 1
================================================================================
Mapper: Hitungan Pasien Berisiko Tinggi per Wilayah (Region)
================================================================================
KONTEKS STUDI KASUS:
  Rumah Sakit Universitas perlu menganalisis distribusi pasien berisiko tinggi
  dari dataset 10.000+ rekam medis (EHR) untuk perencanaan sumber daya klinis
  dan kebijakan kesehatan regional.

  Kriteria "Pasien Berisiko Tinggi" (High-Risk Patient):
    - Tekanan darah sistolik >= 140 mmHg (Hipertensi Stage 2)
    - ATAU kolesterol >= 240 mg/dL (Hiperkolesterolemia Berat)
    - ATAU outcome = "Deteriorated" (Kondisi Memburuk)
    - ATAU adverse_outcome = 1 (Luaran Buruk)

ALUR MAPREDUCE (Job 1 - Region Analysis):
  Input  : full_dataset_ehr.csv (HDFS)
  Map    : Baca setiap baris CSV → emit (region, 1) jika pasien berisiko tinggi
  Shuffle: Hadoop mengelompokkan semua (region, [1,1,1,...]) per region
  Reduce : Jumlahkan semua 1 per region → (region, total_high_risk)
  Output : /hospital/mapreduce/job1_highrisk_by_region/

Kolom CSV:
  patient_id,age_range,age_midpoint,gender,ethnicity,region,icd10_code,
  condition,severity,onset_year,medication,num_visits,adherence_pct,
  systolic_bp,diastolic_bp,heart_rate,cholesterol_mg_dl,bmi,outcome,adverse_outcome
  [0]       [1]        [2]          [3]    [4]        [5]  [6]       [7]       [8]
  [9]        [10]       [11]         [12]   [13]       [14] [15]      [16]      [17] [18] [19]
================================================================================
"""

import sys

# Indeks kolom (sesuai header CSV)
IDX_REGION       = 5
IDX_SYSTOLIC_BP  = 13
IDX_CHOLESTEROL  = 16
IDX_OUTCOME      = 18
IDX_ADVERSE      = 19

def is_high_risk(fields):
    """Evaluasi apakah pasien termasuk kategori berisiko tinggi."""
    try:
        systolic = float(fields[IDX_SYSTOLIC_BP]) if fields[IDX_SYSTOLIC_BP] else 0
        cholesterol = float(fields[IDX_CHOLESTEROL]) if fields[IDX_CHOLESTEROL] else 0
        outcome = fields[IDX_OUTCOME].strip().lower() if IDX_OUTCOME < len(fields) else ""
        adverse = int(fields[IDX_ADVERSE]) if IDX_ADVERSE < len(fields) and fields[IDX_ADVERSE] else 0

        return (
            systolic >= 140 or
            cholesterol >= 240 or
            outcome == "deteriorated" or
            adverse == 1
        )
    except (ValueError, IndexError):
        return False

def main():
    header_skipped = False

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Skip header baris pertama
        if not header_skipped:
            if line.startswith("patient_id"):
                header_skipped = True
                continue
            header_skipped = True  # Jika header tidak ada, tetap lanjut

        fields = line.split(',')
        if len(fields) < 20:
            continue

        region = fields[IDX_REGION].strip()
        if not region:
            region = "Unknown"

        # Emit (region, 1) HANYA jika pasien berisiko tinggi
        if is_high_risk(fields):
            print(f"{region}\t1")

if __name__ == "__main__":
    main()
