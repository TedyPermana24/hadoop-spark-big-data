#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 3
================================================================================
Mapper: Analisis Profil Usia & Tekanan Darah per Kondisi
        (Age-Severity Profile Analysis)
================================================================================
KONTEKS:
  Menganalisis rata-rata tekanan darah sistolik per kelompok usia dan kondisi
  untuk mendeteksi pola klinis faktor risiko penyakit kronis.

ALUR MAPREDUCE (Job 3):
  Map    : Baca CSV → emit (condition_severity, systolic_bp|1)
  Shuffle: Kelompokkan per condition+severity
  Reduce : Hitung rata-rata systolic_bp per condition+severity
  Output : /hospital/mapreduce/job3_bp_profile/

Contoh output:
  Type 2 Diabetes_Mild     130.5|850
  Type 2 Diabetes_Moderate 148.2|1200
  Type 2 Diabetes_Severe   162.1|400
================================================================================
"""

import sys

IDX_CONDITION   = 7
IDX_SEVERITY    = 8
IDX_SYSTOLIC_BP = 13

def main():
    header_skipped = False

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if not header_skipped:
            if line.startswith("patient_id"):
                header_skipped = True
                continue
            header_skipped = True

        fields = line.split(',')
        if len(fields) < 20:
            continue

        condition = fields[IDX_CONDITION].strip()
        severity  = fields[IDX_SEVERITY].strip()

        if not condition or not severity:
            continue

        try:
            systolic = float(fields[IDX_SYSTOLIC_BP])
        except (ValueError, IndexError):
            continue

        # Composite key: condition_severity
        key = f"{condition}_{severity}"
        # Value: systolic_bp|count
        print(f"{key}\t{systolic}|1")

if __name__ == "__main__":
    main()
