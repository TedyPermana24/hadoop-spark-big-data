#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 2
================================================================================
Mapper: Analisis Mortalitas per Kondisi Penyakit (Condition Mortality Analysis)
================================================================================
KONTEKS:
  Mengidentifikasi kondisi penyakit mana yang memiliki tingkat mortalitas
  (adverse_outcome) tertinggi untuk membantu prioritas alokasi sumber daya medis.

ALUR MAPREDUCE (Job 2 - Condition Mortality):
  Map    : Baca baris CSV → emit (condition, adverse_outcome)
  Shuffle: Kelompokkan semua nilai per condition
  Reduce : Hitung total kasus dan total adverse per condition
           → emit (condition, total_cases|adverse_count|mortality_rate%)
  Output : /hospital/mapreduce/job2_mortality_by_condition/

Contoh output:
  Coronary Artery Disease   150|45|30.00%
  Depression                200|38|19.00%
  Type 2 Diabetes           300|72|24.00%
  Back Pain                 180|12|6.67%
================================================================================
"""

import sys

IDX_CONDITION = 7
IDX_ADVERSE   = 19

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
        if not condition:
            condition = "Unknown"

        try:
            adverse = int(fields[IDX_ADVERSE]) if fields[IDX_ADVERSE] else 0
        except ValueError:
            adverse = 0

        # Emit: (condition, "total_count|adverse_count")
        # Format: 1 kasus, adverse bisa 0 atau 1
        print(f"{condition}\t1|{adverse}")

if __name__ == "__main__":
    main()
