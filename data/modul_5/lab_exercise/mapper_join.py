#!/usr/bin/env python3
"""
mapper_join.py
Melakukan tagging sumber data untuk Reduce-Side Join:
- Tabel A (Demographics: ID, Name, Region) -> Tag: DEMO
- Tabel B (Diagnosis: ID, Condition, Severity) -> Tag: DIAG
Input format: TSV
Output: patient_id \t [TAG]|[DATA]
"""
import sys
import os

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            patient_id = parts[0]
            # Deteksi berdasarkan isi atribut
            # Jika kolom ke-2 adalah Nama pasien -> Tabel Demographics
            # Jika kolom ke-2 adalah Diagnosis klinis -> Tabel Diagnosis
            is_diagnosis = any(cond in parts[1] for cond in [
                "Disease", "Diabetes", "Hypertension", "Cancer", "Asthma", "GERD", "Back Pain", "Depression"
            ])
            if is_diagnosis:
                # Tabel B: DIAG
                condition = parts[1]
                severity = parts[2]
                print(f"{patient_id}\tDIAG|{condition}|{severity}")
            else:
                # Tabel A: DEMO
                name = parts[1]
                region = parts[2]
                print(f"{patient_id}\tDEMO|{name}|{region}")

if __name__ == "__main__":
    main()
