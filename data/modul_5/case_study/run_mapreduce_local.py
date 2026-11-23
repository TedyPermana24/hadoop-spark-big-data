#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: RUNNER SCRIPT (Simulasi MapReduce Lokal)
================================================================================
Script ini menjalankan seluruh pipeline MapReduce Modul 5 secara LOKAL
(tanpa cluster Hadoop) untuk tujuan demonstrasi dan verifikasi hasil.

UNTUK MENJALANKAN DI HADOOP CLUSTER (via Hadoop Streaming):
  Lihat README_MODUL5.md atau perintah di bawah ini.

UNTUK MENJALANKAN LOKAL (Python only):
  python3 run_mapreduce_local.py

Output akan disimpan di folder: ./output/
================================================================================
"""

import os
import sys
import subprocess
import csv
import io
from collections import defaultdict

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
EHR_FILE  = os.path.join(DATA_DIR, "full_dataset_ehr.csv")
OUT_DIR   = os.path.join(BASE_DIR, "output")

os.makedirs(OUT_DIR, exist_ok=True)

SEPARATOR = "=" * 75

def print_header(title):
    print(f"\n{SEPARATOR}")
    print(f" {title}")
    print(SEPARATOR)

# ---------------------------------------------------------------------------
# UTILITAS: Jalankan mapper/reducer seperti Hadoop Streaming
# ---------------------------------------------------------------------------

def run_streaming_job(mapper_file, reducer_file, input_data, job_name):
    """
    Simulasi Hadoop Streaming pipeline:
    input_data → mapper → sort (shuffle) → reducer → output

    Ini mensimulasikan apa yang dilakukan Hadoop secara terdistribusi.
    """
    # Phase 1: MAP
    mapper_path = os.path.join(BASE_DIR, mapper_file)
    map_process = subprocess.run(
        [sys.executable, mapper_path],
        input=input_data,
        capture_output=True,
        text=True
    )
    mapper_output = map_process.stdout

    # Phase 2: SHUFFLE & SORT (Hadoop mengurutkan key secara global)
    lines = [l for l in mapper_output.strip().split('\n') if l]
    sorted_lines = sorted(lines)
    shuffled_data = '\n'.join(sorted_lines)

    # Phase 3: REDUCE
    reducer_path = os.path.join(BASE_DIR, reducer_file)
    reduce_process = subprocess.run(
        [sys.executable, reducer_path],
        input=shuffled_data,
        capture_output=True,
        text=True
    )
    reducer_output = reduce_process.stdout

    return mapper_output, shuffled_data, reducer_output


# ---------------------------------------------------------------------------
# MAIN RUNNER
# ---------------------------------------------------------------------------

def main():
    print_header("MODUL 5 - MAPREDUCE HOSPITAL EHR ANALYSIS")
    print(f" Dataset: {EHR_FILE}")
    print(f" Output : {OUT_DIR}")

    if not os.path.exists(EHR_FILE):
        print(f"\n[!] Dataset tidak ditemukan: {EHR_FILE}")
        print("    Pastikan full_dataset_ehr.csv ada di folder data/")
        return

    # Baca dataset
    with open(EHR_FILE, 'r', encoding='utf-8') as f:
        input_data = f.read()

    total_rows = input_data.count('\n')
    print(f"\n[+] Dataset dimuat: {total_rows:,} baris")

    # ===========================================================================
    # JOB 1: Hitungan Pasien Berisiko Tinggi per Wilayah
    # ===========================================================================
    print_header("JOB 1: HIGH-RISK PATIENT COUNT BY REGION")
    print(" Map   : Setiap pasien berisiko tinggi → emit(region, 1)")
    print(" Reduce: Jumlahkan count per region")

    _, _, job1_output = run_streaming_job(
        "mapper_highrisk_region.py",
        "reducer_highrisk_region.py",
        input_data,
        "Job1"
    )

    print("\n HASIL (Region → Jumlah Pasien Berisiko Tinggi):")
    print(f" {'Region':<20} {'High-Risk Count':>20}")
    print(" " + "-" * 42)
    job1_results = []
    for line in job1_output.strip().split('\n'):
        if '\t' in line:
            region, count = line.split('\t', 1)
            job1_results.append((region.strip(), int(count.strip())))

    job1_results.sort(key=lambda x: x[1], reverse=True)
    total_hr = 0
    for region, count in job1_results:
        print(f" {region:<20} {count:>20,}")
        total_hr += count
    print(f" {'TOTAL':<20} {total_hr:>20,}")

    # Simpan output
    out1_path = os.path.join(OUT_DIR, "job1_highrisk_by_region.txt")
    with open(out1_path, 'w') as f:
        f.write("Region\tHigh_Risk_Count\n")
        for region, count in job1_results:
            f.write(f"{region}\t{count}\n")
    print(f"\n[✓] Hasil disimpan: {out1_path}")

    # ===========================================================================
    # JOB 2: Analisis Mortalitas per Kondisi Penyakit
    # ===========================================================================
    print_header("JOB 2: MORTALITY ANALYSIS BY CONDITION")
    print(" Map   : Setiap pasien → emit(condition, 1|adverse_outcome)")
    print(" Reduce: Hitung total kasus, adverse, mortality_rate% per kondisi")

    _, _, job2_output = run_streaming_job(
        "mapper_mortality_condition.py",
        "reducer_mortality_condition.py",
        input_data,
        "Job2"
    )

    print("\n HASIL (Kondisi Penyakit → Statistik Mortalitas):")
    print(f" {'Kondisi':<30} {'Total':>8} {'Adverse':>8} {'Mortality%':>12}")
    print(" " + "-" * 62)
    job2_results = []
    for line in job2_output.strip().split('\n'):
        if '\t' in line:
            condition, values = line.split('\t', 1)
            parts = values.split('|')
            if len(parts) == 3:
                job2_results.append((condition.strip(), int(parts[0]), int(parts[1]), parts[2]))

    job2_results.sort(key=lambda x: float(x[3].replace('%','')), reverse=True)
    for cond, total, adverse, rate in job2_results:
        print(f" {cond:<30} {total:>8,} {adverse:>8,} {rate:>12}")

    out2_path = os.path.join(OUT_DIR, "job2_mortality_by_condition.txt")
    with open(out2_path, 'w') as f:
        f.write("Condition\tTotal_Cases\tAdverse_Count\tMortality_Rate\n")
        for cond, total, adverse, rate in job2_results:
            f.write(f"{cond}\t{total}\t{adverse}\t{rate}\n")
    print(f"\n[✓] Hasil disimpan: {out2_path}")

    # ===========================================================================
    # JOB 3: Profil Tekanan Darah per Kondisi & Keparahan
    # ===========================================================================
    print_header("JOB 3: BLOOD PRESSURE PROFILE BY CONDITION & SEVERITY")
    print(" Map   : Setiap pasien → emit(condition_severity, systolic_bp|1)")
    print(" Reduce: Hitung rata-rata systolic_bp per condition+severity")

    _, _, job3_output = run_streaming_job(
        "mapper_bp_profile.py",
        "reducer_bp_profile.py",
        input_data,
        "Job3"
    )

    print("\n HASIL (Kondisi_Keparahan → Rata-rata Tekanan Darah Sistolik):")
    print(f" {'Kondisi_Keparahan':<42} {'Avg Systolic':>14} {'Count':>8}")
    print(" " + "-" * 68)
    job3_results = []
    for line in job3_output.strip().split('\n'):
        if '\t' in line:
            key, values = line.split('\t', 1)
            parts = values.split('|')
            if len(parts) == 2:
                job3_results.append((key.strip(), float(parts[0]), int(parts[1])))

    job3_results.sort(key=lambda x: x[1], reverse=True)
    for key, avg_bp, count in job3_results:
        print(f" {key:<42} {avg_bp:>14.1f} {count:>8,}")

    out3_path = os.path.join(OUT_DIR, "job3_bp_profile.txt")
    with open(out3_path, 'w') as f:
        f.write("Condition_Severity\tAvg_Systolic_BP\tCount\n")
        for key, avg_bp, count in job3_results:
            f.write(f"{key}\t{avg_bp:.2f}\t{count}\n")
    print(f"\n[✓] Hasil disimpan: {out3_path}")

    # ===========================================================================
    # RINGKASAN
    # ===========================================================================
    print_header("RINGKASAN EKSEKUSI MAPREDUCE")
    print(" ✓ Job 1: High-Risk Patient by Region  → Selesai")
    print(" ✓ Job 2: Mortality by Condition       → Selesai")
    print(" ✓ Job 3: BP Profile by Condition      → Selesai")
    print(f"\n Semua output tersimpan di: {OUT_DIR}")
    print(SEPARATOR)

if __name__ == "__main__":
    main()
