"""
================================================================================
APACHE CASSANDRA TEST
================================================================================
"""

import subprocess
import os
import sys
import time
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CQL_INIT_FILE = os.path.join(BASE_DIR, "init_cassandra.cql")

def run_cql_command(cql_text):
    cmd = ["docker", "exec", "-i", "cassandra", "cqlsh"]
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input=cql_text)
    return process.returncode, stdout, stderr

def main():
    print("=" * 75)
    print(" APACHE CASSANDRA TEST")
    print("=" * 75)

    # 1. Inisialisasi Skema
    if os.path.exists(CQL_INIT_FILE):
        with open(CQL_INIT_FILE, "r") as f:
            cql_content = f.read()
        ret, _, err = run_cql_command(cql_content)
        if ret != 0:
            print(f"[!] Error skema: {err}")
            return
    else:
        return

    # 2. Pengujian Penulisan Data (100 Record)
    batch_statements = ["USE hospital_telemetry;"]
    patients = [
        ("P-101", "BED-01", "Ahmad Rizki", "Suspect Sepsis"),
        ("P-102", "BED-02", "Dewi Lestari", "Post-Op Recovery"),
        ("P-103", "BED-03", "Budi Santoso", "Septic Shock Alert"),
        ("P-104", "BED-04", "Siti Nurhaliza", "Pneumonia Acute")
    ]

    total_records = 100
    for _ in range(total_records):
        p = random.choice(patients)
        stmt = f"""INSERT INTO icu_patient_vitals 
(patient_id, timestamp, bed_id, patient_name, heart_rate, spo2, temperature, respiratory_rate, systolic_bp, diastolic_bp, condition)
VALUES ('{p[0]}', toTimestamp(now()), '{p[1]}', '{p[2]}', {random.randint(70, 140)}, {random.randint(78, 100)}, {round(random.uniform(36.0, 39.8), 1)}, {random.randint(14, 32)}, {random.randint(68, 140)}, {random.randint(40, 90)}, '{p[3]}');"""
        batch_statements.append(stmt)

    t0_write = time.time()
    ret_write, _, err_write = run_cql_command("\n".join(batch_statements))
    t_write_total = time.time() - t0_write

    if ret_write == 0:
        avg_latency_ms = round((t_write_total / total_records) * 1000, 2)
        throughput_rps = round(total_records / t_write_total, 1)
        print(" HASIL PENGUJIAN PENULISAN DATA (WRITE TEST):")
        print(f" • Total Data                : {total_records} data sensor")
        print(f" • Waktu Eksekusi            : {round(t_write_total, 3)} detik")
        print(f" • Rata-rata Waktu Tulis     : {avg_latency_ms} ms / data")
        print(f" • Throughput                : {throughput_rps} writes / detik")
        print("-" * 75)

    # 3. Kueri Data Pasien
    print(" HASIL KUERI DATA PASIEN ICU:")
    query_cql = """USE hospital_telemetry;
SELECT patient_id, bed_id, patient_name, heart_rate, spo2, temperature, systolic_bp, condition 
FROM icu_patient_vitals 
LIMIT 4;"""

    ret_q, out_q, _ = run_cql_command(query_cql)
    if ret_q == 0:
        print(out_q.strip())

    print("=" * 75)

if __name__ == "__main__":
    main()
