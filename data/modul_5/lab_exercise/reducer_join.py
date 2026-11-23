#!/usr/bin/env python3
"""
reducer_join.py
Menggabungkan data dari dua dataset berdasarkan patient_id (Reduce-Side Join).
Input: patient_id \t [TAG]|[DATA] (sorted by patient_id)
Output: patient_id \t Name \t Region \t Condition \t Severity
"""
import sys

def main():
    current_id = None
    demo_info = None
    diag_info = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) < 2:
            continue

        p_id, payload = parts[0], parts[1]

        if p_id != current_id:
            # Emit join record sebelumnya jika kedua relasi terpenuhi
            if current_id is not None and demo_info and diag_info:
                print(f"{current_id}\t{demo_info}\t{diag_info}")
            current_id = p_id
            demo_info = None
            diag_info = None

        tag_parts = payload.split("|", 1)
        tag = tag_parts[0]
        val = tag_parts[1] if len(tag_parts) > 1 else ""

        if tag == "DEMO":
            demo_info = val.replace("|", "\t")
        elif tag == "DIAG":
            diag_info = val.replace("|", "\t")

    # Emit baris terakhir
    if current_id is not None and demo_info and diag_info:
        print(f"{current_id}\t{demo_info}\t{diag_info}")

if __name__ == "__main__":
    main()
