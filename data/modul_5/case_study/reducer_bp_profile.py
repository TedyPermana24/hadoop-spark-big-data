#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 3
================================================================================
Reducer: Rata-rata Tekanan Darah Sistolik per Kondisi & Tingkat Keparahan
================================================================================
Input terurut:
  Back Pain_Mild\t120.5|1
  Back Pain_Mild\t135.0|1
  Back Pain_Moderate\t148.0|1
  ...

Output:
  Back Pain_Mild\t127.75|2500
  Back Pain_Moderate\t146.90|1800
  ...
================================================================================
"""

import sys

def main():
    current_key = None
    total_systolic = 0.0
    total_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        key, value_str = parts

        value_parts = value_str.split('|')
        if len(value_parts) != 2:
            continue
        try:
            systolic = float(value_parts[0])
            count = int(value_parts[1])
        except ValueError:
            continue

        if key == current_key:
            total_systolic += systolic
            total_count += count
        else:
            if current_key is not None:
                avg_systolic = total_systolic / total_count if total_count > 0 else 0
                print(f"{current_key}\t{avg_systolic:.2f}|{total_count}")
            current_key = key
            total_systolic = systolic
            total_count = count

    if current_key is not None:
        avg_systolic = total_systolic / total_count if total_count > 0 else 0
        print(f"{current_key}\t{avg_systolic:.2f}|{total_count}")

if __name__ == "__main__":
    main()
