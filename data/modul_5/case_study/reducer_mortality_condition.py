#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 2
================================================================================
Reducer: Analisis Mortalitas per Kondisi Penyakit
================================================================================
Menerima input terurut:
  Back Pain\t1|0
  Back Pain\t1|1
  Coronary Artery Disease\t1|0
  ...

Output:
  Back Pain\t2500|180|7.20%
  Coronary Artery Disease\t2200|660|30.00%
  Depression\t2100|399|19.00%
  Type 2 Diabetes\t3200|768|24.00%
================================================================================
"""

import sys

def main():
    current_condition = None
    total_cases = 0
    total_adverse = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        condition, value_str = parts

        # Parse nilai: "total_count|adverse_count"
        value_parts = value_str.split('|')
        if len(value_parts) != 2:
            continue
        try:
            count = int(value_parts[0])
            adverse = int(value_parts[1])
        except ValueError:
            continue

        if condition == current_condition:
            total_cases += count
            total_adverse += adverse
        else:
            if current_condition is not None:
                mortality_rate = (total_adverse / total_cases * 100) if total_cases > 0 else 0
                print(f"{current_condition}\t{total_cases}|{total_adverse}|{mortality_rate:.2f}%")
            current_condition = condition
            total_cases = count
            total_adverse = adverse

    # Emit kondisi terakhir
    if current_condition is not None:
        mortality_rate = (total_adverse / total_cases * 100) if total_cases > 0 else 0
        print(f"{current_condition}\t{total_cases}|{total_adverse}|{mortality_rate:.2f}%")

if __name__ == "__main__":
    main()
