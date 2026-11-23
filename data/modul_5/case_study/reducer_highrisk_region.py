#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - CASE STUDY: MAPREDUCE ANALISIS EHR - JOB 1
================================================================================
Reducer: Hitungan Pasien Berisiko Tinggi per Wilayah (Region)
================================================================================
Menerima input terurut dari Shuffle & Sort:
  Central\t1
  Central\t1
  East\t1
  North\t1
  ...

Output:
  Central\t3421
  East\t1876
  North\t2105
  ...
================================================================================
"""

import sys

def main():
    current_region = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        region, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        if region == current_region:
            current_count += count
        else:
            if current_region is not None:
                print(f"{current_region}\t{current_count}")
            current_region = region
            current_count = count

    if current_region is not None:
        print(f"{current_region}\t{current_count}")

if __name__ == "__main__":
    main()
