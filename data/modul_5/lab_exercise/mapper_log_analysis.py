#!/usr/bin/env python3
"""
mapper_log_analysis.py
Memproses berkas log server untuk mengekstrak status HTTP dan Log Level.
Input format : YYYY-MM-DD HH:MM:SS [LEVEL] [ENDPOINT] [STATUS_CODE] [LATENCY]
Output emit  : STATUS_[code]\t1  atau  LEVEL_[level]\t1
"""
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        # Contoh: ['2026-11-23', '08:14:10', 'ERROR', '/api/patients', '500', '310ms']
        if len(parts) >= 5:
            level = parts[2]
            status_code = parts[4]
            # Emit status code
            print(f"HTTP_{status_code}\t1")
            # Emit log level
            print(f"LEVEL_{level}\t1")

if __name__ == "__main__":
    main()
