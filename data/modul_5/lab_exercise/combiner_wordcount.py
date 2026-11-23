#!/usr/bin/env python3
"""
================================================================================
MODUL 5 - LAB EXERCISE: COMBINER (LOCAL REDUCER) UNTUK WORD COUNT
================================================================================
Combiner adalah LOCAL REDUCER yang berjalan di sisi Mapper sebelum data
dikirim melalui jaringan ke Reducer utama.

MANFAAT COMBINER:
  - Mengurangi volume data yang ditransfer antar node (network traffic)
  - Mempercepat fase Shuffle & Sort
  - Mengurangi beban Reducer

PENTING: Combiner hanya boleh digunakan jika operasinya KOMUTAIF dan ASOSIATIF
  (seperti SUM, MAX, MIN) — BUKAN untuk operasi AVERAGE (rata-rata).

Dalam Word Count, Combiner = Reducer karena SUM bersifat asosiatif:
  Mapper Node-1 output: (hadoop,1),(hadoop,1),(spark,1)
  Combiner Node-1:      (hadoop,2),(spark,1)        <- hanya dikirim ini
  Reducer menerima:     lebih sedikit data!

Cara jalankan via Hadoop Streaming dengan Combiner:
  docker exec namenode hadoop jar \
    /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /user/student/sample.txt \
    -output /user/student/wordcount_combiner_output \
    -mapper /data/modul_5/lab_exercise/mapper_wordcount.py \
    -combiner /data/modul_5/lab_exercise/combiner_wordcount.py \
    -reducer /data/modul_5/lab_exercise/reducer_wordcount.py \
    -file /data/modul_5/lab_exercise/mapper_wordcount.py \
    -file /data/modul_5/lab_exercise/combiner_wordcount.py \
    -file /data/modul_5/lab_exercise/reducer_wordcount.py
================================================================================
"""

import sys

def main():
    """
    Combiner identik dengan Reducer untuk operasi SUM.
    Menerima output mapper (sudah di-sort lokal), akumulasikan per key.
    """
    current_word = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        word, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        if word == current_word:
            current_count += count
        else:
            if current_word is not None:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    if current_word is not None:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    main()
