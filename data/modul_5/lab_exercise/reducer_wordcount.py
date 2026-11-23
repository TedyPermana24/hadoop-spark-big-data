#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)
for line in sys.stdin:
    word, num = line.strip().split("\t")
    counts[word] += int(num)

for word, count in counts.items():
    print(f"{word}\t{count}")
