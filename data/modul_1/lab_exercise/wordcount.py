from pyspark import SparkContext

sc = SparkContext(appName="WordCount")

# Load text file from HDFS
text_rdd = sc.textFile("hdfs://namenode:8020/user/data/sample.txt")

# Split into words and count
word_counts = text_rdd.flatMap(lambda line: line.lower().split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# Display results sorted by frequency
for word, count in word_counts.sortBy(lambda x: x[1], ascending=False).collect():
    print(f"{word}: {count}")

sc.stop()
