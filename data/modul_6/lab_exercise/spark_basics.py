"""
================================================================================
MODUL 6: LAB EXERCISE - GETTING STARTED WITH PYSPARK
================================================================================
Implementasi resmi sesuai modul IFB-451 Big Data:
1. RDD from list & reduce action (Image 2 - Step 3)
2. DataFrame from CSV & show (Image 2 - Step 4)
3. Spark SQL query on temporary view (Image 2 - Step 5)
4. Word Count with RDD: flatMap, map, reduceByKey (Image 3)
================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark import SparkContext

def main():
    print("=" * 70)
    print(" 1. INISIALISASI SPARK SESSION & SPARK CONTEXT")
    print("=" * 70)
    spark = SparkSession.builder \
        .appName("Basics") \
        .getOrCreate()
    sc = spark.sparkContext
    print("[✓] SparkSession dan SparkContext berhasil diinisialisasi.\n")

    # --------------------------------------------------------------------------
    # TASK 3: Create an RDD from a list (Image 2 - Step 3)
    # --------------------------------------------------------------------------
    print("=" * 70)
    print(" 2. TASK 3: RDD FROM LIST & REDUCE ACTION")
    print("=" * 70)
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)
    rdd_sum = rdd.reduce(lambda a, b: a + b)
    print(f"Data List : {data}")
    print(f"Sum       : {rdd_sum}\n")

    # --------------------------------------------------------------------------
    # TASK 4: Create a DataFrame from a CSV file (Image 2 - Step 4)
    # --------------------------------------------------------------------------
    print("=" * 70)
    print(" 3. TASK 4: DATAFRAME FROM CSV FILE")
    print("=" * 70)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "data.csv")
    
    # Baca CSV dengan header dan inferSchema
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print("[✓] Schema DataFrame:")
    df.printSchema()
    print("[✓] Menampilkan 5 baris pertama data:")
    df.show(5)

    # --------------------------------------------------------------------------
    # TASK 5: Run a SQL query (Image 2 - Step 5)
    # --------------------------------------------------------------------------
    print("=" * 70)
    print(" 4. TASK 5: SPARK SQL QUERY (TEMPORARY VIEW)")
    print("=" * 70)
    df.createOrReplaceTempView("people")
    result = spark.sql("SELECT name, age FROM people WHERE age > 30")
    print("[✓] Hasil kueri SQL (SELECT name, age FROM people WHERE age > 30):")
    result.show()

    # --------------------------------------------------------------------------
    # SAMPLE CODE: Word Count with RDD (Image 3)
    # --------------------------------------------------------------------------
    print("=" * 70)
    print(" 5. SAMPLE CODE: WORD COUNT WITH RDD")
    print("=" * 70)
    sample_txt_path = os.path.join(base_dir, "sample.txt")
    text_rdd = sc.textFile(sample_txt_path)
    
    word_counts = (text_rdd
                   .flatMap(lambda line: line.split())
                   .map(lambda word: (word, 1))
                   .reduceByKey(lambda a, b: a + b))

    print("[✓] 10 Kata Pertama dari Word Count RDD:")
    for word, count in word_counts.take(10):
        print(f"  {word:<15} : {count}")

    print("\n" + "=" * 70)
    print(" [✓] SELURUH TAHAPAN LAB EXERCISE MODUL 6 BERHASIL DIJALANKAN!")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()
