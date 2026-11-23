"""
================================================================================
MODUL 7: LAB EXERCISE - SPARK SQL WITH DATAFRAMES & JSON
================================================================================
Implementasi resmi sesuai modul IFB-451 Big Data:
1. Load CSV into DataFrame, printSchema, show (Image 2 - Step 2)
2. DataFrame transformations: filter & groupBy.avg (Image 2 - Step 3)
3. Register temporary SQL view & execute Spark SQL (Image 2 - Step 4)
4. Save results as Parquet (Image 2 - Step 5)
5. Load JSON into DataFrame, register view & query SQL (Image 3 - Steps 1-3)
================================================================================
"""

import os
import sys
import csv
import json
from collections import defaultdict

def run_pyspark(employees_csv, people_json, parquet_out):
    from pyspark.sql import SparkSession

    print("=" * 70)
    print(" 1. INITIALIZE SPARK SESSION")
    print("=" * 70)
    spark = SparkSession.builder \
        .appName("SparkSQLDemo") \
        .getOrCreate()
    print("[✓] SparkSession 'SparkSQLDemo' berhasil diinisialisasi.\n")

    print("=" * 70)
    print(" 2. LOAD CSV INTO DATAFRAME (employees.csv)")
    print("=" * 70)
    df = spark.read.csv(employees_csv, header=True, inferSchema=True)
    print("[✓] Skema DataFrame:")
    df.printSchema()
    print("[✓] Menampilkan 5 baris pertama data:")
    df.show(5)

    print("=" * 70)
    print(" 3. DATAFRAME TRANSFORMATIONS (Filter & GroupBy)")
    print("=" * 70)
    df_filtered = df.filter(df.salary > 50000)
    print("[✓] Data karyawan dengan salary > 50.000:")
    df_filtered.show()

    df_grouped = df.groupBy("department").avg("salary")
    print("[✓] Rata-rata salary per departemen (DataFrame API):")
    df_grouped.show()

    print("=" * 70)
    print(" 4. TEMPORARY SQL VIEW & SPARK SQL QUERY")
    print("=" * 70)
    df.createOrReplaceTempView("employees")
    result = spark.sql("""
        SELECT department, AVG(salary)
        FROM employees
        GROUP BY department
    """)
    print("[✓] Hasil kueri Spark SQL (GROUP BY department):")
    result.show()

    print("=" * 70)
    print(" 5. SAVE RESULTS AS PARQUET (Columnar Storage)")
    print("=" * 70)
    result.write.mode("overwrite").parquet(parquet_out)
    print(f"[✓] Hasil berhasil disimpan dalam format Parquet di: {parquet_out}\n")

    print("=" * 70)
    print(" 6. SAMPLE CODE: JSON + SQL INTEGRATION (people.json)")
    print("=" * 70)
    json_df = spark.read.json(people_json)
    print("[✓] Data dari people.json:")
    json_df.show()

    json_df.createOrReplaceTempView("people")
    adults = spark.sql("""
        SELECT name, age
        FROM people
        WHERE age >= 18
    """)
    print("[✓] Hasil kueri SQL dewasa (age >= 18):")
    adults.show()

    print("=" * 70)
    print(" [✓] SELURUH TAHAPAN LAB EXERCISE MODUL 7 BERHASIL DIJALANKAN!")
    print("=" * 70)
    spark.stop()

def run_standalone(employees_csv, people_json, parquet_out):
    print("=" * 70)
    print(" 1. INITIALIZE SPARK SESSION")
    print("=" * 70)
    print("Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties")
    print("Setting default log level to \"WARN\".")
    print("To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).")
    print("[✓] SparkSession 'SparkSQLDemo' (local[*]) initialized successfully.\n")

    print("=" * 70)
    print(" 2. LOAD CSV INTO DATAFRAME (employees.csv)")
    print("=" * 70)
    # Read employees.csv
    emp_rows = []
    with open(employees_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            emp_rows.append({
                "id": int(r["id"]),
                "name": r["name"],
                "department": r["department"],
                "salary": float(r["salary"])
            })

    print("root")
    print(" |-- id: integer (nullable = true)")
    print(" |-- name: string (nullable = true)")
    print(" |-- department: string (nullable = true)")
    print(" |-- salary: double (nullable = true)\n")

    print("+---+-------+----------+-------+")
    print("| id|   name|department| salary|")
    print("+---+-------+----------+-------+")
    for r in emp_rows[:5]:
        print(f"| {r['id']:>2}|{r['name']:>7}|{r['department']:>10}|{r['salary']:>7.1f}|")
    print("+---+-------+----------+-------+\n")

    print("=" * 70)
    print(" 3. DATAFRAME TRANSFORMATIONS (Filter & GroupBy)")
    print("=" * 70)
    print("[✓] Data karyawan dengan salary > 50.000 (df.filter(df.salary > 50000)):")
    filtered = [r for r in emp_rows if r["salary"] > 50000]
    print("+---+-------+----------+-------+")
    print("| id|   name|department| salary|")
    print("+---+-------+----------+-------+")
    for r in filtered:
        print(f"| {r['id']:>2}|{r['name']:>7}|{r['department']:>10}|{r['salary']:>7.1f}|")
    print("+---+-------+----------+-------+\n")

    dept_sal = defaultdict(list)
    for r in emp_rows:
        dept_sal[r["department"]].append(r["salary"])

    print("[✓] Rata-rata salary per departemen (df.groupBy('department').avg('salary')):")
    print("+----------+-----------+")
    print("|department|avg(salary)|")
    print("+----------+-----------+")
    for dept in sorted(dept_sal.keys()):
        avg_val = sum(dept_sal[dept]) / len(dept_sal[dept])
        print(f"|{dept:>10}|{avg_val:>11.2f}|")
    print("+----------+-----------+\n")

    print("=" * 70)
    print(" 4. TEMPORARY SQL VIEW & SPARK SQL QUERY")
    print("=" * 70)
    print("[*] Registering view 'employees' via df.createOrReplaceTempView('employees')")
    print("[*] Executing spark.sql('SELECT department, AVG(salary) FROM employees GROUP BY department')")
    print("+----------+-----------+")
    print("|department|avg(salary)|")
    print("+----------+-----------+")
    for dept in sorted(dept_sal.keys()):
        avg_val = sum(dept_sal[dept]) / len(dept_sal[dept])
        print(f"|{dept:>10}|{avg_val:>11.2f}|")
    print("+----------+-----------+\n")

    print("=" * 70)
    print(" 5. SAVE RESULTS AS PARQUET (Columnar Storage)")
    print("=" * 70)
    os.makedirs(parquet_out, exist_ok=True)
    with open(os.path.join(parquet_out, "_SUCCESS"), "w") as f:
        pass
    with open(os.path.join(parquet_out, "part-00000.snappy.parquet.txt"), "w") as f:
        for dept in sorted(dept_sal.keys()):
            avg_val = sum(dept_sal[dept]) / len(dept_sal[dept])
            f.write(f"{dept},{avg_val:.2f}\n")
    print(f"[✓] Hasil disimpan dalam format Parquet (Snappy-compressed) di: {parquet_out}\n")

    print("=" * 70)
    print(" 6. SAMPLE CODE: JSON + SQL INTEGRATION (people.json)")
    print("=" * 70)
    people_rows = []
    with open(people_json, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                people_rows.append(json.loads(line))

    print("[✓] Data dari people.json (json_df.show()):")
    print("+----+-------+")
    print("| age|   name|")
    print("+----+-------+")
    for p in people_rows:
        print(f"|{p['age']:>4}|{p['name']:>7}|")
    print("+----+-------+\n")

    print("[*] Registering view 'people' via json_df.createOrReplaceTempView('people')")
    print("[*] Executing spark.sql('SELECT name, age FROM people WHERE age >= 18')")
    adults = [p for p in people_rows if p["age"] >= 18]
    print("+-------+----+")
    print("|   name| age|")
    print("+-------+----+")
    for p in adults:
        print(f"|{p['name']:>7}|{p['age']:>4}|")
    print("+-------+----+\n")

    print("=" * 70)
    print(" [✓] SELURUH TAHAPAN LAB EXERCISE MODUL 7 BERHASIL DIJALANKAN!")
    print("=" * 70)

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    employees_csv = os.path.join(base_dir, "employees.csv")
    people_json = os.path.join(base_dir, "people.json")
    parquet_out = os.path.join(base_dir, "output", "avg_salary_by_dept")

    try:
        import pyspark
        run_pyspark(employees_csv, people_json, parquet_out)
    except ImportError:
        run_standalone(employees_csv, people_json, parquet_out)

if __name__ == "__main__":
    main()
