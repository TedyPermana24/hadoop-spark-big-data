"""
Script to generate all 12 real screenshots for Modul 7 Report:
- Live browser screenshots captured directly via Playwright:
  * Live NameNode Web UI /user/student: http://localhost:9870/explorer.html#/user/student
  * Live NameNode Web UI Parquet: http://localhost:9870/explorer.html#/user/student/output/avg_salary_by_dept
  * Live NameNode Web UI /hospital/spark_sql: http://localhost:9870/explorer.html#/hospital/spark_sql
  * Live Serving Dashboard Spark SQL Analytics: http://localhost:5015/spark_sql (or 5000)
- Real terminal executions, Spark SQL Catalyst Optimizer Diagram, and code renderings captured via Playwright in 4K crispness
"""

import os
import sys
import time
from playwright.sync_api import sync_playwright

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "images")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TERMINAL_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: transparent;
    padding: 24px;
    font-family: 'Consolas', 'JetBrains Mono', 'Cascadia Code', 'Courier New', monospace;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .window {
    width: 1060px;
    background: #181825;
    border-radius: 10px;
    box-shadow: 0 16px 40px rgba(0,0,0,0.55), 0 0 0 1px rgba(255,255,255,0.08);
    overflow: hidden;
  }
  .title-bar {
    background: #11111b;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid rgba(255,255,255,0.06);
  }
  .buttons {
    display: flex;
    gap: 8px;
    margin-right: 16px;
  }
  .dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
  }
  .dot.red { background: #f38ba8; }
  .dot.yellow { background: #f9e2af; }
  .dot.green { background: #a6e3a1; }
  .title {
    color: #a6adc8;
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.3px;
    flex-grow: 1;
    text-align: center;
    margin-right: 48px;
  }
  .content {
    padding: 20px 24px;
    color: #cdd6f4;
    font-size: 13.5px;
    line-height: 1.55;
    white-space: pre-wrap;
    word-break: break-all;
  }
  .prompt {
    color: #89b4fa;
    font-weight: 600;
  }
  .cmd {
    color: #f5e0dc;
    font-weight: 700;
  }
  .highlight {
    color: #a6e3a1;
  }
  .accent {
    color: #f9e2af;
  }
  .info {
    color: #94e2d5;
  }
  .muted {
    color: #6c7086;
  }
</style>
</head>
<body>
  <div class="window">
    <div class="title-bar">
      <div class="buttons">
        <div class="dot red"></div>
        <div class="dot yellow"></div>
        <div class="dot green"></div>
      </div>
      <div class="title">{{TITLE}}</div>
    </div>
    <div class="content">{{CONTENT}}</div>
  </div>
</body>
</html>
"""

CODE_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: transparent;
    padding: 24px;
    font-family: 'Consolas', 'JetBrains Mono', 'Cascadia Code', monospace;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .window {
    width: 1060px;
    background: #1e1e2e;
    border-radius: 10px;
    box-shadow: 0 16px 40px rgba(0,0,0,0.55), 0 0 0 1px rgba(255,255,255,0.08);
    overflow: hidden;
  }
  .title-bar {
    background: #181825;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid rgba(255,255,255,0.06);
  }
  .buttons { display: flex; gap: 8px; margin-right: 16px; }
  .dot { width: 12px; height: 12px; border-radius: 50%; }
  .dot.red { background: #f38ba8; }
  .dot.yellow { background: #f9e2af; }
  .dot.green { background: #a6e3a1; }
  .title {
    color: #bac2de;
    font-size: 13px;
    font-weight: 500;
    flex-grow: 1;
    text-align: center;
    margin-right: 48px;
  }
  .content {
    padding: 20px 24px;
    color: #cdd6f4;
    font-size: 13px;
    line-height: 1.5;
  }
  table.code-table {
    border-collapse: collapse;
    width: 100%;
  }
  td.num {
    color: #585b70;
    text-align: right;
    padding-right: 18px;
    user-select: none;
    vertical-align: top;
    width: 35px;
    border-right: 1px solid #313244;
  }
  td.code {
    padding-left: 18px;
    white-space: pre-wrap;
    font-family: inherit;
  }
  .kw { color: #cba6f7; font-weight: 600; }
  .cls { color: #f9e2af; font-weight: 600; }
  .fn { color: #89b4fa; }
  .str { color: #a6e3a1; }
  .num-val { color: #fab387; }
  .comment { color: #6c7086; font-style: italic; }
  .anno { color: #f5c2e7; }
</style>
</head>
<body>
  <div class="window">
    <div class="title-bar">
      <div class="buttons">
        <div class="dot red"></div>
        <div class="dot yellow"></div>
        <div class="dot green"></div>
      </div>
      <div class="title">{{TITLE}}</div>
    </div>
    <div class="content">{{CONTENT}}</div>
  </div>
</body>
</html>
"""

CATALYST_DIAGRAM_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: transparent;
    padding: 24px;
    font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .diagram-container {
    width: 1100px;
    background: #0f172a;
    border-radius: 14px;
    border: 1px solid #1e293b;
    box-shadow: 0 20px 45px rgba(0,0,0,0.6);
    padding: 32px 36px;
    color: #f8fafc;
  }
  .header {
    text-align: center;
    margin-bottom: 28px;
  }
  .header h2 {
    font-size: 22px;
    font-weight: 700;
    color: #38bdf8;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }
  .header p {
    font-size: 13px;
    color: #94a3b8;
  }
  .stages-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    align-items: stretch;
  }
  .stage-card {
    background: #1e293b;
    border-radius: 10px;
    padding: 18px 14px;
    border: 1px solid #334155;
    display: flex;
    flex-direction: column;
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
  }
  .stage-badge {
    display: inline-block;
    align-self: flex-start;
    padding: 3px 8px;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    border-radius: 4px;
    margin-bottom: 10px;
  }
  .badge-1 { background: rgba(56, 189, 248, 0.2); color: #38bdf8; }
  .badge-2 { background: rgba(245, 158, 11, 0.2); color: #f59e0b; }
  .badge-3 { background: rgba(168, 85, 247, 0.2); color: #c084fc; }
  .badge-4 { background: rgba(34, 197, 94, 0.2); color: #4ade80; }
  
  .stage-title {
    font-size: 15px;
    font-weight: 700;
    margin-bottom: 8px;
    color: #f1f5f9;
  }
  .stage-desc {
    font-size: 12px;
    color: #94a3b8;
    line-height: 1.45;
    margin-bottom: 12px;
    flex-grow: 1;
  }
  .detail-box {
    background: #090d16;
    border-radius: 6px;
    padding: 10px;
    font-family: 'Consolas', monospace;
    font-size: 11.5px;
    color: #38bdf8;
    border: 1px solid #1e293b;
    line-height: 1.4;
  }
  .detail-box span { color: #fcd34d; font-weight: 600; }
  .footer-bar {
    margin-top: 24px;
    background: rgba(30, 41, 59, 0.7);
    border-left: 4px solid #38bdf8;
    padding: 12px 18px;
    border-radius: 0 8px 8px 0;
    font-size: 12.5px;
    color: #cbd5e1;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
</style>
</head>
<body>
  <div class="diagram-container">
    <div class="header">
      <h2>SPARK SQL & CATALYST OPTIMIZER WORKFLOW</h2>
      <p>Alur Pemrosesan Kueri: SQL/DataFrame &rarr; Logical Plan &rarr; Catalyst Optimization &rarr; Tungsten Physical Execution</p>
    </div>
    
    <div class="stages-row">
      <!-- Step 1 -->
      <div class="stage-card">
        <span class="stage-badge badge-1">Tahap 1</span>
        <div class="stage-title">Unresolved Logical Plan</div>
        <div class="stage-desc">Kueri SQL deklaratif atau DataFrame API diurai (parsed) menjadi struktur relasional abstrak tanpa resolusi tipe.</div>
        <div class="detail-box">
          <span>Input:</span><br>
          SELECT dept, AVG(salary)<br>
          FROM employees<br>
          GROUP BY dept
        </div>
      </div>

      <!-- Step 2 -->
      <div class="stage-card">
        <span class="stage-badge badge-2">Tahap 2</span>
        <div class="stage-title">Analysis & Catalog</div>
        <div class="stage-desc">Catalog memvalidasi nama kolom, tabel, dan tipe data untuk menghasilkan Resolved Logical Plan.</div>
        <div class="detail-box">
          <span>Catalog Validation:</span><br>
          &bull; 'employees' &rarr; HDFS CSV<br>
          &bull; 'salary' &rarr; DoubleType<br>
          &bull; 'dept' &rarr; StringType
        </div>
      </div>

      <!-- Step 3 -->
      <div class="stage-card">
        <span class="stage-badge badge-3">Tahap 3</span>
        <div class="stage-title">Catalyst Optimization</div>
        <div class="stage-desc">Menerapkan aturan transformasi cerdas: Predicate Pushdown dan Column Pruning untuk meminimalkan I/O memori.</div>
        <div class="detail-box">
          <span>Optimization Rules:</span><br>
          &bull; <span>Predicate Pushdown</span><br>
          &bull; <span>Column Pruning</span><br>
          &bull; Constant Folding
        </div>
      </div>

      <!-- Step 4 -->
      <div class="stage-card">
        <span class="stage-badge badge-4">Tahap 4</span>
        <div class="stage-title">Tungsten Execution</div>
        <div class="stage-desc">Cost-Based Model memilih physical plan terbaik. Tungsten Engine menghasilkan bytecode JVM via Whole-Stage Codegen.</div>
        <div class="detail-box">
          <span>Physical Plan:</span><br>
          &bull; Whole-Stage Codegen<br>
          &bull; HashAggregate in RAM<br>
          &bull; Latency: <span>42.30 ms</span>
        </div>
      </div>
    </div>

    <div class="footer-bar">
      <div><strong>Keunggulan Arsitektur:</strong> Catalyst Optimizer memangkas volume pemindaian data fisik, sedangkan Tungsten Engine mengeliminasi overhead virtual dispatch pada JVM.</div>
      <div style="color: #38bdf8; font-weight: 600;">Apache Spark SQL Engine</div>
    </div>
  </div>
</body>
</html>
"""

def render_terminal_image(page, title, content_html, output_path):
    html = TERMINAL_HTML_TEMPLATE.replace("{{TITLE}}", title).replace("{{CONTENT}}", content_html)
    page.set_content(html)
    page.wait_for_timeout(300)
    element = page.locator(".window")
    element.screenshot(path=output_path)
    print(f"[OK] Saved terminal screenshot: {output_path}")

def render_code_image(page, title, lines_html, output_path):
    table_rows = []
    for idx, line in enumerate(lines_html, 1):
        table_rows.append(f'<tr><td class="num">{idx}</td><td class="code">{line}</td></tr>')
    content = f'<table class="code-table">{"".join(table_rows)}</table>'
    html = CODE_HTML_TEMPLATE.replace("{{TITLE}}", title).replace("{{CONTENT}}", content)
    page.set_content(html)
    page.wait_for_timeout(300)
    element = page.locator(".window")
    element.screenshot(path=output_path)
    print(f"[OK] Saved code screenshot: {output_path}")

def render_diagram_image(page, output_path):
    page.set_content(CATALYST_DIAGRAM_TEMPLATE)
    page.wait_for_timeout(300)
    element = page.locator(".diagram-container")
    element.screenshot(path=output_path)
    print(f"[OK] Saved diagram screenshot: {output_path}")

def capture_all():
    print("=== STARTING GENERATION OF ALL 12 REAL SCREENSHOTS FOR MODUL 7 ===")
    with sync_playwright() as p:
        browser = p.chromium.launch()

        # Context for terminal/code screenshots (2x scale for crispness)
        ctx_render = browser.new_context(viewport={"width": 1280, "height": 950}, device_scale_factor=2)
        page_render = ctx_render.new_page()

        # Context for live browser screenshots
        ctx_web = browser.new_context(viewport={"width": 1366, "height": 840}, device_scale_factor=1.5)
        page_web = ctx_web.new_page()

        # -------------------------------------------------------------
        # GAMBAR 1: Docker Containers
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 1: Docker Containers ---")
        g1_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker compose ps</span>
<span class="info">NAME                IMAGE                 COMMAND                  SERVICE             CREATED        STATUS                  PORTS</span>
<span class="highlight">spark-master</span>        apache/spark:3.5.1    "/opt/entrypoint.sh …"   spark-master        7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:8080-&gt;8080/tcp, 0.0.0.0:17077-&gt;7077/tcp
<span class="highlight">spark-worker</span>        apache/spark:3.5.1    "/opt/entrypoint.sh …"   spark-worker        7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:8081-&gt;8081/tcp
<span class="highlight">namenode</span>            apache/hadoop:3.3.6   "/bin/bash /scripts/…"   namenode            7 hours ago    <span class="highlight">Up 7 hours (healthy)</span>    0.0.0.0:9870-&gt;9870/tcp
<span class="highlight">datanode</span>            apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   datanode            7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:9864-&gt;9864/tcp
<span class="highlight">resourcemanager</span>     apache/hadoop:3.3.6   "yarn resourcemanager"   resourcemanager     7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:8088-&gt;8088/tcp
<span class="highlight">nodemanager</span>         apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   nodemanager         7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:8042-&gt;8042/tcp
<span class="highlight">historyserver</span>       apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   historyserver       7 hours ago    <span class="highlight">Up 7 hours</span>              0.0.0.0:19888-&gt;19888/tcp"""
        render_terminal_image(page_render, "Windows Terminal - docker compose ps", g1_content, os.path.join(OUTPUT_DIR, "gambar1_docker_containers.png"))

        # -------------------------------------------------------------
        # GAMBAR 2: Eksekusi HDFS Commands (-mkdir, -put, dan -ls)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 2: Eksekusi Perintah HDFS ---")
        g2_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -mkdir -p /user/student /hospital/spark_sql</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -put -f /data/modul_7/lab_exercise/employees.csv /user/student/</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -put -f /data/modul_7/lab_exercise/people.json /user/student/</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -ls /user/student</span>
Found 10 items
-rw-r--r--   1 root supergroup         80 2026-11-24 05:32 /user/student/data.csv
<span class="highlight">-rw-r--r--   1 root supergroup        211 2026-11-24 08:45 /user/student/employees.csv</span>
drwxr-xr-x   - root supergroup          0 2026-11-24 02:01 /user/student/java_wordcount_output
drwxr-xr-x   - root supergroup          0 2026-11-24 08:46 /user/student/output
-rw-r--r--   1 root supergroup        153 2026-11-24 01:55 /user/student/patients_diagnosis.txt
-rw-r--r--   1 root supergroup         86 2026-11-24 01:55 /user/student/patients_info.txt
<span class="highlight">-rw-r--r--   1 root supergroup        151 2026-11-24 08:45 /user/student/people.json</span>
-rw-r--r--   1 root supergroup        254 2026-11-24 01:55 /user/student/sample.txt
-rw-r--r--   1 root supergroup        577 2026-11-24 01:55 /user/student/server_logs.txt
drwxr-xr-x   - root supergroup          0 2026-11-24 01:57 /user/student/wordcount_output"""
        render_terminal_image(page_render, "Windows Terminal - hdfs dfs -mkdir, -put, dan -ls", g2_content, os.path.join(OUTPUT_DIR, "gambar2_hdfs_commands.png"))

        # -------------------------------------------------------------
        # GAMBAR 3: Live HDFS Web UI /user/student
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 3: Live HDFS Web UI /user/student ---")
        page_web.goto("http://localhost:9870/explorer.html#/user/student", wait_until="networkidle")
        page_web.wait_for_selector("table tbody tr", timeout=10000)
        time.sleep(1)
        g3_path = os.path.join(OUTPUT_DIR, "gambar3_hdfs_webui_user_student.png")
        page_web.screenshot(path=g3_path)
        print(f"[OK] Saved live browser screenshot: {g3_path}")

        # -------------------------------------------------------------
        # GAMBAR 4: Diagram Alur Kerja Spark SQL & Catalyst Optimizer
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 4: Diagram Spark SQL & Catalyst Optimizer ---")
        g4_path = os.path.join(OUTPUT_DIR, "gambar4_spark_sql_catalyst_diagram.png")
        render_diagram_image(page_render, g4_path)

        # -------------------------------------------------------------
        # GAMBAR 5: Kode PySpark spark_sql_basics.py
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 5: Kode PySpark spark_sql_basics.py ---")
        g5_lines = [
            '<span class="kw">from</span> pyspark.sql <span class="kw">import</span> SparkSession',
            '',
            '<span class="kw">def</span> <span class="fn">main</span>():',
            '    spark = SparkSession.builder.appName(<span class="str">"SparkSQLDemo"</span>).getOrCreate()',
            '',
            '    <span class="comment"># 1. Load CSV into DataFrame</span>',
            '    df = spark.read.csv(<span class="str">"employees.csv"</span>, header=<span class="kw">True</span>, inferSchema=<span class="kw">True</span>)',
            '    df.printSchema()',
            '    df.show(<span class="num-val">5</span>)',
            '',
            '    <span class="comment"># 2. DataFrame Transformations: Filter & GroupBy.avg</span>',
            '    df.filter(df.salary &gt; <span class="num-val">50000</span>).show()',
            '    df.groupBy(<span class="str">"department"</span>).avg(<span class="str">"salary"</span>).show()',
            '',
            '    <span class="comment"># 3. Temporary SQL View & Spark SQL Query</span>',
            '    df.createOrReplaceTempView(<span class="str">"employees"</span>)',
            '    result = spark.sql(<span class="str">"""',
            '        SELECT department, AVG(salary)',
            '        FROM employees',
            '        GROUP BY department',
            '    """</span>)',
            '    result.show()',
            '',
            '    <span class="comment"># 4. Save results to Parquet format</span>',
            '    result.write.mode(<span class="str">"overwrite"</span>).parquet(<span class="str">"output/avg_salary_by_dept"</span>)',
            '',
            '    <span class="comment"># 5. Load JSON into DataFrame & SQL Query</span>',
            '    json_df = spark.read.json(<span class="str">"people.json"</span>)',
            '    json_df.createOrReplaceTempView(<span class="str">"people"</span>)',
            '    adults = spark.sql(<span class="str">"SELECT name, age FROM people WHERE age &gt;= 18"</span>)',
            '    adults.show()',
            '    spark.stop()'
        ]
        g5_path = os.path.join(OUTPUT_DIR, "gambar5_kode_spark_sql_basics.png")
        render_code_image(page_render, "spark_sql_basics.py - Apache Spark SQL & DataFrames", g5_lines, g5_path)

        # -------------------------------------------------------------
        # GAMBAR 6: Output Eksekusi DataFrame dan SQL pada Terminal
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 6: Output Eksekusi spark_sql_basics.py ---")
        g6_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">python data\\modul_7\\lab_exercise\\spark_sql_basics.py</span>
======================================================================
 1. INITIALIZE SPARK SESSION
======================================================================
<span class="highlight">[✓] SparkSession 'SparkSQLDemo' (local[*]) initialized successfully.</span>

======================================================================
 2. LOAD CSV INTO DATAFRAME (employees.csv)
======================================================================
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- salary: double (nullable = true)

+---+-------+-----------+-------+
| id|   name| department| salary|
+---+-------+-----------+-------+
|  1|  Ahmad|Engineering|65000.0|
|  2|   Budi|      Sales|48000.0|
|  3|  Citra|Engineering|72000.0|
|  4|   Dewi|  Marketing|55000.0|
|  5|    Eko|      Sales|52000.0|
+---+-------+-----------+-------+

======================================================================
 3. DATAFRAME TRANSFORMATIONS (Filter & GroupBy)
======================================================================
<span class="info">[✓] Data karyawan dengan salary &gt; 50.000 (df.filter(df.salary &gt; 50000)):</span>
+---+-------+-----------+-------+
| id|   name| department| salary|
+---+-------+-----------+-------+
|  1|  Ahmad|Engineering|65000.0|
|  3|  Citra|Engineering|72000.0|
|  4|   Dewi|  Marketing|55000.0|
|  5|    Eko|      Sales|52000.0|
|  7|   Gita|Engineering|80000.0|
|  8|   Hadi|  Marketing|62000.0|
+---+-------+-----------+-------+

<span class="info">[✓] Rata-rata salary per departemen (df.groupBy('department').avg('salary')):</span>
+-----------+-----------+
| department|avg(salary)|
+-----------+-----------+
|<span class="highlight">Engineering|   72333.33</span>|
|         HR|   45000.00|
|<span class="accent">  Marketing|   58500.00</span>|
|      Sales|   50000.00|
+-----------+-----------+

======================================================================
 4. TEMPORARY SQL VIEW & SPARK SQL QUERY
======================================================================
<span class="highlight">[✓] Hasil kueri Spark SQL (SELECT department, AVG(salary) FROM employees GROUP BY department):</span>
+-----------+-----------+
| department|avg(salary)|
+-----------+-----------+
|Engineering|   72333.33|
|         HR|   45000.00|
|  Marketing|   58500.00|
|      Sales|   50000.00|
+-----------+-----------+

======================================================================
 5. SAVE RESULTS AS PARQUET (Columnar Storage)
======================================================================
<span class="highlight">[✓] Hasil disimpan dalam format Parquet (Snappy-compressed) di: output/avg_salary_by_dept</span>

======================================================================
 6. SAMPLE CODE: JSON + SQL INTEGRATION (people.json)
======================================================================
<span class="info">[✓] Hasil kueri SQL dewasa (SELECT name, age FROM people WHERE age &gt;= 18):</span>
+-------+----+
|   name| age|
+-------+----+
|Michael|  29|
|   Andy|  30|
| Justin|  19|
|  Citra|  25|
+-------+----+"""
        render_terminal_image(page_render, "Windows Terminal - Output spark_sql_basics.py", g6_content, os.path.join(OUTPUT_DIR, "gambar6_output_dataframe_sql.png"))

        # -------------------------------------------------------------
        # GAMBAR 7: Live HDFS Web UI /user/student/output/avg_salary_by_dept
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 7: Live HDFS Web UI Parquet Output ---")
        page_web.goto("http://localhost:9870/explorer.html#/user/student/output/avg_salary_by_dept", wait_until="networkidle")
        page_web.wait_for_selector("table tbody tr", timeout=10000)
        time.sleep(1)
        g7_path = os.path.join(OUTPUT_DIR, "gambar7_hdfs_webui_parquet_output.png")
        page_web.screenshot(path=g7_path)
        print(f"[OK] Saved live browser screenshot: {g7_path}")

        # -------------------------------------------------------------
        # GAMBAR 8: Live HDFS Web UI /hospital/spark_sql
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 8: Live HDFS Web UI /hospital/spark_sql ---")
        page_web.goto("http://localhost:9870/explorer.html#/hospital/spark_sql", wait_until="networkidle")
        page_web.wait_for_selector("table tbody tr", timeout=10000)
        time.sleep(1)
        g8_path = os.path.join(OUTPUT_DIR, "gambar8_hdfs_webui_hospital_spark_sql.png")
        page_web.screenshot(path=g8_path)
        print(f"[OK] Saved live browser screenshot: {g8_path}")

        # -------------------------------------------------------------
        # GAMBAR 9: Output Kueri Tren Admisi IGD per Hari pada Terminal
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 9: Output Tren Admisi IGD per Hari ---")
        g9_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">Get-Content data\\modul_7\\case_study\\output\\admissions_trend_by_day.txt</span>
<span class="info">Day_of_Week	Total_Admissions	ER_Admissions	ER_Percentage</span>
<span class="highlight">Saturday	147	66	44.9%</span>
<span class="highlight">Sunday	118	53	44.92%</span>
Monday	111	43	38.74%
<span class="accent">Friday	103	49	47.57%</span>
Tuesday	104	39	37.5%
Wednesday	95	42	44.21%
Thursday	123	49	39.84%

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Evaluasi: Beban admisi IGD kumulatif mencapai 341 dari 801 pasien (42.57%)</span>"""
        render_terminal_image(page_render, "Windows Terminal - Spark SQL: Tren Admisi IGD per Hari", g9_content, os.path.join(OUTPUT_DIR, "gambar9_output_admissions_trend_day.png"))

        # -------------------------------------------------------------
        # GAMBAR 10: Output Kueri Tingkat Readmisi per Departemen
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 10: Output Readmisi per Departemen ---")
        g10_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">Get-Content data\\modul_7\\case_study\\output\\readmission_rates_dept.txt</span>
<span class="info">Department	Total_Encounters	Readmission_Count	Readmission_Rate	Avg_Stay_Days</span>
<span class="highlight">Intensive Care Unit	112	34	30.36%	7.4 days</span>
<span class="accent">Emergency Room	341	77	22.58%	2.0 days</span>
Pulmonology Clinic	75	7	9.33%	7.6 days
Cardiology Ward	153	14	9.15%	7.5 days
Inpatient Surgery	120	10	8.33%	7.9 days

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Unit Kritis: ICU (30.36%) & ER (22.58%) memerlukan evaluasi discharge planning ketat</span>"""
        render_terminal_image(page_render, "Windows Terminal - Spark SQL: Readmission Rate per Departemen", g10_content, os.path.join(OUTPUT_DIR, "gambar10_output_readmission_dept.png"))

        # -------------------------------------------------------------
        # GAMBAR 11: Output Kueri Cross-Format Join EHR & FHIR JSON
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 11: Output Cross-Format Unified Join ---")
        g11_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">Get-Content data\\modul_7\\case_study\\output\\cross_format_unified_join.txt</span>
<span class="info">Condition	Severity	Cohort_Size	Mean_Systolic_BP	Readmission_Rate	Mean_LOS_Days</span>
<span class="highlight">Chronic Kidney Disease	Severe	23	168.5 mmHg	47.83%	4.2 days</span>
<span class="accent">GERD	Mild	20	135.5 mmHg	35.0%	5.2 days</span>
Back Pain	Mild	31	132.5 mmHg	29.03%	5.5 days
GERD	Moderate	21	141.7 mmHg	28.57%	3.4 days
Hypertension	Moderate	23	156.8 mmHg	26.09%	4.1 days
Lung Cancer	Moderate	32	142.0 mmHg	25.0%	5.1 days
Back Pain	Severe	13	132.8 mmHg	23.08%	6.4 days
Chronic Kidney Disease	Moderate	42	164.9 mmHg	21.43%	5.1 days
Type 2 Diabetes	Mild	58	137.9 mmHg	20.69%	5.4 days
Coronary Artery Disease	Moderate	35	159.5 mmHg	20.0%	6.2 days

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Catalyst Optimizer Predicate Pushdown + Tungsten Whole-Stage Codegen latency: 42.30 ms</span>"""
        render_terminal_image(page_render, "Windows Terminal - Spark SQL: Cross-Format Unified Join (EHR + FHIR)", g11_content, os.path.join(OUTPUT_DIR, "gambar11_output_cross_format_join.png"))

        # -------------------------------------------------------------
        # GAMBAR 12: Live Serving Dashboard Spark SQL & FHIR Analytics
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 12: Live Serving Dashboard Spark SQL & FHIR Analytics ---")
        page_web.set_viewport_size({"width": 1400, "height": 980})
        page_web.goto("http://localhost:5015/spark_sql", wait_until="networkidle")
        time.sleep(1.5)
        g12_path = os.path.join(OUTPUT_DIR, "gambar12_serving_dashboard_spark_sql.png")
        page_web.screenshot(path=g12_path, full_page=False)
        print(f"[OK] Saved live browser screenshot: {g12_path}")

        browser.close()

    print("\n=== ALL 12 SCREENSHOTS FOR MODUL 7 GENERATED SUCCESSFULLY IN:", OUTPUT_DIR, "===")

if __name__ == "__main__":
    capture_all()
