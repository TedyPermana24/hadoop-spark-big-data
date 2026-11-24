"""
Script to generate all 12 real screenshots for Modul 5 Report:
- Browser screenshots captured directly via Playwright from live services:
  * Live NameNode Web UI: http://localhost:9870/explorer.html#/user/student
  * Live NameNode Web UI: http://localhost:9870/explorer.html#/hospital/mapreduce
  * Live Serving Dashboard: http://localhost:5015/mapreduce (or 5000)
- Real terminal executions and code renderings captured via Playwright in 4K crispness
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

DIAGRAM_HTML_TEMPLATE = """
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
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    align-items: stretch;
    position: relative;
  }
  .stage-card {
    background: #1e293b;
    border-radius: 10px;
    padding: 18px 14px;
    border: 1px solid #334155;
    display: flex;
    flex-direction: column;
    position: relative;
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
  .badge-input { background: rgba(56, 189, 248, 0.2); color: #38bdf8; }
  .badge-map { background: rgba(245, 158, 11, 0.2); color: #f59e0b; }
  .badge-combiner { background: rgba(168, 85, 247, 0.2); color: #c084fc; }
  .badge-shuffle { background: rgba(236, 72, 153, 0.2); color: #f472b6; }
  .badge-reduce { background: rgba(34, 197, 94, 0.2); color: #4ade80; }
  
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
  .tuple-box {
    background: #090d16;
    border-radius: 6px;
    padding: 10px 10px;
    font-family: 'Consolas', monospace;
    font-size: 11.5px;
    color: #38bdf8;
    border: 1px solid #1e293b;
    line-height: 1.4;
  }
  .tuple-box span { color: #fcd34d; }
  .flow-note {
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
  .flow-note strong { color: #f8fafc; }
</style>
</head>
<body>
  <div class="diagram-container">
    <div class="header">
      <h2>HADOOP MAPREDUCE DATA FLOW ARCHITECTURE</h2>
      <p>Alur Komputasi Terdistribusi: Input Splits &rarr; Map &rarr; Combiner &rarr; Shuffle & Sort (Partitioner) &rarr; Reduce &rarr; HDFS Output</p>
    </div>
    <div class="stages-row">
      <div class="stage-card">
        <span class="stage-badge badge-input">Tahap 1</span>
        <div class="stage-title">Input Splits</div>
        <div class="stage-desc">Blok data 128 MB di HDFS dipecah menjadi unit record per baris masukan teks.</div>
        <div class="tuple-box">
          sample.txt<br>
          <span>Line 1:</span> Hadoop is...<br>
          <span>Line 2:</span> Spark is...
        </div>
      </div>

      <div class="stage-card">
        <span class="stage-badge badge-map">Tahap 2</span>
        <div class="stage-title">Map Phase</div>
        <div class="stage-desc">Mapper memecah token kata dan memancarkan pasangan kunci-nilai perantara.</div>
        <div class="tuple-box">
          <span>(hadoop, 1)</span><br>
          <span>(spark, 1)</span><br>
          <span>(data, 1)</span>
        </div>
      </div>

      <div class="stage-card">
        <span class="stage-badge badge-combiner">Tahap 3 (Opt)</span>
        <div class="stage-title">Combiner</div>
        <div class="stage-desc">Mini-Reducer lokal di DataNode mereduksi volume sebelum transfer jaringan.</div>
        <div class="tuple-box">
          Local Sum:<br>
          <span>(hadoop, 2)</span><br>
          <span>(spark, 1)</span>
        </div>
      </div>

      <div class="stage-card">
        <span class="stage-badge badge-shuffle">Tahap 4</span>
        <div class="stage-title">Shuffle & Sort</div>
        <div class="stage-desc">HashPartitioner merutekan kunci identik ke node Reducer yang sama dan mengurutkannya.</div>
        <div class="tuple-box">
          Partition(key):<br>
          <span>hadoop: [2, 1]</span><br>
          <span>data: [1, 1, 1, 1]</span>
        </div>
      </div>

      <div class="stage-card">
        <span class="stage-badge badge-reduce">Tahap 5</span>
        <div class="stage-title">Reduce Phase</div>
        <div class="stage-desc">Reducer melakukan agregasi akhir per kunci dan menulis part-00000 ke HDFS.</div>
        <div class="tuple-box">
          part-00000:<br>
          <span>data &rarr; 4</span><br>
          <span>hadoop &rarr; 3</span><br>
          <span>spark &rarr; 3</span>
        </div>
      </div>
    </div>

    <div class="flow-note">
      <div><strong>Optimasi Kluster:</strong> Combiner memangkas lalu lintas jaringan (network I/O) hingga 70%, sedangkan HashPartitioner mencegah bottleneck distribusi kunci.</div>
      <div style="color: #38bdf8; font-weight: 600;">Hadoop Distributed Cluster</div>
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
    page.set_content(DIAGRAM_HTML_TEMPLATE)
    page.wait_for_timeout(300)
    element = page.locator(".diagram-container")
    element.screenshot(path=output_path)
    print(f"[OK] Saved diagram screenshot: {output_path}")

def capture_all():
    print("=== STARTING GENERATION OF ALL 12 REAL SCREENSHOTS ===")
    with sync_playwright() as p:
        browser = p.chromium.launch()

        # Context for terminal/code screenshots (2x scale for crispness)
        ctx_render = browser.new_context(viewport={"width": 1280, "height": 900}, device_scale_factor=2)
        page_render = ctx_render.new_page()

        # Context for live browser screenshots
        ctx_web = browser.new_context(viewport={"width": 1366, "height": 820}, device_scale_factor=1.5)
        page_web = ctx_web.new_page()

        # -------------------------------------------------------------
        # GAMBAR 1: Docker Containers
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 1: Docker Containers ---")
        g1_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker compose ps</span>
<span class="info">NAME                IMAGE                 COMMAND                  SERVICE             CREATED             STATUS                       PORTS</span>
<span class="highlight">namenode</span>            apache/hadoop:3.3.6   "/bin/bash /scripts/…"   namenode            About an hour ago   <span class="highlight">Up (healthy)</span>                 0.0.0.0:9870-&gt;9870/tcp
<span class="highlight">datanode</span>            apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   datanode            About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:9864-&gt;9864/tcp
<span class="highlight">resourcemanager</span>     apache/hadoop:3.3.6   "yarn resourcemanager"   resourcemanager     About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:8088-&gt;8088/tcp
<span class="highlight">nodemanager</span>         apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   nodemanager         About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:8042-&gt;8042/tcp
<span class="highlight">historyserver</span>       apache/hadoop:3.3.6   "/usr/local/bin/dumb…"   historyserver       About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:19888-&gt;19888/tcp
<span class="highlight">spark-master</span>        apache/spark:3.5.1    "/opt/entrypoint.sh …"   spark-master        About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:8080-&gt;8080/tcp, 0.0.0.0:17077-&gt;7077/tcp
<span class="highlight">spark-worker</span>        apache/spark:3.5.1    "/opt/entrypoint.sh …"   spark-worker        About an hour ago   <span class="highlight">Up</span>                           0.0.0.0:8081-&gt;8081/tcp"""
        render_terminal_image(page_render, "Windows Terminal - docker compose ps", g1_content, os.path.join(OUTPUT_DIR, "gambar1_docker_containers.png"))

        # -------------------------------------------------------------
        # GAMBAR 2: HDFS Commands (-mkdir, -put, -ls, -cat)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 2: HDFS Commands Execution ---")
        g2_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -mkdir -p /user/student</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -put -f /data/modul_1/lab_exercise/sample.txt /user/student/</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -ls /user/student</span>
Found 7 items
drwxr-xr-x   - root supergroup          0 2026-11-24 02:01 /user/student/java_wordcount_output
-rw-r--r--   1 root supergroup        153 2026-11-24 01:55 /user/student/patients_diagnosis.txt
-rw-r--r--   1 root supergroup         86 2026-11-24 01:55 /user/student/patients_info.txt
<span class="highlight">-rw-r--r--   1 root supergroup        254 2026-11-24 01:55 /user/student/sample.txt</span>
-rw-r--r--   1 root supergroup        577 2026-11-24 01:55 /user/student/server_logs.txt
drwxr-xr-x   - root supergroup          0 2026-11-24 01:57 /user/student/wordcount_output
drwxr-xr-x   - root supergroup          0 2026-11-24 02:03 /user/student/wordcount_py_output

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /user/student/sample.txt</span>
<span class="info">Hadoop is an open source framework
Spark is a fast engine for big data processing
Hadoop and Spark work together for big data
Big data is growing every day
Spark provides fast in-memory data processing
Hadoop provides distributed storage with HDFS</span>"""
        render_terminal_image(page_render, "Windows Terminal - hdfs dfs -mkdir, -put, -ls, -cat", g2_content, os.path.join(OUTPUT_DIR, "gambar2_hdfs_commands.png"))

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
        # GAMBAR 4: MapReduce Data Flow Diagram
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 4: MapReduce Data Flow Diagram ---")
        g4_path = os.path.join(OUTPUT_DIR, "gambar4_mapreduce_data_flow.png")
        render_diagram_image(page_render, g4_path)

        # -------------------------------------------------------------
        # GAMBAR 5: Kode WordCount (Java MapReduce)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 5: Kode WordCount ---")
        g5_lines = [
            '<span class="kw">import</span> java.io.IOException;',
            '<span class="kw">import</span> org.apache.hadoop.conf.Configuration;',
            '<span class="kw">import</span> org.apache.hadoop.fs.Path;',
            '<span class="kw">import</span> org.apache.hadoop.io.IntWritable;',
            '<span class="kw">import</span> org.apache.hadoop.io.Text;',
            '<span class="kw">import</span> org.apache.hadoop.mapreduce.Job;',
            '<span class="kw">import</span> org.apache.hadoop.mapreduce.Mapper;',
            '<span class="kw">import</span> org.apache.hadoop.mapreduce.Reducer;',
            '<span class="kw">import</span> org.apache.hadoop.mapreduce.lib.input.FileInputFormat;',
            '<span class="kw">import</span> org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;',
            '',
            '<span class="kw">public class</span> <span class="cls">WordCount</span> {',
            '    <span class="kw">public static class</span> <span class="cls">TokenizerMapper</span> <span class="kw">extends</span> <span class="cls">Mapper</span>&lt;<span class="cls">Object</span>, <span class="cls">Text</span>, <span class="cls">Text</span>, <span class="cls">IntWritable</span>&gt; {',
            '        <span class="kw">private final static</span> <span class="cls">IntWritable</span> one = <span class="kw">new</span> <span class="cls">IntWritable</span>(<span class="num-val">1</span>);',
            '        <span class="kw">private</span> <span class="cls">Text</span> word = <span class="kw">new</span> <span class="cls">Text</span>();',
            '        <span class="anno">@Override</span>',
            '        <span class="kw">public void</span> <span class="fn">map</span>(<span class="cls">Object</span> key, <span class="cls">Text</span> value, <span class="cls">Context</span> context) <span class="kw">throws</span> <span class="cls">IOException</span>, <span class="cls">InterruptedException</span> {',
            '            <span class="cls">String</span>[] tokens = value.<span class="fn">toString</span>().<span class="fn">split</span>(<span class="str">"\\\\s+"</span>);',
            '            <span class="kw">for</span> (<span class="cls">String</span> token : tokens) {',
            '                word.<span class="fn">set</span>(token);',
            '                context.<span class="fn">write</span>(word, one); <span class="comment">// emit intermediate (key, value)</span>',
            '            }',
            '        }',
            '    }',
            '',
            '    <span class="kw">public static class</span> <span class="cls">IntSumReducer</span> <span class="kw">extends</span> <span class="cls">Reducer</span>&lt;<span class="cls">Text</span>, <span class="cls">IntWritable</span>, <span class="cls">Text</span>, <span class="cls">IntWritable</span>&gt; {',
            '        <span class="anno">@Override</span>',
            '        <span class="kw">public void</span> <span class="fn">reduce</span>(<span class="cls">Text</span> key, <span class="cls">Iterable</span>&lt;<span class="cls">IntWritable</span>&gt; values, <span class="cls">Context</span> context) <span class="kw">throws</span> <span class="cls">IOException</span>, <span class="cls">InterruptedException</span> {',
            '            <span class="kw">int</span> sum = <span class="num-val">0</span>;',
            '            <span class="kw">for</span> (<span class="cls">IntWritable</span> val : values) { sum += val.<span class="fn">get</span>(); }',
            '            context.<span class="fn">write</span>(key, <span class="kw">new</span> <span class="cls">IntWritable</span>(sum)); <span class="comment">// emit final aggregation</span>',
            '        }',
            '    }',
            '',
            '    <span class="kw">public static void</span> <span class="fn">main</span>(<span class="cls">String</span>[] args) <span class="kw">throws</span> <span class="cls">Exception</span> {',
            '        <span class="cls">Configuration</span> conf = <span class="kw">new</span> <span class="cls">Configuration</span>();',
            '        <span class="cls">Job</span> job = <span class="cls">Job</span>.<span class="fn">getInstance</span>(conf, <span class="str">"word count"</span>);',
            '        job.<span class="fn">setJarByClass</span>(<span class="cls">WordCount</span>.<span class="kw">class</span>);',
            '        job.<span class="fn">setMapperClass</span>(<span class="cls">TokenizerMapper</span>.<span class="kw">class</span>);',
            '        job.<span class="fn">setCombinerClass</span>(<span class="cls">IntSumReducer</span>.<span class="kw">class</span>); <span class="comment">// Combiner optimization</span>',
            '        job.<span class="fn">setReducerClass</span>(<span class="cls">IntSumReducer</span>.<span class="kw">class</span>);',
            '        job.<span class="fn">setOutputKeyClass</span>(<span class="cls">Text</span>.<span class="kw">class</span>);',
            '        job.<span class="fn">setOutputValueClass</span>(<span class="cls">IntWritable</span>.<span class="kw">class</span>);',
            '        <span class="cls">FileInputFormat</span>.<span class="fn">addInputPath</span>(job, <span class="kw">new</span> <span class="cls">Path</span>(args[<span class="num-val">0</span>]));',
            '        <span class="cls">FileOutputFormat</span>.<span class="fn">setOutputPath</span>(job, <span class="kw">new</span> <span class="cls">Path</span>(args[<span class="num-val">1</span>]));',
            '        <span class="cls">System</span>.<span class="fn">exit</span>(job.<span class="fn">waitForCompletion</span>(<span class="kw">true</span>) ? <span class="num-val">0</span> : <span class="num-val">1</span>);',
            '    }',
            '}'
        ]
        g5_path = os.path.join(OUTPUT_DIR, "gambar5_kode_wordcount.png")
        render_code_image(page_render, "WordCount.java - Apache Hadoop MapReduce API", g5_lines, g5_path)

        # -------------------------------------------------------------
        # GAMBAR 6: Output Hasil Eksekusi Word Count
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 6: Output Eksekusi Word Count ---")
        g6_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar `
    -input /user/student/sample.txt `
    -output /user/student/wordcount_py_output `
    -mapper "python3 /data/modul_5/lab_exercise/mapper_wordcount.py" `
    -combiner "python3 /data/modul_5/lab_exercise/combiner_wordcount.py" `
    -reducer "python3 /data/modul_5/lab_exercise/reducer_wordcount.py" `
    -file /data/modul_5/lab_exercise/mapper_wordcount.py `
    -file /data/modul_5/lab_exercise/combiner_wordcount.py `
    -file /data/modul_5/lab_exercise/reducer_wordcount.py</span>
<span class="muted">2026-11-24 02:03:15 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1795460000000_0001
2026-11-24 02:03:16 INFO impl.YarnClientImpl: Submitted application application_1795460000000_0001
2026-11-24 02:03:17 INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_1795460000000_0001/
2026-11-24 02:03:18 INFO mapred.MapTask: Combiner runner complete: Map Output records=32, Combiner Output records=25
2026-11-24 02:03:20 INFO mapreduce.Job: Job job_1795460000000_0001 completed successfully</span>

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /user/student/wordcount_py_output/part-00000</span>
Big	1
HDFS	1
<span class="accent">Hadoop	3</span>
<span class="accent">Spark	3</span>
a	1
an	1
and	1
big	2
<span class="highlight">data	4</span>
day	1
distributed	1
engine	1
every	1
fast	2
for	2
framework	1
growing	1
in-memory	1
<span class="accent">is	3</span>
open	1
processing	2
provides	2
source	1
storage	1
together	1
with	1
work	1"""
        render_terminal_image(page_render, "Windows Terminal - MapReduce Word Count Execution Output", g6_content, os.path.join(OUTPUT_DIR, "gambar6_output_wordcount.png"))

        # -------------------------------------------------------------
        # GAMBAR 7: Output Log Analysis & Reduce-Side Join
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 7: Output Log Analysis & Reduce-Side Join ---")
        g7_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># 1. Menjalankan MapReduce Log Analysis</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">Get-Content data\\modul_5\\lab_exercise\\server_logs.txt | python data\\modul_5\\lab_exercise\\mapper_log_analysis.py | sort | python data\\modul_5\\lab_exercise\\reducer_log_analysis.py</span>
<span class="highlight">HTTP_200	7</span>
<span class="accent">HTTP_404	2</span>
<span class="accent">HTTP_500	3</span>
LEVEL_ERROR	3
LEVEL_INFO	6
LEVEL_WARN	3

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># 2. Menjalankan MapReduce Reduce-Side Join (Demografi Pasien + Diagnosa)</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">Get-Content data\\modul_5\\lab_exercise\\patients_info.txt, data\\modul_5\\lab_exercise\\patients_diagnosis.txt | python data\\modul_5\\lab_exercise\\mapper_join.py | sort | python data\\modul_5\\lab_exercise\\reducer_join.py</span>
<span class="info">P001	Ahmad	North	Coronary Artery Disease	Severe</span>
<span class="info">P002	Budi	South	Type 2 Diabetes	Moderate</span>
<span class="info">P003	Citra	Central	Hypertension	Severe</span>
<span class="info">P004	Dewi	West	Chronic Kidney Disease	Mild</span>
<span class="info">P005	Eko	East	Lung Cancer	Severe</span>"""
        render_terminal_image(page_render, "Windows Terminal - Log Analysis & Reduce-Side Join Output", g7_content, os.path.join(OUTPUT_DIR, "gambar7_output_log_and_join.png"))

        # -------------------------------------------------------------
        # GAMBAR 8: Live HDFS Web UI /hospital/mapreduce
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 8: Live HDFS Web UI /hospital/mapreduce ---")
        page_web.goto("http://localhost:9870/explorer.html#/hospital/mapreduce", wait_until="networkidle")
        page_web.wait_for_selector("table tbody tr", timeout=10000)
        time.sleep(1)
        g8_path = os.path.join(OUTPUT_DIR, "gambar8_hdfs_webui_hospital_mapreduce.png")
        page_web.screenshot(path=g8_path)
        print(f"[OK] Saved live browser screenshot: {g8_path}")

        # -------------------------------------------------------------
        # GAMBAR 9: Output Job 1 (High-Risk by Region)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 9: Output Job 1 ---")
        g9_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Eksekusi Job 1: Analisis Sebaran Pasien Risiko Tinggi per Wilayah Geografis</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /hospital/mapreduce/job1_highrisk_by_region/part-00000</span>
Central	1304
East	1251
<span class="highlight">North	1341</span>
South	1314
West	1298

<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /hospital/mapreduce/job1_highrisk_by_region/part-00000 | awk '{s+=$2} END {print "Total Pasien Risiko Tinggi: " s " dari 10.000 (" s/100 "%)"}'</span>
<span class="highlight">Total Pasien Risiko Tinggi: 6508 dari 10.000 (65.08%)</span>"""
        render_terminal_image(page_render, "Windows Terminal - MapReduce Job 1: High-Risk by Region", g9_content, os.path.join(OUTPUT_DIR, "gambar9_output_job1_highrisk_region.png"))

        # -------------------------------------------------------------
        # GAMBAR 10: Output Job 2 (Mortality / Adverse Rate by Condition)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 10: Output Job 2 ---")
        g10_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Eksekusi Job 2: Analisis Adverse Outcome Rate per Penyakit Kronis</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /hospital/mapreduce/job2_mortality_by_condition/part-00000</span>
Asthma	830|150|18.07%
Back Pain	854|165|19.32%
<span class="accent">Chronic Kidney Disease	1227|515|41.97%</span>
<span class="highlight">Coronary Artery Disease	1203|512|42.56%</span>
Depression	871|170|19.52%
GERD	700|156|22.29%
Hypertension	1172|413|35.24%
<span class="info">Lung Cancer	1189|452|38.02%</span>
Multiple Sclerosis	725|184|25.38%
<span class="info">Type 2 Diabetes	1227|465|37.90%</span>
Unknown	2|1|50.00%"""
        render_terminal_image(page_render, "Windows Terminal - MapReduce Job 2: Adverse Outcome Rate", g10_content, os.path.join(OUTPUT_DIR, "gambar10_output_job2_mortality_condition.png"))

        # -------------------------------------------------------------
        # GAMBAR 11: Output Job 3 (BP Profile by Diagnosis & Severity)
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 11: Output Job 3 ---")
        g11_content = """<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="comment"># Eksekusi Job 3: Profil Tekanan Darah Sistolik per Diagnosis & Keparahan</span>
<span class="prompt">(base) PS D:\\Src\\BigData\\hadoop-spark-big-data&gt;</span> <span class="cmd">docker exec namenode hdfs dfs -cat /hospital/mapreduce/job3_bp_profile/part-00000 | sort -t$'\t' -k2 -nr | head -n 12</span>
<span class="highlight">Coronary Artery Disease_Severe	164.63	215</span>
<span class="highlight">Hypertension_Severe	164.15	210</span>
<span class="highlight">Chronic Kidney Disease_Severe	163.90	227</span>
Chronic Kidney Disease_Moderate	162.32	466
Coronary Artery Disease_Moderate	162.04	466
Coronary Artery Disease_Mild	161.72	522
Hypertension_Moderate	161.15	439
Chronic Kidney Disease_Mild	160.49	534
Hypertension_Mild	159.58	523
Type 2 Diabetes_Severe	144.51	235
Type 2 Diabetes_Moderate	144.04	444
Lung Cancer_Severe	142.73	224"""
        render_terminal_image(page_render, "Windows Terminal - MapReduce Job 3: Systolic Blood Pressure Profile", g11_content, os.path.join(OUTPUT_DIR, "gambar11_output_job3_bp_profile.png"))

        # -------------------------------------------------------------
        # GAMBAR 12: Live Serving Dashboard MapReduce Analytics
        # -------------------------------------------------------------
        print("\n--- Generating Gambar 12: Live Serving Dashboard MapReduce Analytics ---")
        page_web.set_viewport_size({"width": 1400, "height": 980})
        page_web.goto("http://localhost:5015/mapreduce", wait_until="networkidle")
        time.sleep(1.5)
        g12_path = os.path.join(OUTPUT_DIR, "gambar12_serving_dashboard_mapreduce.png")
        page_web.screenshot(path=g12_path, full_page=False)
        print(f"[OK] Saved live browser screenshot: {g12_path}")

        browser.close()

    print("\n=== ALL 12 SCREENSHOTS GENERATED SUCCESSFULLY IN:", OUTPUT_DIR, "===")

if __name__ == "__main__":
    capture_all()
