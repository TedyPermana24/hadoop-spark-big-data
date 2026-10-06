"""
================================================================================
SERVING LAYER: Clinician Dashboard & REST API
================================================================================
Menyajikan data dari:
1. Speed Layer: Tanda vital monitor ICU real-time & alert sepsis (MEWS).
2. Batch Layer: Data riwayat rekam medis (EHR) untuk studi klinis & AI.

Desain: Minimalist, clean, modern, zero-logo.
Dapat dijalankan langsung di Windows:
  python d:\\TEDY\\hadoop-spark\\data\\modul_1\\case_study_hospital\\serving_dashboard.py
Buka browser: http://localhost:5000
================================================================================
"""

import os
import glob
import json
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EHR_FILE = os.path.join(BASE_DIR, "ehr_patients.json")

LATEST_VITALS = {}

def start_kafka_listener():
    """Background listener yang membaca streaming tanda vital langsung dari Kafka broker."""
    def listener_thread():
        from kafka import KafkaConsumer
        # Coba port external Windows (localhost:29092) lalu port internal Docker (kafka:9092)
        candidates = ["localhost:29092", "kafka:9092"]
        consumer = None
        for broker in candidates:
            try:
                consumer = KafkaConsumer(
                    "icu-vitals",
                    bootstrap_servers=broker,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000
                )
                print(f"[✓] Serving Layer terhubung langsung ke Kafka broker: {broker}")
                break
            except Exception:
                continue

        if not consumer:
            print("[!] Serving Layer belum dapat terhubung ke Kafka. Pastikan Kafka sudah berjalan.")
            return

        while True:
            try:
                for msg in consumer:
                    item = msg.value
                    if isinstance(item, dict):
                        # Perhitungan Early Warning Score (MEWS)
                        score = 0
                        if item.get("heart_rate", 0) > 110: score += 2
                        if item.get("spo2", 100) < 90: score += 3
                        elif item.get("spo2", 100) < 94: score += 1
                        if item.get("temperature", 37) > 38.5: score += 2
                        if item.get("systolic_bp", 120) < 90: score += 2
                        if item.get("respiratory_rate", 16) > 24: score += 2

                        item["sepsis_score"] = score
                        if score >= 6:
                            item["alert_level"] = "CRITICAL"
                            item["alert_text"] = "CRITICAL SEPSIS ALERT"
                        elif score >= 4:
                            item["alert_level"] = "HIGH"
                            item["alert_text"] = "HIGH RISK WARNING"
                        else:
                            item["alert_level"] = "NORMAL"
                            item["alert_text"] = "STABLE"

                        LATEST_VITALS[item.get("bed_id", "UNKNOWN")] = item
            except Exception as e:
                time.sleep(1)

    t = threading.Thread(target=listener_thread, daemon=True)
    t.start()

start_kafka_listener()

def get_latest_icu_vitals():
    """Mengambil tanda vital ICU terkini langsung dari memori Kafka consumer."""
    return list(LATEST_VITALS.values())

def get_historical_ehr():
    """Membaca data rekam medis pasien dari Batch Layer."""
    if not os.path.exists(EHR_FILE):
        return []
    records = []
    try:
        with open(EHR_FILE, "r") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
    except Exception as e:
        print(f"Error reading EHR: {e}")
    return records

# ─────────────────────────────────────────────────────────────────────────────
# MINIMALIST DARK HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Clinical Monitoring & Research Platform</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #09090b;
            --surface: #121215;
            --surface-elevated: #18181b;
            --border: #27272a;
            --border-subtle: #1f1f23;
            --text: #f4f4f5;
            --text-secondary: #a1a1aa;
            --text-muted: #71717a;
            --danger: #ef4444;
            --warning: #f59e0b;
            --success: #10b981;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            -webkit-font-smoothing: antialiased;
        }

        body {
            background-color: var(--bg);
            color: var(--text);
            padding: 32px 40px;
            max-width: 1400px;
            margin: 0 auto;
        }

        /* Header */
        header {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            margin-bottom: 32px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
        }

        .title-group h1 {
            font-size: 20px;
            font-weight: 600;
            letter-spacing: -0.02em;
            color: var(--text);
        }

        .title-group p {
            font-size: 13px;
            color: var(--text-muted);
            margin-top: 4px;
        }

        .status-indicator {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            color: var(--text-secondary);
            font-weight: 500;
        }

        .live-dot {
            width: 6px;
            height: 6px;
            background-color: var(--success);
            border-radius: 50%;
        }

        /* Architecture Metrics */
        .metrics-strip {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 16px;
            margin-bottom: 36px;
        }

        .metric-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 16px 20px;
        }

        .metric-label {
            font-size: 11px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 6px;
        }

        .metric-value {
            font-size: 14px;
            font-weight: 500;
            color: var(--text);
            margin-bottom: 2px;
        }

        .metric-desc {
            font-size: 12px;
            color: var(--text-secondary);
        }

        /* Section Headings */
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .section-title {
            font-size: 14px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: var(--text-secondary);
        }

        /* ICU Grid */
        .icu-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(290px, 1fr));
            gap: 16px;
            margin-bottom: 44px;
        }

        .bed-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 18px 20px;
            transition: border-color 0.15s ease;
        }

        .bed-card.critical {
            border-color: rgba(239, 68, 68, 0.4);
            background: #140d10;
        }

        .bed-card.high {
            border-color: rgba(245, 158, 11, 0.4);
            background: #14120d;
        }

        .bed-card.normal {
            border-color: var(--border);
        }

        .card-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .bed-id {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-muted);
            letter-spacing: 0.02em;
        }

        .alert-pill {
            font-size: 10px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 4px;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }

        .alert-pill.critical {
            color: #f87171;
            background: rgba(239, 68, 68, 0.12);
            border: 1px solid rgba(239, 68, 68, 0.25);
        }

        .alert-pill.high {
            color: #fbbf24;
            background: rgba(245, 158, 11, 0.12);
            border: 1px solid rgba(245, 158, 11, 0.25);
        }

        .alert-pill.normal {
            color: #34d399;
            background: rgba(16, 185, 129, 0.08);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }

        .patient-info {
            margin-bottom: 16px;
        }

        .patient-name {
            font-size: 15px;
            font-weight: 600;
            color: var(--text);
        }

        .patient-condition {
            font-size: 12px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .vitals-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 16px;
        }

        .vital-box {
            background: var(--surface-elevated);
            border: 1px solid var(--border-subtle);
            border-radius: 6px;
            padding: 10px 12px;
        }

        .vital-label {
            font-size: 11px;
            color: var(--text-muted);
        }

        .vital-num {
            font-size: 18px;
            font-weight: 600;
            color: var(--text);
            margin-top: 2px;
        }

        .vital-unit {
            font-size: 11px;
            font-weight: 400;
            color: var(--text-muted);
        }

        .vital-num.alert {
            color: #f87171;
        }

        .vital-num.warn {
            color: #fbbf24;
        }

        .card-bottom {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-top: 12px;
            border-top: 1px solid var(--border-subtle);
            font-size: 12px;
            color: var(--text-secondary);
        }

        .score-val {
            font-weight: 600;
            font-size: 13px;
        }

        /* Batch Layer Table */
        .table-wrap {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            overflow: hidden;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 13px;
        }

        thead {
            background: var(--surface-elevated);
        }

        th {
            color: var(--text-muted);
            font-weight: 500;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            padding: 12px 16px;
            border-bottom: 1px solid var(--border);
        }

        td {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-subtle);
            color: var(--text-secondary);
        }

        tr:last-child td {
            border-bottom: none;
        }

        tr:hover td {
            color: var(--text);
            background: rgba(255, 255, 255, 0.015);
        }

        .tag {
            font-size: 11px;
            font-weight: 500;
            padding: 2px 6px;
            border-radius: 4px;
        }

        .tag.recovered {
            color: #34d399;
            background: rgba(16, 185, 129, 0.1);
        }

        .tag.deceased {
            color: #f87171;
            background: rgba(239, 68, 68, 0.1);
        }
    </style>
</head>
<body>
    <header>
        <div class="title-group">
            <h1>Clinical Monitoring & Research Platform</h1>
            <p>University Hospital · Hybrid Big Data Architecture</p>
        </div>
        <div class="status-indicator">
            <span class="live-dot"></span> Real-Time Active
        </div>
    </header>

    <!-- Architecture Strip -->
    <div class="metrics-strip">
        <div class="metric-card">
            <div class="metric-label">Batch Layer</div>
            <div class="metric-value">Hadoop HDFS & Spark</div>
            <div class="metric-desc">Long-term EHR storage & clinical AI model training</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Speed Layer</div>
            <div class="metric-value">Apache Kafka & Streaming</div>
            <div class="metric-desc">ICU vitals stream & early warning sepsis alerts</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Serving Layer</div>
            <div class="metric-value">REST API & Dashboard</div>
            <div class="metric-desc">Real-time clinician feed & historical data endpoints</div>
        </div>
    </div>

    <!-- Live ICU Section -->
    <div class="section-header">
        <span class="section-title">Speed Layer · Real-Time ICU Monitors</span>
    </div>
    <div class="icu-grid" id="icuGrid">
        <div style="color: var(--text-muted); font-size: 13px;">Waiting for stream incoming data...</div>
    </div>

    <!-- Batch EHR Section -->
    <div class="section-header">
        <span class="section-title">Batch Layer · Retrospective EHR Records (HDFS)</span>
    </div>
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Patient ID</th>
                    <th>Name</th>
                    <th>Age</th>
                    <th>Department</th>
                    <th>Diagnosis</th>
                    <th>Avg Heart Rate</th>
                    <th>Avg SpO2</th>
                    <th>Lactate</th>
                    <th>Outcome</th>
                </tr>
            </thead>
            <tbody>
                {% for p in patients %}
                <tr>
                    <td><strong style="color: var(--text);">{{ p.patient_id }}</strong></td>
                    <td>{{ p.name }}</td>
                    <td>{{ p.age }}</td>
                    <td>{{ p.department }}</td>
                    <td>{{ p.diagnosis }}</td>
                    <td>{{ p.heart_rate_avg }} bpm</td>
                    <td>{{ p.spo2_avg }}%</td>
                    <td>{{ p.lab_lactate }} mmol/L</td>
                    <td><span class="tag {{ p.outcome|lower }}">{{ p.outcome }}</span></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

    <script>
        async function fetchVitals() {
            try {
                const res = await fetch('/api/vitals');
                const data = await res.json();
                const container = document.getElementById('icuGrid');

                if (!data || data.length === 0) {
                    container.innerHTML = '<div style="color: var(--text-muted); font-size: 13px; grid-column: 1/-1;">No streaming vitals detected. Run the producer script to stream sensor data.</div>';
                    return;
                }

                let html = '';
                data.forEach(p => {
                    const statusClass = p.alert_level === 'CRITICAL' ? 'critical' : (p.alert_level === 'HIGH' ? 'high' : 'normal');
                    const hrAlert = p.heart_rate > 110 ? 'alert' : '';
                    const spo2Alert = p.spo2 < 90 ? 'alert' : (p.spo2 < 94 ? 'warn' : '');

                    html += `
                        <div class="bed-card ${statusClass}">
                            <div class="card-top">
                                <span class="bed-id">${p.bed_id} · ${p.patient_id}</span>
                                <span class="alert-pill ${statusClass}">${p.alert_text}</span>
                            </div>
                            <div class="patient-info">
                                <div class="patient-name">${p.patient_name}</div>
                                <div class="patient-condition">${p.condition}</div>
                            </div>
                            <div class="vitals-grid">
                                <div class="vital-box">
                                    <div class="vital-label">Heart Rate</div>
                                    <div class="vital-num ${hrAlert}">${p.heart_rate} <span class="vital-unit">bpm</span></div>
                                </div>
                                <div class="vital-box">
                                    <div class="vital-label">SpO2</div>
                                    <div class="vital-num ${spo2Alert}">${p.spo2} <span class="vital-unit">%</span></div>
                                </div>
                                <div class="vital-box">
                                    <div class="vital-label">Temperature</div>
                                    <div class="vital-num">${p.temperature} <span class="vital-unit">°C</span></div>
                                </div>
                                <div class="vital-box">
                                    <div class="vital-label">Systolic BP</div>
                                    <div class="vital-num">${p.systolic_bp} <span class="vital-unit">mmHg</span></div>
                                </div>
                            </div>
                            <div class="card-bottom">
                                <span>MEWS Score</span>
                                <span class="score-val" style="color: ${statusClass === 'critical' ? 'var(--danger)' : (statusClass === 'high' ? 'var(--warning)' : 'var(--text)')};">
                                    ${p.sepsis_score} / 10
                                </span>
                            </div>
                        </div>
                    `;
                });
                container.innerHTML = html;
            } catch (err) {
                console.error(err);
            }
        }

        setInterval(fetchVitals, 2000);
        fetchVitals();
    </script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# REST API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """Halaman Dashboard Klinisi."""
    patients = get_historical_ehr()
    return render_template_string(HTML_TEMPLATE, patients=patients)

@app.route("/api/vitals")
def api_vitals():
    """Endpoint REST API: Tanda vital ICU live dari Speed Layer."""
    return jsonify(get_latest_icu_vitals())

@app.route("/api/patients")
def api_patients():
    """Endpoint REST API: Rekam medis pasien dari Batch Layer."""
    return jsonify(get_historical_ehr())

@app.route("/api/alerts")
def api_alerts():
    """Endpoint REST API: Pasien dengan kondisi darurat sepsis."""
    vitals = get_latest_icu_vitals()
    alerts = [p for p in vitals if p.get("alert_level") in ["CRITICAL", "HIGH"]]
    return jsonify(alerts)

if __name__ == "__main__":
    print("=" * 70)
    print(" CLINICIAN MONITORING PLATFORM & REST API")
    print("=" * 70)
    print(" URL: http://localhost:5000")
    print(" Endpoints:")
    print("   GET /api/vitals")
    print("   GET /api/alerts")
    print("   GET /api/patients")
    print("=" * 70)
    app.run(host="0.0.0.0", port=5000, debug=False)
