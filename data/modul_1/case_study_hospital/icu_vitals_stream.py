"""
================================================================================
SPEED LAYER - ICU Bed Monitor Kafka Producer
================================================================================
Mensimulasikan monitor ICU yang mengirim pembacaan sensor vital (Detak Jantung,
Saturasi Oksigen SpO2, Suhu, Tekanan Darah Sistolik, Laju Pernapasan) secara real-time
ke Apache Kafka Topic: 'icu-vitals'.
================================================================================
"""

import json
import time
import random
import os
import sys
from datetime import datetime

# Konfigurasi Kafka
KAFKA_TOPIC = "icu-vitals"
# Jika dijalankan di dalam container Docker: 'kafka:9092', jika dari Windows host: 'localhost:29092'
KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

PATIENTS = [
    {"bed": "BED-01", "id": "P-101", "name": "Ahmad Rizki", "base_hr": 115, "base_spo2": 88, "condition": "Suspect Sepsis"},
    {"bed": "BED-02", "id": "P-102", "name": "Dewi Lestari", "base_hr": 78, "base_spo2": 98, "condition": "Post-Op Recovery"},
    {"bed": "BED-03", "id": "P-103", "name": "Budi Santoso", "base_hr": 128, "base_spo2": 82, "condition": "Septic Shock Alert"},
    {"bed": "BED-04", "id": "P-104", "name": "Siti Nurhaliza", "base_hr": 96, "base_spo2": 93, "condition": "Pneumonia Acute"},
]

def main():
    print("=" * 70)
    print(" [SPEED LAYER] SIMULATOR SENSOR MONITOR ICU -> KAFKA PRODUCER")
    print(f" Target Kafka Broker: {KAFKA_BROKER}")
    print(f" Target Topic       : {KAFKA_TOPIC}")
    print("=" * 70)

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[!] Modul kafka-python belum terpasang. Menginstall kafka-python...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python"])
        from kafka import KafkaProducer

    # Inisialisasi Kafka Producer
    print(f"[+] Menghubungkan ke Kafka ({KAFKA_BROKER})...")
    producer = None
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            print("[✓] Berhasil terhubung ke Kafka!")
            break
        except Exception as e:
            print(f"    Percobaan {attempt}/10: Belum bisa terhubung ke Kafka ({e}), tunggu 3 detik...")
            time.sleep(3)

    if not producer:
        print("[✗] Gagal terhubung ke Kafka. Pastikan container Kafka sudah running!")
        return

    seq = 0
    try:
        while True:
            seq += 1
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for p in PATIENTS:
                hr = max(40, int(p["base_hr"] + random.uniform(-6, 8)))
                spo2 = min(100, max(60, int(p["base_spo2"] + random.uniform(-3, 3))))
                temp = round(37.0 + random.uniform(-0.5, 2.5), 1)
                resp_rate = max(10, int(20 + random.uniform(-3, 8)))
                sbp = max(60, int(90 + random.uniform(-15, 25)))

                payload = {
                    "timestamp": now_str,
                    "bed_id": p["bed"],
                    "patient_id": p["id"],
                    "patient_name": p["name"],
                    "heart_rate": hr,
                    "spo2": spo2,
                    "temperature": temp,
                    "respiratory_rate": resp_rate,
                    "systolic_bp": sbp,
                    "condition": p["condition"]
                }

                # Kirim ke Kafka
                producer.send(KAFKA_TOPIC, key=p["bed"], value=payload)

            producer.flush()
            print(f"[✓] Detik ke-{seq}: Mengirim 4 data tanda vital pasien ke Kafka topic '{KAFKA_TOPIC}'")
            time.sleep(3)

    except KeyboardInterrupt:
        print("\n[!] Producer dihentikan.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
