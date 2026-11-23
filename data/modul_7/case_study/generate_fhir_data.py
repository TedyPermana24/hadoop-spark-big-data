import csv
import json
import random
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", ".."))
EHR_CSV = os.path.join(ROOT_DIR, "data", "full_dataset_ehr.csv")
OUT_JSON = os.path.join(BASE_DIR, "fhir_admissions.json")

random.seed(42)

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
departments = ["Emergency Room", "Intensive Care Unit", "Cardiology Ward", "Inpatient Surgery", "Pulmonology Clinic"]
adm_types = ["Emergency", "Urgent", "Elective"]
sources = ["Central Hospital", "Acquired East Memorial", "North Valley Health"]

patient_ids = []
with open(EHR_CSV, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        patient_ids.append(row["patient_id"])
        if i >= 800:
            break

records = []
for i, pid in enumerate(patient_ids):
    day = random.choices(days, weights=[18, 16, 15, 14, 17, 22, 20])[0]
    dept = random.choices(departments, weights=[40, 15, 20, 15, 10])[0]
    adm_type = "Emergency" if dept == "Emergency Room" else random.choice(adm_types)
    readm = 1 if random.random() < (0.28 if dept in ["Emergency Room", "Intensive Care Unit"] else 0.12) else 0
    los = random.randint(1, 14) if dept != "Emergency Room" else random.randint(1, 3)
    triage = random.randint(1, 3) if dept == "Emergency Room" else random.randint(2, 5)

    record = {
        "resourceType": "Encounter",
        "admission_id": f"ADM-{10000 + i}",
        "patient_id": pid,
        "department": dept,
        "admission_type": adm_type,
        "day_of_week": day,
        "length_of_stay_days": los,
        "triage_level": triage,
        "readmission_30d": readm,
        "hospital_network": sources[i % len(sources)]
    }

    # Evolving schema: new acquired hospital systems introduce extra metadata
    if i % 2 == 0:
        record["acquired_clinic_code"] = f"CLN-{random.randint(100, 999)}"
        record["triage_score_mews"] = random.randint(1, 8)
    if i % 3 == 0:
        record["hl7_message_id"] = f"MSG-{random.randint(50000, 99999)}"

    records.append(record)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

print(f"Generated {len(records)} FHIR admission records at: {OUT_JSON}")
