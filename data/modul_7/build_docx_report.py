"""
Builds the complete Word (.docx) report for Modul 7 Big Data
from Laporan_Modul7_BigData.txt and embeds the 12 real generated screenshots.
"""

import os
import docx
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TXT_PATH = os.path.join(BASE_DIR, "Laporan_Modul7_BigData.txt")
DOCX_PATH = os.path.join(BASE_DIR, "Laporan_Modul7_BigData.docx")
IMG_DIR = os.path.join(BASE_DIR, "images")

# Mapping from placeholder pattern in TXT to image filename and proper formal Indonesian caption
IMAGE_MAPPING = {
    "docker container": (
        "gambar1_docker_containers.png",
        "Gambar 1. Status aktif seluruh kontainer Docker (Spark Master, Spark Worker, NameNode, DataNode, YARN) pada kluster terdistribusi"
    ),
    "-mkdir, -put, dan -ls": (
        "gambar2_hdfs_commands.png",
        "Gambar 2. Eksekusi perintah hdfs dfs (-mkdir, -put, dan -ls) pada terminal pengujian HDFS untuk persiapan data masukan Modul 7"
    ),
    "browse directory HDFS /user/student pada Web UI": (
        "gambar3_hdfs_webui_user_student.png",
        "Gambar 3. Antarmuka Web UI NameNode Utilities menampilkan direktori HDFS /user/student beserta berkas employees.csv dan people.json"
    ),
    "diagram alur kerja Spark SQL dan Catalyst Optimizer": (
        "gambar4_spark_sql_catalyst_diagram.png",
        "Gambar 4. Diagram alur kerja pemrosesan kueri Spark SQL dan tahap optimasi Catalyst Optimizer (Analysis, Logical Optimization, Physical Planning, Cost Model, Code Generation)"
    ),
    "kode PySpark spark_sql_basics.py": (
        "gambar5_kode_spark_sql_basics.png",
        "Gambar 5. Kode sumber implementasi PySpark spark_sql_basics.py mencakup pembuatan DataFrame CSV/JSON, operasi Spark SQL, agregasi departemen, dan ekspor Parquet"
    ),
    "output hasil eksekusi DataFrame dan SQL": (
        "gambar6_output_dataframe_sql.png",
        "Gambar 6. Output eksekusi spark-submit spark_sql_basics.py pada terminal menampilkan skema DataFrame, hasil filter usia, agregasi gaji departemen, dan verifikasi ekspor Parquet"
    ),
    "/user/student/output/avg_salary_by_dept": (
        "gambar7_hdfs_webui_parquet_output.png",
        "Gambar 7. Antarmuka Web UI NameNode menampilkan partisi penyimpanan berkas Parquet (_SUCCESS dan part-00000.snappy.parquet) pada direktori /user/student/output/avg_salary_by_dept"
    ),
    "browse directory HDFS /hospital/spark_sql": (
        "gambar8_hdfs_webui_hospital_spark_sql.png",
        "Gambar 8. Antarmuka Web UI NameNode menampilkan direktori HDFS /hospital/spark_sql berisi arsip data klinis fhir_admissions.json dan full_dataset_ehr.csv"
    ),
    "tren admisi IGD per hari": (
        "gambar9_output_admissions_trend_day.png",
        "Gambar 9. Output eksekusi kueri Spark SQL pada terminal: Tren total admisi dan distribusi admisi gawat darurat (Emergency) berdasarkan hari dalam seminggu"
    ),
    "tingkat readmisi per departemen": (
        "gambar10_output_readmission_dept.png",
        "Gambar 10. Output eksekusi kueri Spark SQL pada terminal: Tingkat readmisi 30 hari dan rata-rata lama rawat inap (LOS) per departemen rumah sakit"
    ),
    "cross-format join EHR dan FHIR JSON": (
        "gambar11_output_cross_format_join.png",
        "Gambar 11. Output eksekusi kueri Spark SQL pada terminal: Cross-format unified join antara tabel relasional EHR dan semi-terstruktur FHIR JSON untuk stratifikasi risiko klinis"
    ),
    "Serving Dashboard": (
        "gambar12_serving_dashboard_spark_sql.png",
        "Gambar 12. Antarmuka web terintegrasi Spark SQL & FHIR Analytics pada Serving Dashboard menyajikan ringkasan KPI, arsitektur Catalyst, tren admisi, readmisi, dan cross-format join"
    )
}

def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    tcPr = cell._element.get_or_add_tcPr()
    tcMar = OxmlElement('w:tcMar')
    for m, val in [('top', top), ('bottom', bottom), ('left', left), ('right', right)]:
        node = OxmlElement(f'w:{m}')
        node.set(qn('w:w'), str(val))
        node.set(qn('w:type'), 'dxa')
        tcMar.append(node)
    tcPr.append(tcMar)

def build_docx():
    print(f"Reading text from: {TXT_PATH}")
    with open(TXT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    doc = Document()

    # Set page margins (Normal: 1 inch / 2.54 cm all around)
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)

    # Base styling
    normal_style = doc.styles['Normal']
    normal_style.font.name = 'Times New Roman'
    normal_style.font.size = Pt(12)
    normal_style.font.color.rgb = RGBColor(30, 30, 30)

    lines = content.split('\n')

    # -------------------------------------------------------------
    # 1. COVER / TITLE PAGE
    # -------------------------------------------------------------
    p_course = doc.add_paragraph()
    p_course.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_course = p_course.add_run("TUGAS BESAR MK. IFB-451 BIG DATA\n")
    r_course.bold = True
    r_course.font.size = Pt(14)

    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_title = p_title.add_run("\nLAPORAN PRAKTIKUM MODUL 7\nPEMROSESAN DATA TERDISTRIBUSI DENGAN SPARK SQL DAN DATA ANALYTICS\n\n\n\n")
    r_title.bold = True
    r_title.font.size = Pt(16)
    r_title.font.color.rgb = RGBColor(16, 44, 87)

    p_by = doc.add_paragraph()
    p_by.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_by = p_by.add_run("Disusun Oleh Kelompok :\n\n")
    r_by.bold = True
    r_by.font.size = Pt(12)

    # Table of authors
    table_authors = doc.add_table(rows=0, cols=3)
    table_authors.alignment = WD_TABLE_ALIGNMENT.CENTER
    authors = [
        ("15-2023-057", "Fathurrahman Pratama Putra"),
        ("15-2023-073", "Dindin Imanudin"),
        ("15-2023-124", "Muhammad Fariz Alfaritzi"),
        ("15-2023-131", "Viktorikus Nokia Laksanama F"),
        ("15-2023-151", "Tedy Sukma Permana")
    ]
    for nrp, name in authors:
        row = table_authors.add_row()
        c1, c2, c3 = row.cells
        c1.text = nrp
        c2.text = "-"
        c3.text = name
        c1.paragraphs[0].runs[0].font.name = 'Times New Roman'
        c1.paragraphs[0].runs[0].font.size = Pt(11)
        c2.paragraphs[0].runs[0].font.name = 'Times New Roman'
        c2.paragraphs[0].runs[0].font.size = Pt(11)
        c3.paragraphs[0].runs[0].font.name = 'Times New Roman'
        c3.paragraphs[0].runs[0].font.size = Pt(11)
        c3.paragraphs[0].runs[0].font.bold = True
        set_cell_margins(c1, top=40, bottom=40, left=80, right=80)
        set_cell_margins(c2, top=40, bottom=40, left=40, right=40)
        set_cell_margins(c3, top=40, bottom=40, left=80, right=80)

    p_date = doc.add_paragraph()
    p_date.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_date = p_date.add_run("\n\n\n\n\nTgl. Penugasan : 24 November 2026\nTgl. Penyerahan : 24 November 2026\n\n\n")
    r_date.font.size = Pt(11)

    p_inst = doc.add_paragraph()
    p_inst.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_inst = p_inst.add_run(
        "PROGRAM STUDI INFORMATIKA\n"
        "FAKULTAS TEKNOLOGI INDUSTRI\n"
        "INSTITUT TEKNOLOGI NASIONAL\n"
        "BANDUNG\n"
        "2026"
    )
    r_inst.bold = True
    r_inst.font.size = Pt(13)

    doc.add_page_break()

    # -------------------------------------------------------------
    # 2. BODY CONTENT
    # -------------------------------------------------------------
    start_body = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("1\tPendahuluan") or stripped.startswith("1  Pendahuluan") or stripped == "1 Pendahuluan":
            start_body = True
        
        if not start_body:
            continue

        if not stripped:
            continue

        # Check if heading 1 (e.g. "1\tPendahuluan", "2\tLatihan", "3\tStudi", "4\tLuaran")
        if (stripped.startswith("1\t") or stripped.startswith("2\t") or 
            stripped.startswith("3\t") or stripped.startswith("4\t") or
            stripped.startswith("1 ") or stripped.startswith("2 ") or 
            stripped.startswith("3 ") or stripped.startswith("4 ") or
            (len(stripped) > 2 and stripped[0].isdigit() and stripped[1] in ['\t', ' '])):
            # Make sure it's not a numbered list item like "1. Penjelasan..." or "1) ..."
            if not (len(stripped) > 2 and stripped[1] in ['.', ')']):
                h = doc.add_paragraph()
                h.paragraph_format.space_before = Pt(14)
                h.paragraph_format.space_after = Pt(6)
                h.paragraph_format.keep_with_next = True
                cleaned_title = stripped.replace('\t', '. ')
                if not cleaned_title.startswith(("1. ", "2. ", "3. ", "4. ")):
                    # format e.g. "1 Pendahuluan" -> "1. Pendahuluan"
                    parts = cleaned_title.split(' ', 1)
                    if parts[0].isdigit():
                        cleaned_title = f"{parts[0]}. {parts[1]}"
                r = h.add_run(cleaned_title)
                r.bold = True
                r.font.name = 'Times New Roman'
                r.font.size = Pt(14)
                r.font.color.rgb = RGBColor(16, 44, 87)
                continue

        # Check if heading 2 (e.g. "3.1\tLuaran...", etc.)
        if (stripped.startswith("3.1") or stripped.startswith("3.2") or 
            stripped.startswith("3.3") or stripped.startswith("3.4") or 
            stripped.startswith("3.5")):
            h = doc.add_paragraph()
            h.paragraph_format.space_before = Pt(12)
            h.paragraph_format.space_after = Pt(4)
            h.paragraph_format.keep_with_next = True
            r = h.add_run(stripped.replace('\t', ' '))
            r.bold = True
            r.font.name = 'Times New Roman'
            r.font.size = Pt(12.5)
            r.font.color.rgb = RGBColor(30, 60, 110)
            continue

        # Check if placeholder for images
        if stripped.startswith("[Gambar"):
            matched_img = None
            matched_cap = None
            for key, (img_file, caption) in IMAGE_MAPPING.items():
                if key in stripped:
                    matched_img = img_file
                    matched_cap = caption
                    break
            
            if matched_img:
                img_full_path = os.path.join(IMG_DIR, matched_img)
                if os.path.exists(img_full_path):
                    p_img = doc.add_paragraph()
                    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    p_img.paragraph_format.space_before = Pt(8)
                    p_img.paragraph_format.space_after = Pt(4)
                    run_img = p_img.add_run()
                    run_img.add_picture(img_full_path, width=Inches(6.0))

                    p_cap = doc.add_paragraph()
                    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    p_cap.paragraph_format.space_before = Pt(2)
                    p_cap.paragraph_format.space_after = Pt(12)
                    r_cap = p_cap.add_run(matched_cap)
                    r_cap.font.name = 'Times New Roman'
                    r_cap.font.size = Pt(10)
                    r_cap.font.italic = True
                    r_cap.font.color.rgb = RGBColor(60, 60, 60)
                else:
                    print(f"Warning: Image not found {img_full_path}")
            continue

        # Numbered list items
        if len(stripped) > 2 and stripped[0].isdigit() and stripped[1] in ['.', ')']:
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Inches(0.25)
            p.paragraph_format.space_after = Pt(4)
            p.paragraph_format.line_spacing = 1.15
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            r = p.add_run(stripped.replace('\t', ' '))
            r.font.name = 'Times New Roman'
            r.font.size = Pt(11.5)
            continue

        # Regular prose paragraph
        p = doc.add_paragraph()
        p.paragraph_format.space_after = Pt(6)
        p.paragraph_format.line_spacing = 1.15
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        r = p.add_run(stripped)
        r.font.name = 'Times New Roman'
        r.font.size = Pt(11.5)

    doc.save(DOCX_PATH)
    print(f"\n[SUCCESS] Document saved successfully to: {DOCX_PATH}")

if __name__ == "__main__":
    build_docx()
