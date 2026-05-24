import os
import re

# 1. Update railway_pipeline.py
with open('railway_pipeline.py', 'r', encoding='utf-8') as f:
    pipe_content = f.read()

new_funcs = '''
try:
    import docx
except ImportError:
    docx = None

def process_csv(file_path, db_path):
    log.info(f"Processing CSV: {file_path.name}")
    variant = "Chord" if "Chord" in file_path.name else "Profile" if "Profile" in file_path.name else "Unknown"
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        try:
            for chunk in pd.read_csv(file_path, sep=';', chunksize=100000, encoding='latin1', low_memory=False):
                chunk.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_').replace(':', '') for c in chunk.columns]
                chunk['data_variant'] = variant
                chunk['source_file'] = file_path.name
                chunk.to_sql('csv_waveform_data', conn, if_exists='append', index=False)
            log.info(f"  -> Inserted CSV data for {file_path.name}")
        except Exception as e:
            log.error(f"  Failed CSV {file_path.name}: {e}")

def process_docx(file_path, db_path):
    log.info(f"Processing DOCX: {file_path.name}")
    if not docx:
        log.error("python-docx not installed.")
        return

    import re, sqlite3
    m_date = re.search(r"(\d{4}-\d{2}-\d{2})", file_path.name)
    m_trc = re.search(r"(RT\d+)", file_path.name)
    m_sec = re.search(r"Central_([A-Za-z0-9\-]+)", file_path.name)

    run_date = m_date.group(1) if m_date else ""
    trc_no = m_trc.group(1) if m_trc else ""
    section = m_sec.group(1) if m_sec else ""

    try:
        doc = docx.Document(file_path)
    except Exception as e:
        log.error(f"  Failed to read DOCX: {e}")
        return

    records = []
    current_loc = {}
    report_type = ""

    def clean_text(t): return str(t).replace('\x00', '-').strip()

    for p in doc.paragraphs:
        text = clean_text(p.text)
        if not text: continue

        if text in ["Detail Ballast Report", "Detail Fastening Report", "Rail Defect Detail Report"]:
            report_type = text
        elif text.startswith("LOC:"):
            m_line = re.search(r"Line\s*[-:=]\s*([a-zA-Z0-9]+)", text, re.I)
            m_km = re.search(r"KM\s*[-:=]?\s*(\d+)", text, re.I)
            m_meter = re.search(r"Meter\s*[-:=]?\s*([\d\.]+)", text, re.I)
            m_rail = re.search(r"Rail\s*[-:=]?\s*([A-Za-z]+)", text, re.I)

            current_loc = {
                "line": m_line.group(1) if m_line else "",
                "km": int(m_km.group(1)) if m_km else 0,
                "meter": float(m_meter.group(1)) if m_meter else 0.0,
                "rail_side": m_rail.group(1) if m_rail else ""
            }
        elif current_loc and text != "Details:":
            records.append({
                "report_type": report_type,
                "line_direction": current_loc.get("line"),
                "location_km": current_loc.get("km"),
                "location_meter": current_loc.get("meter"),
                "rail_side": current_loc.get("rail_side"),
                "defect_type": text,
                "section_name": section,
                "trc_no": trc_no,
                "run_date": run_date,
                "source_file": file_path.name
            })
            current_loc = {}

    if records:
        with sqlite3.connect(db_path) as conn:
            pd.DataFrame(records).to_sql("docx_defect_records", conn, if_exists='append', index=False)
        log.info(f"  -> Inserted {len(records)} rows into docx_defect_records")
    else:
        log.warning(f"  No records found in {file_path.name}")

'''

pipe_content = pipe_content.replace('def process_folder(', new_funcs + '\ndef process_folder(')

old_pf = '''def process_folder(folder_path: str, db_path: str = DB_PATH):
    folder = Path(folder_path)
    xlsx_files = sorted(folder.glob("*.xlsx"))'''

new_pf = '''def process_folder(folder_path: str, db_path: str = DB_PATH):
    folder = Path(folder_path)
    all_files = list(folder.rglob("*"))
    
    csv_files = [f for f in all_files if f.suffix.lower() == '.csv']
    for f in csv_files:
        process_csv(f, db_path)
        
    docx_files = [f for f in all_files if f.suffix.lower() == '.docx' and not f.name.startswith('~')]
    for f in docx_files:
        process_docx(f, db_path)
        
    xlsx_files = [f for f in all_files if f.suffix.lower() == '.xlsx' and not f.name.startswith('~')]'''

pipe_content = pipe_content.replace(old_pf, new_pf)

with open('railway_pipeline.py', 'w', encoding='utf-8') as f:
    f.write(pipe_content)

# 2. Update app.py
with open('app.py', 'r', encoding='utf-8') as f:
    app_content = f.read()

app_content = app_content.replace(
    'value="D:\\\\ENGINEER\\\\IndianRailwaysProject\\\\data"',
    'value="D:\\\\ENGINEER\\\\IndianRailwaysProject - Second_Data\\\\data"'
)

q1_inject = '''
    if _tbl(conn, "docx_defect_records"):
        try:
            df = pd.read_sql("SELECT section_name, line_direction, location_km, defect_type, run_date FROM docx_defect_records", conn)
            df_grp = df.groupby(['section_name', 'line_direction', 'location_km'], as_index=False).size()
            df_grp.rename(columns={'size': 'score'}, inplace=True)
            df_grp = df_grp.sort_values('score', ascending=False)
            top_n = max(1, math.ceil(len(df_grp) * 0.2))
            top = df_grp.head(top_n).copy()
            R["DOCX Defects"] = {
                "total": int(len(df_grp)), "top20_count": int(len(top)),
                "unit": "Total Defects / KM",
                "cols": list(top.columns), "rows": top.to_dict("records"),
            }
        except Exception as ex: print("Q1 docx error:", ex)
        
    conn.close()'''

app_content = app_content.replace('conn.close()\n    return R\n\n\n# ════════════════════════════════════════════════════════════\n#  Q2', q1_inject + '\n    return R\n\n\n# ════════════════════════════════════════════════════════════\n#  Q2')

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(app_content)

print('Success')
