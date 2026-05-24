"""
Quick validation: test the fixed converter on one Urgent Report DOCX.
Checks: column names, trc_no, railway, division, section, loc_km, loc_meter.
"""
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from docx_to_xlsx_converter import convert_urgent_report_docx_to_xlsx, _flatten_loc_header

# ── Test 1: _flatten_loc_header ───────────────────────────────────────────────
header_rows = [
    ['PARAMETER', 'THRESHOLD', 'RECORDED VALUE', 'LOC', ''],
    ['PARAMETER', 'THRESHOLD', 'RECORDED VALUE', 'KM', 'METER'],
]
result = _flatten_loc_header(header_rows)
print(f"\n=== Test 1: _flatten_loc_header ===")
print(f"  Input:    {header_rows}")
print(f"  Output:   {result}")
expected = ['parameter', 'threshold', 'recorded_value', 'loc_km', 'loc_meter']
ok = (result == expected)
print(f"  EXPECTED: {expected}")
print(f"  PASS: {ok}" if ok else f"  FAIL: expected {expected}, got {result}")

# ── Test 2: Convert Urgent Report ─────────────────────────────────────────────
print(f"\n=== Test 2: convert_urgent_report_docx_to_xlsx ===")
docx_path = Path("data/April-26 TRC-9001/RUN NO. D(IGP-BSL) DN LINE/TrackParametersUrgentReport_2026_04_20_20_31_24_361.docx")
output_dir = Path("data")

if not docx_path.exists():
    print(f"  SKIP: {docx_path} not found")
    sys.exit(0)

xlsx = convert_urgent_report_docx_to_xlsx(docx_path, output_dir)
if xlsx is None:
    print("  FAIL: converter returned None")
    sys.exit(1)

print(f"  Output: {xlsx}")

import pandas as pd
df = pd.read_excel(xlsx, sheet_name="ReportData", header=None, dtype=str)
print(f"  Raw XLSX rows: {len(df)}  cols: {df.shape[1]}")

# Locate the EXCEPTION REPORT trigger row
trigger_row = None
for i, row in df.iterrows():
    if "EXCEPTION REPORT" in str(row.iloc[0]):
        trigger_row = i
        break

if trigger_row is None:
    print("  FAIL: No EXCEPTION REPORT trigger row found")
    sys.exit(1)

print(f"  Trigger row index: {trigger_row}")

# Column-name row is 4 rows below trigger (rows: trigger, meta1, meta2, blank, header)
col_row_idx = trigger_row + 4
col_names = df.iloc[col_row_idx].tolist()
print(f"  Column names row ({col_row_idx}): {col_names}")

# Data starts one row after column header
data_df = df.iloc[col_row_idx + 1:].copy()
data_df.columns = col_names
data_df = data_df.dropna(how='all')
print(f"  Data rows: {len(data_df)}")

# Spot-check values
checks = {
    "parameter":      lambda df: not df['parameter'].isna().all() if 'parameter' in df.columns else False,
    "loc_km":         lambda df: not df['loc_km'].isna().all()     if 'loc_km'    in df.columns else False,
    "loc_meter":      lambda df: not df['loc_meter'].isna().all()  if 'loc_meter' in df.columns else False,
    "trc_no=9001":    lambda df: '9001' in df['trc_no'].values      if 'trc_no'   in df.columns else False,
    "railway=Central":lambda df: 'Central' in df['railway'].values  if 'railway'  in df.columns else False,
    "division=BSL":   lambda df: 'BSL' in df['division'].values     if 'division' in df.columns else False,
    "section=IGP-BSL":lambda df: 'IGP-BSL' in df['section'].values  if 'section'  in df.columns else False,
    "run_date set":   lambda df: not df['run_date'].isna().all()    if 'run_date' in df.columns else False,
    "section_spd=130":lambda df: '130' in df['section_spd'].values  if 'section_spd' in df.columns else False,
}

print(f"\n  Column presence: {list(data_df.columns)}")
print(f"\n  Checks:")
all_pass = True
for name, fn in checks.items():
    ok = fn(data_df)
    status = "PASS" if ok else "FAIL"
    print(f"    [{status}] {name}")
    if not ok:
        all_pass = False

if 'trc_no' in data_df.columns:
    print(f"\n  Sample trc_no values: {data_df['trc_no'].unique()[:5]}")
if 'railway' in data_df.columns:
    print(f"  Sample railway:       {data_df['railway'].unique()[:3]}")
if 'division' in data_df.columns:
    print(f"  Sample division:      {data_df['division'].unique()[:3]}")
if 'section' in data_df.columns:
    print(f"  Sample section:       {data_df['section'].unique()[:5]}")
if 'section_spd' in data_df.columns:
    print(f"  Sample section_spd:   {data_df['section_spd'].unique()[:3]}")
if 'loc_km' in data_df.columns:
    print(f"  Sample loc_km:        {data_df['loc_km'].dropna().head(3).tolist()}")
if 'loc_meter' in data_df.columns:
    print(f"  Sample loc_meter:     {data_df['loc_meter'].dropna().head(3).tolist()}")
if 'parameter' in data_df.columns:
    print(f"  Sample parameter:     {data_df['parameter'].dropna().head(3).tolist()}")

print(f"\n{'='*50}")
print(f"OVERALL: {'ALL PASS' if all_pass else 'SOME CHECKS FAILED'}")
