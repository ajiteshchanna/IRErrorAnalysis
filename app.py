"""
Railway TRC Analytics — Complete Web Application
=================================================
SETUP:
  pip install flask pandas openpyxl

FOLDER STRUCTURE (all files in the SAME folder):
  your_project/
    app.py
    railway_pipeline.py
    railway.db  (created after first pipeline run)
    data/
      1 SOD exception.xlsx ... (all 8 files)

RUN:
  python app.py
  Open: http://localhost:5000
"""

import sys, os, re, math, queue, sqlite3, threading, subprocess
from pathlib import Path
import pandas as pd
from flask import Flask, Response, request, jsonify

app = Flask(__name__)
_log_queue: queue.Queue = queue.Queue()
_pipeline_running = threading.Event()


# ════════════════════════════════════════════════════════════
#  PIPELINE SCRIPT FINDER
# ════════════════════════════════════════════════════════════
def _find_pipeline(hint=""):
    checks = []
    if hint: checks.append(Path(hint))
    try:    checks.append(Path(__file__).resolve().parent / "railway_pipeline.py")
    except: pass
    checks.append(Path.cwd() / "railway_pipeline.py")
    checks.append(Path(os.path.abspath(".")) / "railway_pipeline.py")
    for p in checks:
        if p.exists(): return p
    return None


def _find_tqi_pipeline(hint=""):
    """Locate docx_tqi_pipeline.py using same search strategy."""
    checks = []
    if hint: checks.append(Path(hint))
    try:    checks.append(Path(__file__).resolve().parent / "docx_tqi_pipeline.py")
    except: pass
    checks.append(Path.cwd() / "docx_tqi_pipeline.py")
    for p in checks:
        if p.exists(): return p
    return None


# ════════════════════════════════════════════════════════════
#  DB HELPERS
# ════════════════════════════════════════════════════════════
def _conn(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    return c

def _tbl(conn, name):
    return bool(conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())

def _safe_int(v):
    try: return int(v)
    except: return 0

def _safe_float(v):
    try: return float(v)
    except: return 0.0


# ════════════════════════════════════════════════════════════
#  DYNAMIC TABLE DISCOVERY (one-file → one-table architecture)
# ════════════════════════════════════════════════════════════
def _get_data_tables(conn) -> list:
    """Return all data table names, preferring the processed_files registry.
    Falls back to sqlite_master scan if the registry doesn't exist yet.
    Always excludes 'processed_files' itself."""
    EXCLUDE = {"processed_files"}
    try:
        if _tbl(conn, "processed_files"):
            rows = conn.execute(
                "SELECT DISTINCT generated_table FROM processed_files WHERE status != 'error'"
            ).fetchall()
            names = [r[0] for r in rows if r[0] not in EXCLUDE]
            if names:
                return names
    except Exception:
        pass
    # Fallback: scan sqlite_master
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return [r[0] for r in rows if r[0] not in EXCLUDE]


def _union_df(conn, required_cols: list, extra_cols: list = None) -> pd.DataFrame:
    """Build a combined DataFrame from all data tables that contain required_cols.
    Silently skips tables missing any required column.
    Optional extra_cols are included when present."""
    all_cols = list(required_cols) + (extra_cols or [])
    tables = _get_data_tables(conn)
    frames = []
    for tbl in tables:
        try:
            existing = [r[1] for r in conn.execute(f"PRAGMA table_info('{tbl}')")]
            if not all(c in existing for c in required_cols):
                continue
            select_cols = [c for c in all_cols if c in existing]
            df = pd.read_sql(f"SELECT {', '.join(select_cols)} FROM '{tbl}'", conn)
            df["_source_table"] = tbl
            frames.append(df)
        except Exception:
            continue
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=all_cols + ["_source_table"])


# ════════════════════════════════════════════════════════════
#  Q1 — Worst 20% Locations
# ════════════════════════════════════════════════════════════
def query_worst_20(db_path, scope="division"):
    conn = _conn(db_path)
    R = {}

    def fmt(label):
        return label.replace("_data","").replace("_"," ").title().replace("And","&")

    def _run_score(df, sc, label, unit):
        df["score"] = pd.to_numeric(df[sc], errors="coerce")
        if scope != "division" and "section_name" in df.columns:
            df = df[df["section_name"].str.contains(scope, case=False, na=False)]
        df = df.dropna(subset=["score"]).sort_values("score", ascending=False)
        top = df.head(max(1, math.ceil(len(df) * 0.2))).fillna("").copy()
        for c in top.select_dtypes(include="number").columns:
            top[c] = top[c].apply(lambda x: round(float(x), 4) if x != "" else "")
        R[label] = {"total": int(len(df)), "top20_count": int(len(top)),
                    "unit": unit, "cols": list(top.columns), "rows": top.to_dict("records")}

    # ── Wear metrics (dynamic: any table with section_name + score col) ──────
    wear_cfg = [
        (["section_name","line_direction","start_location_km","max_vertical_wear_mm","run_date"],
         "max_vertical_wear_mm", "Vertical Wear", "mm (Max Vertical Wear)"),
        (["section_name","line_direction","start_location_km","max_wear_mm","run_date"],
         "max_wear_mm", "Lateral Wear", "mm (Max Lateral Wear)"),
        (["section_name","line_direction","start_location_km","max_value_mm","run_date"],
         "max_value_mm", "Lip Flow", "mm (Max Lip Flow)"),
        (["section_name","line_direction","location_km","value_of_defect_mm","run_date"],
         "value_of_defect_mm", "Rail Defects", "mm (Rail Gap)"),
        (["section_name","line_direction","length_of_track_having_m_defect_length","run_date"],
         "length_of_track_having_m_defect_length", "Ballast & Vegetation", "m (Defect Length)"),
    ]
    for req_cols, sc, label, unit in wear_cfg:
        try:
            df = _union_df(conn, req_cols)
            if not df.empty:
                _run_score(df, sc, label, unit)
        except Exception as ex:
            print(f"Q1 dynamic error [{label}]: {ex}")

    # ── SOD — volume score (dynamic) ─────────────────────────────────────────
    sod_cols = ["section_name","line_direction","location_km","location_meter",
                "size_of_obstacle_mm_l","size_of_obstacle_mm_b","size_of_obstacle_mm_h","run_date"]
    try:
        df = _union_df(conn, ["section_name","size_of_obstacle_mm_l","size_of_obstacle_mm_b","size_of_obstacle_mm_h"])
        if not df.empty:
            for c in ["size_of_obstacle_mm_l","size_of_obstacle_mm_b","size_of_obstacle_mm_h"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df["score"] = df["size_of_obstacle_mm_l"] * df["size_of_obstacle_mm_b"] * df["size_of_obstacle_mm_h"]
            if scope != "division" and "section_name" in df.columns:
                df = df[df["section_name"].str.contains(scope, case=False, na=False)]
            df = df[df["score"] > 0].sort_values("score", ascending=False)
            top = df.head(max(1, math.ceil(len(df) * 0.2))).fillna("").copy()
            for c in top.select_dtypes(include="number").columns:
                top[c] = top[c].apply(lambda x: round(float(x), 4) if x != "" else "")
            R["Sod"] = {"total": int(len(df)), "top20_count": int(len(top)),
                        "unit": "mm³ Volume", "cols": list(top.columns), "rows": top.to_dict("records")}
    except Exception as ex:
        print(f"Q1 SOD dynamic error: {ex}")

    # ── Sleeper & Fittings — sum score (dynamic) ──────────────────────────────
    sleeper_dcols = [
        "nos_of_affected_sleepers_broken_sleeper",
        "nos_of_affected_sleepers_cracked_sleeper_2mm_100mm",
        "nos_of_affected_sleepers_spalling_in_sleeper_1000_mm2",
        "nos_of_affected_sleepers_misalignment_50",
        "nos_of_affected_sleepers_dancing_sleeper",
        "nos_of_affected_sleepers_improper_spacing_20mm",
    ]
    fittings_dcols = [
        "component_defects_left_rail_missing_loose_clip",
        "component_defects_left_rail_missing_bolt_and_nut",
        "component_defects_right_rail_missing_loose_clip",
        "component_defects_right_rail_missing_bolt_and_nut",
    ]
    for label, dcols in [("Sleeper Defects", sleeper_dcols), ("Fittings", fittings_dcols)]:
        try:
            req = ["section_name", "line_direction"] + dcols[:1]  # at least one defect col
            df = _union_df(conn, ["section_name", "line_direction"], dcols)
            ec = [c for c in dcols if c in df.columns]
            if df.empty or not ec:
                continue
            for c in ec:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df["score"] = df[ec].sum(axis=1)
            if scope != "division":
                df = df[df["section_name"].str.contains(scope, case=False, na=False)]
            df = df[df["score"] > 0]
            grp_col = "location_km" if "location_km" in df.columns else df.columns[0]
            df = df.groupby(["section_name","line_direction", grp_col], as_index=False)["score"].sum()
            df = df.sort_values("score", ascending=False)
            top = df.head(max(1, math.ceil(len(df) * 0.2))).fillna("").copy()
            for c in top.select_dtypes(include="number").columns:
                top[c] = top[c].apply(lambda x: round(float(x), 4) if x != "" else "")
            R[label] = {"total": int(len(df)), "top20_count": int(len(top)),
                        "unit": "Total Defects / Block", "cols": list(top.columns), "rows": top.to_dict("records")}
        except Exception as ex:
            print(f"Q1 {label} dynamic error: {ex}")

    # ── DOCX defect records (dynamic: any table with defect_type col) ─────────
    try:
        df = _union_df(conn, ["section_name","line_direction","defect_type"],
                       ["location_km","run_date","km"])
        if not df.empty:
            km_col = "location_km" if "location_km" in df.columns else ("km" if "km" in df.columns else None)
            grp = ["section_name","line_direction"] + ([km_col] if km_col else [])
            df_grp = df.groupby(grp, as_index=False).size().rename(columns={"size":"score"})
            df_grp = df_grp.sort_values("score", ascending=False)
            top = df_grp.head(max(1, math.ceil(len(df_grp) * 0.2))).copy()
            R["Defect Records"] = {"total": int(len(df_grp)), "top20_count": int(len(top)),
                                    "unit": "Total Defects / KM", "cols": list(top.columns),
                                    "rows": top.to_dict("records")}
    except Exception as ex:
        print("Q1 defect_records dynamic error:", ex)

    conn.close()
    return R


# ════════════════════════════════════════════════════════════
#  Q2 — Resource Deployment
# ════════════════════════════════════════════════════════════
def query_resources(db_path):
    conn = _conn(db_path); R = {}

    # Rail wear — dynamic across all tables with wear columns
    wear_rows = []
    for sc, lbl in [("max_vertical_wear_mm","Vertical"),("max_wear_mm","Lateral")]:
        try:
            df = _union_df(conn, ["section_name","line_direction","start_location_km",sc],["sheet_name","run_date"])
            if not df.empty:
                df = df.rename(columns={sc: "wear_mm"})
                df["wear_mm"] = pd.to_numeric(df["wear_mm"], errors="coerce")
                df["type"] = lbl
                wear_rows.append(df)
        except: pass
    if wear_rows:
        df = pd.concat(wear_rows, ignore_index=True).dropna(subset=["wear_mm"])
        thr = float(df["wear_mm"].quantile(0.8))
        df = df[df["wear_mm"] >= thr].sort_values("wear_mm", ascending=False)
        avail = [c for c in ["section_name","line_direction","sheet_name","start_location_km","type","wear_mm","run_date"] if c in df.columns]
        R["Rail Supply"] = {"desc": "Worst 20% KM locations by wear — priority rail replacement.",
            "threshold": round(thr,2), "unit": "mm", "cols": avail, "rows": df[avail].fillna("").to_dict("records")}

    # Sleepers — dynamic
    sleeper_alias = {"nos_of_affected_sleepers_broken_sleeper":"broken",
        "nos_of_affected_sleepers_cracked_sleeper_2mm_100mm":"cracked",
        "nos_of_affected_sleepers_spalling_in_sleeper_1000_mm2":"spalling"}
    try:
        df = _union_df(conn,["section_name","line_direction"],list(sleeper_alias.keys())+["sheet_name","location_km","location_block","run_date"])
        ec = [c for c in sleeper_alias if c in df.columns]
        if not df.empty and ec:
            for c in ec: df[sleeper_alias[c]] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df["total"] = sum(df[sleeper_alias[c]] for c in ec)
            df = df[df["total"] > 0].sort_values("total", ascending=False)
            thr = int(df["total"].quantile(0.8)) if len(df) else 0
            avail = [c for c in ["section_name","line_direction","sheet_name","location_km","location_block","broken","cracked","spalling","total","run_date"] if c in df.columns]
            R["Sleepers Supply"] = {"desc": "Blocks with most unserviceable sleepers.", "threshold": thr,
                "unit": "count", "cols": avail, "rows": df[df["total"]>=thr][avail].fillna("").to_dict("records")}
    except Exception as ex: print(f"Q2 sleeper error: {ex}")

    # Fittings — dynamic
    fit_cols = ["component_defects_left_rail_missing_loose_clip","component_defects_left_rail_missing_bolt_and_nut",
        "component_defects_right_rail_missing_loose_clip","component_defects_right_rail_missing_bolt_and_nut"]
    try:
        df = _union_df(conn,["section_name","line_direction"],fit_cols+["sheet_name","location_km","location_block","run_date"])
        ec = [c for c in fit_cols if c in df.columns]
        if not df.empty and ec:
            aliases = {fit_cols[0]:"lc",fit_cols[1]:"lb",fit_cols[2]:"rc",fit_cols[3]:"rb"}
            for c in ec:
                df[aliases[c]] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df["total"] = sum(df[aliases[c]] for c in ec)
            df = df[df["total"] > 0].sort_values("total", ascending=False)
            thr = int(df["total"].quantile(0.8)) if len(df) else 0
            avail = [c for c in ["section_name","line_direction","sheet_name","location_km","location_block","lc","lb","rc","rb","total","run_date"] if c in df.columns]
            R["Fitting Recoupment"] = {"desc": "Blocks with most missing/loose clips & bolts.",
                "threshold": thr, "unit": "count", "cols": avail, "rows": df[df["total"]>=thr][avail].fillna("").to_dict("records")}
    except Exception as ex: print(f"Q2 fittings error: {ex}")

    # Rail gap — dynamic
    try:
        df = _union_df(conn,["section_name","line_direction","value_of_defect_mm"],["sheet_name","location_km","location_meter","run_date"])
        if not df.empty:
            df["gap_mm"] = pd.to_numeric(df["value_of_defect_mm"], errors="coerce")
            df = df.dropna(subset=["gap_mm"]).sort_values("gap_mm", ascending=False)
            thr = float(df["gap_mm"].quantile(0.8)) if len(df) else 0.0
            avail = [c for c in ["section_name","line_direction","sheet_name","location_km","location_meter","gap_mm","run_date"] if c in df.columns]
            R["Rail Gap Adjustment"] = {"desc": "Largest rail gaps — immediate closure required.",
                "threshold": round(thr,2), "unit": "mm", "cols": avail,
                "rows": df[df["gap_mm"]>=thr][avail].fillna("").to_dict("records")}
    except Exception as ex: print(f"Q2 gap error: {ex}")

    conn.close()
    return R



# ════════════════════════════════════════════════════════════
#  Q3 — False Alert Detection
# ════════════════════════════════════════════════════════════
def query_false_alerts(db_path):
    conn = _conn(db_path); R = {}

    # SOD false alerts — dynamic
    try:
        df = _union_df(conn,["section_name","line_direction","location_km","run_no"],["sheet_name","location_meter","size_of_obstacle_mm_l","run_date"])
        if not df.empty and "run_no" in df.columns:
            df["location_km"] = pd.to_numeric(df["location_km"], errors="coerce")
            df["location_meter"] = pd.to_numeric(df.get("location_meter", 0), errors="coerce")
            df["key"] = df["location_km"].round(3).astype(str)+"_"+df["location_meter"].fillna(0).round(0).astype(str)
            freq = df.groupby("key")["run_no"].nunique().reset_index(name="rc")
            df = df.merge(freq, on="key")
            sc = [c for c in ["section_name","line_direction","sheet_name","location_km","location_meter","size_of_obstacle_mm_l","run_date","rc"] if c in df.columns]
            R["SOD False Alerts"] = {"desc": "Alerts appearing only once are likely false positives.",
                "single": int(df[df["rc"]==1]["key"].nunique()),
                "persistent": int(df[df["rc"]>=2]["key"].nunique()),
                "tabs": {"Likely False (1 run)": {"cols": sc, "rows": df[df["rc"]==1].sort_values("location_km")[sc].fillna("").to_dict("records")},
                         "Persistent (2+ runs)": {"cols": sc, "rows": df[df["rc"]>=2].drop_duplicates("key").sort_values("location_km")[sc].fillna("").to_dict("records")}}}
    except Exception as ex: print(f"Q3 SOD error: {ex}")

    # Vegetation false alerts — dynamic
    try:
        df = _union_df(conn,["section_name","line_direction","defect","run_no"],["sheet_name","location_start_location_km","location_start_location_m","length_of_track_having_m_defect_length","run_date"])
        if not df.empty and "defect" in df.columns:
            df = df[df["defect"].str.lower().str.contains("vegetation", na=False)]
            if not df.empty:
                km_c = "location_start_location_km" if "location_start_location_km" in df.columns else "location_km"
                m_c  = "location_start_location_m" if "location_start_location_m" in df.columns else "location_meter"
                df["km"] = pd.to_numeric(df.get(km_c), errors="coerce")
                df["m"]  = pd.to_numeric(df.get(m_c),  errors="coerce")
                df["key"] = df["km"].round(3).astype(str)+"_"+df["m"].fillna(0).round(0).astype(str)
                freq = df.groupby("key")["run_no"].nunique().reset_index(name="rc")
                df = df.merge(freq, on="key")
                sc = [c for c in ["section_name","line_direction","sheet_name","km","m","length_of_track_having_m_defect_length","run_date","rc"] if c in df.columns]
                R["Vegetation False Alerts"] = {"desc": "Vegetation alerts appearing once are likely cleared or false.",
                    "single": int(df[df["rc"]==1]["key"].nunique()),
                    "persistent": int(df[df["rc"]>=2]["key"].nunique()),
                    "tabs": {"Likely False (1 run)": {"cols": sc, "rows": df[df["rc"]==1].sort_values("km")[sc].fillna("").to_dict("records")},
                             "Persistent (2+ runs)": {"cols": sc, "rows": df[df["rc"]>=2].drop_duplicates("key").sort_values("km")[sc].fillna("").to_dict("records")}}}
    except Exception as ex: print(f"Q3 vegetation error: {ex}")

    conn.close()
    return R



# ════════════════════════════════════════════════════════════
#  Q4 — Consecutive Defects
# ════════════════════════════════════════════════════════════
def query_consecutive(db_path, n=2):
    conn = _conn(db_path)
    sleeper_dcols = ["nos_of_affected_sleepers_broken_sleeper",
        "nos_of_affected_sleepers_cracked_sleeper_2mm_100mm",
        "nos_of_affected_sleepers_spalling_in_sleeper_1000_mm2",
        "nos_of_affected_sleepers_misalignment_50",
        "nos_of_affected_sleepers_dancing_sleeper",
        "nos_of_affected_sleepers_improper_spacing_20mm"]
    try:
        df = _union_df(conn,["section_name","line_direction","location_km","run_date","run_no"],sleeper_dcols)
    except Exception as ex:
        conn.close()
        return {"error": str(ex)}
    conn.close()
    if df.empty:
        return {"error": "No sleeper defect data found across any table"}
    # rename defect cols to single letters for calculation
    aliases = dict(zip(sleeper_dcols, list("bcsmdi")))
    for orig, alias in aliases.items():
        if orig in df.columns:
            df[alias] = df[orig]

    for col in list("bcsmdi"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["total"] = df[list("bcsmdi")].sum(axis=1)
    df["location_km"] = pd.to_numeric(df["location_km"], errors="coerce")
    df = df.dropna(subset=["location_km"])

    seqs = []
    for (sec, ld, rno, rd), grp in df[df["total"] > 0].groupby(
        ["section_name","line_direction","run_no","run_date"]
    ):
        kms = sorted(grp["location_km"].unique())
        if not kms: continue
        ss, sl = kms[0], 1
        for i in range(1, len(kms)):
            if kms[i] - kms[i-1] <= 1:
                sl += 1
            else:
                if sl >= n:
                    td = int(grp[grp["location_km"].between(ss, kms[i-1])]["total"].sum())
                    seqs.append({"section_name":sec,"line_direction":ld,"run_date":rd,
                                 "from_km":float(ss),"to_km":float(kms[i-1]),
                                 "consecutive_kms":sl,"total_defects":td})
                ss, sl = kms[i], 1
        if sl >= n:
            td = int(grp[grp["location_km"].between(ss, kms[-1])]["total"].sum())
            seqs.append({"section_name":sec,"line_direction":ld,"run_date":rd,
                         "from_km":float(ss),"to_km":float(kms[-1]),
                         "consecutive_kms":sl,"total_defects":td})

    seqs.sort(key=lambda x: (-x["consecutive_kms"], -x["total_defects"]))
    return {
        "desc": f"Sequences of {n}+ consecutive KMs with unserviceable sleepers.",
        "min_consecutive": n,
        "total_sequences": len(seqs),
        "cols": ["section_name","line_direction","from_km","to_km","consecutive_kms","total_defects","run_date"],
        "rows": seqs,
    }


# ════════════════════════════════════════════════════════════
#  Q5 — Repeated in 3+ TRC Runs
# ════════════════════════════════════════════════════════════
def query_repeated(db_path):
    conn = _conn(db_path)
    out = {}
    # Dynamic: discover all tables with section_name + run_no + a KM column
    for km_col in ["start_location_km", "location_km"]:
        try:
            df = _union_df(conn, ["section_name","line_direction",km_col,"run_date","run_no"])
            if df.empty: continue
            df["km"] = pd.to_numeric(df[km_col], errors="coerce")
            df = df.dropna(subset=["km"])
            # Group by source table + KM
            for tbl_name, grp_tbl in df.groupby("_source_table"):
                grp = grp_tbl.groupby(["section_name","line_direction","km"])["run_no"].nunique().reset_index()
                grp.columns = ["section_name","line_direction","km","run_count"]
                pers = grp[grp["run_count"] >= 3].sort_values("run_count", ascending=False)
                if not pers.empty:
                    dates = grp_tbl.groupby(["section_name","line_direction","km"])["run_date"]\
                        .apply(lambda x: ", ".join(sorted(set(str(v) for v in x)))).reset_index()
                    dates.columns = ["section_name","line_direction","km","run_dates"]
                    pers = pers.merge(dates, on=["section_name","line_direction","km"])
                    lbl = tbl_name.replace("_"," ").title()
                    out[lbl] = {"count": int(len(pers)),
                        "cols": ["section_name","line_direction","km","run_count","run_dates"],
                        "rows": pers.fillna("").to_dict("records")}
        except Exception as ex: print(f"Q5 dynamic error [{km_col}]: {ex}")
    conn.close()
    return {"desc": "Locations deficient in 3+ separate TRC runs — chronic problem zones.", "tables": out}


# ════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ════════════════════════════════════════════════════════════
@app.route("/")
def index(): return PAGE

@app.route("/api/script_status")
def script_status():
    s = _find_pipeline()
    return jsonify({"found": s is not None, "path": str(s) if s else None})

@app.route("/run", methods=["POST"])
def run():
    if _pipeline_running.is_set():
        return jsonify({"error": "Pipeline already running"}), 409
    d = request.get_json(force=True)
    folder = d.get("folder", "").strip()
    dbf    = d.get("db", "railway.db").strip()
    script = d.get("script", "").strip()
    if not folder:
        return jsonify({"error": "Data folder path is required"}), 400
    p = _find_pipeline(script)
    if not p:
        searched = []
        try: searched.append(str(Path(__file__).resolve().parent / "railway_pipeline.py"))
        except: pass
        searched.append(str(Path.cwd() / "railway_pipeline.py"))
        return jsonify({"error":
            "railway_pipeline.py not found.\n\nSearched in:\n" +
            "\n".join(f"• {x}" for x in searched) +
            "\n\nFix: put railway_pipeline.py in the same folder as app.py."}), 404
    while not _log_queue.empty():
        _log_queue.get_nowait()
    def _go():
        _pipeline_running.set()
        try:
            proc = subprocess.Popen(
                [sys.executable, str(p), folder, dbf],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(p.parent))
            for line in proc.stdout: _log_queue.put(line.rstrip())
            proc.wait()
            _log_queue.put("__DONE__" if proc.returncode == 0 else "__ERROR__")
        except Exception as ex:
            _log_queue.put(f"2000-01-01 00:00:00,000 [ERROR] {ex}")
            _log_queue.put("__ERROR__")
        finally: _pipeline_running.clear()
    threading.Thread(target=_go, daemon=True).start()
    return jsonify({"status": "started", "script": str(p)})

@app.route("/stream")
def stream():
    def gen():
        while True:
            try:
                line = _log_queue.get(timeout=30)
                yield f"data: {line}\n\n"
                if line in ("__DONE__", "__ERROR__"): break
            except queue.Empty: yield "data: \n\n"
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/worst20")
def a_worst20():
    try: return jsonify(query_worst_20(request.args.get("db","railway.db"), request.args.get("scope","division")))
    except Exception as ex: print(f"worst20 error: {ex}"); return jsonify({"error": str(ex)}), 500

@app.route("/api/resources")
def a_res():
    try: return jsonify(query_resources(request.args.get("db","railway.db")))
    except Exception as ex: return jsonify({"error": str(ex)}), 500

@app.route("/api/false_alerts")
def a_fa():
    try: return jsonify(query_false_alerts(request.args.get("db","railway.db")))
    except Exception as ex: return jsonify({"error": str(ex)}), 500

@app.route("/api/consecutive")
def a_con():
    try: return jsonify(query_consecutive(request.args.get("db","railway.db"), int(request.args.get("n",2))))
    except Exception as ex: return jsonify({"error": str(ex)}), 500

@app.route("/api/repeated")
def a_rep():
    try: return jsonify(query_repeated(request.args.get("db","railway.db")))
    except Exception as ex: return jsonify({"error": str(ex)}), 500

@app.route("/api/tables")
def a_tbls():
    dbf = request.args.get("db", "railway.db")
    if not Path(dbf).exists(): return jsonify([])
    try:
        conn = sqlite3.connect(dbf)
        # Enrich with processed_files metadata when available
        registry = {}
        if bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='processed_files'"
        ).fetchone()):
            for r in conn.execute(
                "SELECT generated_table, file_type, processed_date, record_count, source_filename, status "
                "FROM processed_files"
            ).fetchall():
                registry[r[0]] = {
                    "file_type": r[1], "processed_date": r[2],
                    "record_count": r[3], "source_filename": r[4], "status": r[5]
                }
        out = []
        for (n,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM '{n}'").fetchone()[0]
            except Exception:
                cnt = 0
            entry = {"table": n, "rows": cnt}
            if n in registry:
                entry.update(registry[n])
            out.append(entry)
        conn.close()
        return jsonify(out)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@app.route("/api/processed_files")
def a_processed_files():
    """Return the full processed_files registry."""
    dbf = request.args.get("db", "railway.db")
    if not Path(dbf).exists(): return jsonify([])
    try:
        conn = sqlite3.connect(dbf)
        if not bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='processed_files'"
        ).fetchone()):
            conn.close()
            return jsonify([])
        rows = conn.execute(
            "SELECT source_filename, generated_table, file_type, "
            "processed_date, last_modified, record_count, status "
            "FROM processed_files ORDER BY processed_date DESC"
        ).fetchall()
        conn.close()
        keys = ["source_filename", "generated_table", "file_type",
                "processed_date", "last_modified", "record_count", "status"]
        return jsonify([dict(zip(keys, r)) for r in rows])
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@app.route("/api/search")
def a_search():
    """Cross-table keyword search across all ingested data tables."""
    dbf = request.args.get("db", "railway.db")
    q   = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Provide ?q=keyword"}), 400
    if not Path(dbf).exists():
        return jsonify({"error": "Database not found"}), 404
    try:
        conn = sqlite3.connect(dbf)
        tables = _get_data_tables(conn)
        results = []
        for tbl in tables:
            try:
                cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{tbl}')")]
                text_cols = [c for c in cols if c not in (
                    "id", "record_count", "last_modified"
                )]
                if not text_cols:
                    continue
                where = " OR ".join(
                    f"CAST(\"{ c}\" AS TEXT) LIKE ?" for c in text_cols
                )
                params = [f"%{q}%"] * len(text_cols)
                rows = conn.execute(
                    f"SELECT * FROM '{tbl}' WHERE {where} LIMIT 50",
                    params
                ).fetchall()
                if rows:
                    results.append({
                        "table": tbl,
                        "cols": cols,
                        "rows": [list(r) for r in rows],
                        "count": len(rows),
                    })
                if len(results) >= 20:  # cap total tables returned
                    break
            except Exception:
                continue
        conn.close()
        return jsonify({"query": q, "tables_matched": len(results), "results": results})
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


# ════════════════════════════════════════════════════════════
#  TQI PIPELINE RUNNER ROUTE
# ════════════════════════════════════════════════════════════
@app.route("/run/tqi", methods=["POST"])
def run_tqi():
    """Trigger the DOCX TQI pipeline (docx_tqi_pipeline.py) for a given folder."""
    if _pipeline_running.is_set():
        return jsonify({"error": "A pipeline is already running"}), 409
    d      = request.get_json(force=True)
    folder = d.get("folder", "").strip()
    dbf    = d.get("db", "railway.db").strip()
    force  = bool(d.get("force", False))
    if not folder:
        return jsonify({"error": "Data folder path is required"}), 400
    p = _find_tqi_pipeline()
    if not p:
        return jsonify({"error":
            "docx_tqi_pipeline.py not found.\nPut it in the same folder as app.py."}), 404
    while not _log_queue.empty():
        _log_queue.get_nowait()
    def _go():
        _pipeline_running.set()
        try:
            cmd = [sys.executable, str(p), folder, dbf]
            if force:
                cmd.append("--force")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(p.parent))
            for line in proc.stdout: _log_queue.put(line.rstrip())
            proc.wait()
            _log_queue.put("__DONE__" if proc.returncode == 0 else "__ERROR__")
        except Exception as ex:
            _log_queue.put(f"2000-01-01 00:00:00,000 [ERROR] {ex}")
            _log_queue.put("__ERROR__")
        finally: _pipeline_running.clear()
    threading.Thread(target=_go, daemon=True).start()
    return jsonify({"status": "started", "script": str(p)})


# ════════════════════════════════════════════════════════════
#  Q_TQI — TQI Analytics (docx_tqi_pipeline data)
# ════════════════════════════════════════════════════════════
def query_tqi_analytics(db_path: str) -> dict:
    """
    Analytics powered by the v_tqi_* views created by docx_tqi_pipeline.
    Returns a dict with sections: worst_tqi, run_comparison, chronic_zones, by_run.
    """
    conn = _conn(db_path)
    R    = {}

    def _view(name):
        return bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?", (name,)
        ).fetchone())

    def _cols(view_or_tbl):
        try:
            return [r[1] for r in conn.execute(f'PRAGMA table_info("{view_or_tbl}")')]
        except Exception:
            return []

    # ── Worst TQI sections ───────────────────────────────────────────────
    if _view("v_tqi_all"):
        all_cols = _cols("v_tqi_all")
        for col, label in [("tqi_s", "Worst Short-Chord (TQI-S)"),
                           ("tqi_l", "Worst Long-Chord (TQI-L)"),
                           ("tqi_c", "Worst Composite (TQI-C)")]:
            if col not in all_cols:
                continue
            try:
                sel = ", ".join(
                    c for c in ["km_from","km_to",col,"section","direction",
                                "trc_no","run_no","run_date","source_file"]
                    if c in all_cols
                )
                df = pd.read_sql(
                    f"SELECT {sel} FROM v_tqi_all "
                    f"WHERE {col} IS NOT NULL AND {col} != '' "
                    f"ORDER BY CAST({col} AS REAL) ASC LIMIT 50",
                    conn
                ).fillna("")
                for c in df.select_dtypes(include="number").columns:
                    df[c] = df[c].apply(lambda x: round(float(x), 4) if x != "" else "")
                R[label] = {"cols": list(df.columns), "rows": df.to_dict("records"),
                             "total": len(df), "unit": f"{col.upper()} index (lower = worse)"}
            except Exception as ex:
                print(f"TQI analytics [{label}] error: {ex}")

    # ── Run comparison ───────────────────────────────────────────────────
    if _view("v_tqi_by_run"):
        try:
            df = pd.read_sql(
                "SELECT * FROM v_tqi_by_run ORDER BY run_date DESC", conn
            ).fillna("")
            for c in df.select_dtypes(include="number").columns:
                df[c] = df[c].apply(lambda x: round(float(x), 4) if x != "" else "")
            R["Run Comparison"] = {"cols": list(df.columns), "rows": df.to_dict("records")}
        except Exception as ex:
            print(f"TQI run_comparison error: {ex}")

    # ── Chronic zones ─────────────────────────────────────────────────────
    if _view("v_tqi_by_km"):
        try:
            df = pd.read_sql(
                "SELECT * FROM v_tqi_by_km WHERE run_count >= 2 "
                "ORDER BY avg_tqi_s ASC LIMIT 50", conn
            ).fillna("")
            for c in df.select_dtypes(include="number").columns:
                df[c] = df[c].apply(lambda x: round(float(x), 4) if x != "" else "")
            R["Chronic Zones (2+ runs)"] = {
                "cols": list(df.columns), "rows": df.to_dict("records"),
                "total": len(df)
            }
        except Exception as ex:
            print(f"TQI chronic_zones error: {ex}")

    # ── Summary counts ────────────────────────────────────────────────────
    try:
        tqi_tables = conn.execute(
            "SELECT generated_table, record_count, source_filename, processed_date "
            "FROM processed_files WHERE file_type='docx_tqi' AND status='ok' "
            "ORDER BY processed_date DESC"
        ).fetchall()
        R["_meta"] = {
            "tqi_tables_count": len(tqi_tables),
            "tables": [
                {"table": r[0], "rows": r[1], "source": r[2], "date": r[3]}
                for r in tqi_tables
            ]
        }
    except Exception:
        R["_meta"] = {"tqi_tables_count": 0, "tables": []}

    conn.close()
    return R


@app.route("/api/tqi")
def a_tqi():
    """TQI analytics endpoint — powered by docx_tqi_pipeline data."""
    try:
        return jsonify(query_tqi_analytics(request.args.get("db", "railway.db")))
    except Exception as ex:
        print(f"TQI analytics error: {ex}")
        return jsonify({"error": str(ex)}), 500


# ════════════════════════════════════════════════════════════
#  Q_REPORTS — Report Analytics (rpt_* tables from docx_tqi_pipeline fallback)
# ════════════════════════════════════════════════════════════
def query_report_analytics(db_path: str) -> dict:
    """
    Analytics for rpt_* tables extracted by the fallback path in docx_tqi_pipeline.
    Queries v_rpt_all and v_rpt_by_file views (created by run_analytics_views).
    Falls back to direct rpt_* table scanning when views are absent.

    Returns a dict with sections:
      _meta          — table inventory (count, rows, source files)
      by_file        — rows from v_rpt_by_file (per-document summary)
      tables         — per-table preview (first 200 rows each)
    """
    conn = _conn(db_path)
    R: dict = {}

    def _view(name):
        return bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?", (name,)
        ).fetchone())

    def _rpt_tables():
        """Return all rpt_* table names that exist in the DB."""
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'rpt_%'"
        ).fetchall()
        return [r[0] for r in rows]

    rpt_tbls = _rpt_tables()

    # — Meta / inventory —
    meta_rows = []
    for tbl in rpt_tbls:
        try:
            cnt  = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")')]
            # Grab source_file and report_type from first row if they exist
            src = rtype = ""
            if "source_file" in cols:
                row = conn.execute(
                    f'SELECT source_file FROM "{tbl}" LIMIT 1'
                ).fetchone()
                src = row[0] if row else ""
            if "report_type" in cols:
                row = conn.execute(
                    f'SELECT report_type FROM "{tbl}" LIMIT 1'
                ).fetchone()
                rtype = row[0] if row else ""
            meta_rows.append({
                "table": tbl,
                "rows": cnt,
                "columns": len(cols),
                "source_file": src,
                "report_type": rtype,
            })
        except Exception:
            pass

    R["_meta"] = {
        "rpt_tables_count": len(rpt_tbls),
        "total_rows": sum(m["rows"] for m in meta_rows),
        "tables": meta_rows,
    }

    # — By-file summary (from v_rpt_by_file view or fallback) —
    if _view("v_rpt_by_file"):
        try:
            df = pd.read_sql("SELECT * FROM v_rpt_by_file ORDER BY source_file", conn)
            df = df.fillna("")
            R["by_file"] = {
                "cols": list(df.columns),
                "rows": df.to_dict("records"),
            }
        except Exception as ex:
            print(f"report_analytics by_file error: {ex}")
    elif rpt_tbls:
        # Fallback: aggregate from direct table queries
        agg_rows = []
        for tbl in rpt_tbls:
            try:
                cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")')]
                sel = ", ".join(
                    c for c in ["source_file", "report_type", "section",
                                "direction", "trc_no", "run_no", "run_date"]
                    if c in cols
                )
                cnt = conn.execute(
                    f'SELECT COUNT(*) FROM "{tbl}"'
                ).fetchone()[0]
                if sel:
                    row = conn.execute(
                        f'SELECT {sel} FROM "{tbl}" LIMIT 1'
                    ).fetchone()
                    row_dict = dict(zip(sel.split(", "), row)) if row else {}
                else:
                    row_dict = {}
                row_dict["table"] = tbl
                row_dict["row_count"] = cnt
                agg_rows.append(row_dict)
            except Exception:
                pass
        if agg_rows:
            all_keys = list(dict.fromkeys(k for r in agg_rows for k in r.keys()))
            R["by_file"] = {"cols": all_keys, "rows": agg_rows}

    # — Per-table previews (first 200 rows each) —
    table_previews = {}
    for tbl in rpt_tbls:
        try:
            df = pd.read_sql(f'SELECT * FROM "{tbl}" LIMIT 200', conn)
            df = df.fillna("")
            for c in df.select_dtypes(include="number").columns:
                df[c] = df[c].apply(lambda x: round(float(x), 4) if x != "" else "")
            table_previews[tbl] = {
                "cols": list(df.columns),
                "rows": df.to_dict("records"),
                "total_rows": conn.execute(
                    f'SELECT COUNT(*) FROM "{tbl}"'
                ).fetchone()[0],
            }
        except Exception as ex:
            print(f"report_analytics table preview [{tbl}] error: {ex}")

    R["tables"] = table_previews

    conn.close()
    return R


@app.route("/api/reports")
def a_reports():
    """Track Reports analytics endpoint — powered by rpt_* tables from docx_tqi_pipeline."""
    try:
        return jsonify(query_report_analytics(request.args.get("db", "railway.db")))
    except Exception as ex:
        print(f"Reports analytics error: {ex}")
        return jsonify({"error": str(ex)}), 500



@app.route("/api/tqi_script_status")
def tqi_script_status():
    s = _find_tqi_pipeline()
    return jsonify({"found": s is not None, "path": str(s) if s else None})


# ════════════════════════════════════════════════════════════
#  HTML PAGE
# ════════════════════════════════════════════════════════════
PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>TRC Analytics — BSL Division</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
:root{
  --sidebar-bg:#0d1117;--sidebar-bd:#21262d;--sidebar-hover:#161b22;
  --bg:#f6f8fa;--surface:#ffffff;--surface2:#f6f8fa;
  --border:#d0d7de;--border2:#b1bac4;
  --ink:#1f2328;--body:#36393f;--mid:#656d76;--muted:#9198a1;
  --blue:#0969da;--blue-l:#ddf4ff;--green:#1a7f37;--green-l:#dcffe4;
  --amber:#9a6700;--amber-l:#fff8c5;--red:#cf222e;--red-l:#ffebe9;
  --purple:#8250df;--purple-l:#fbefff;
  --font:'Plus Jakarta Sans',sans-serif;--mono:'JetBrains Mono',monospace;
  --r4:4px;--r6:6px;--r8:8px;--r12:12px;
  --s1:0 1px 2px rgba(0,0,0,.06);
  --s2:0 2px 6px rgba(0,0,0,.08),0 0 0 1px rgba(0,0,0,.04);
  --s3:0 4px 16px rgba(0,0,0,.10),0 0 0 1px rgba(0,0,0,.04);
  --s4:0 8px 32px rgba(0,0,0,.14),0 0 0 1px rgba(0,0,0,.04);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;font-family:var(--font);font-size:14px;line-height:1.5;
  background:var(--bg);color:var(--ink);-webkit-font-smoothing:antialiased}
.app{display:grid;grid-template-columns:264px 1fr;height:100vh;overflow:hidden}

/* SIDEBAR */
.sb{background:var(--sidebar-bg);display:flex;flex-direction:column;
  overflow:hidden;border-right:1px solid #30363d}
.sb-brand{padding:20px 16px 16px;border-bottom:1px solid #21262d}
.sb-brand-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.sb-badge{width:34px;height:34px;background:linear-gradient(135deg,#1f6feb,#0d419d);
  border-radius:var(--r8);display:flex;align-items:center;justify-content:center;
  box-shadow:0 2px 8px rgba(31,111,235,.4);flex-shrink:0}
.sb-badge svg{width:18px;height:18px;fill:#fff}
.sb-name{font-size:14px;font-weight:700;color:#e6edf3;letter-spacing:-.01em}
.sb-tagline{font-size:11px;color:#7d8590;margin-top:1px}
.sb-org{display:inline-flex;align-items:center;gap:6px;font-size:11px;
  color:#7d8590;background:rgba(255,255,255,.05);border:1px solid #30363d;
  border-radius:20px;padding:3px 10px}
.sb-org-dot{width:6px;height:6px;border-radius:50%;background:#3fb950;
  box-shadow:0 0 6px #3fb95088}
.sb-pipe{padding:14px 12px;border-bottom:1px solid #21262d}
.sb-label{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  color:#484f58;margin-bottom:8px;padding:0 4px}
.sb-field{margin-bottom:8px}
.sb-field label{display:flex;align-items:center;gap:5px;font-size:11px;
  font-weight:500;color:#7d8590;margin-bottom:4px}
.sb-field label svg{width:12px;height:12px;fill:#7d8590;flex-shrink:0}
.sb-input{width:100%;font-family:var(--mono);font-size:11px;padding:7px 10px;
  background:#161b22;border:1px solid #30363d;border-radius:var(--r6);
  color:#c9d1d9;outline:none;transition:border-color .15s}
.sb-input:focus{border-color:#1f6feb;box-shadow:0 0 0 3px rgba(31,111,235,.2)}
.sb-script{display:flex;align-items:center;gap:7px;padding:7px 10px;
  border-radius:var(--r6);font-size:11px;margin-bottom:8px;
  border:1px solid #30363d;background:#161b22;color:#7d8590;transition:all .2s}
.sb-script.ok{background:rgba(63,185,80,.08);border-color:rgba(63,185,80,.3);color:#3fb950}
.sb-script.err{background:rgba(248,81,73,.08);border-color:rgba(248,81,73,.3);color:#f85149}
.sb-script-dot{width:7px;height:7px;border-radius:50%;background:currentColor;flex-shrink:0}
.sb-script-txt{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px}
.btn-run{width:100%;padding:10px;
  background:linear-gradient(135deg,#1f6feb,#0d419d);color:#fff;border:none;
  border-radius:var(--r6);font-family:var(--font);font-size:13px;font-weight:600;
  cursor:pointer;display:flex;align-items:center;justify-content:center;gap:7px;
  box-shadow:0 2px 8px rgba(31,111,235,.35);transition:all .18s;letter-spacing:.01em}
.btn-run:hover:not(:disabled){background:linear-gradient(135deg,#388bfd,#1f6feb);
  box-shadow:0 4px 16px rgba(31,111,235,.45);transform:translateY(-1px)}
.btn-run:active:not(:disabled){transform:translateY(0)}
.btn-run:disabled{background:#21262d;box-shadow:none;cursor:not-allowed;color:#484f58}
.btn-run svg{width:13px;height:13px;fill:currentColor}
.prog-wrap{margin-top:8px}
.prog-bg{height:2px;background:#21262d;border-radius:1px;overflow:hidden}
.prog-fill{height:100%;width:0;background:linear-gradient(90deg,#1f6feb,#58a6ff);
  border-radius:1px;transition:width .3s ease}
.prog-lbl{font-size:10px;color:#484f58;margin-top:4px;font-family:var(--mono)}
.sb-nav{flex:1;overflow-y:auto;padding:12px 8px}
.sb-nav::-webkit-scrollbar{width:3px}
.sb-nav::-webkit-scrollbar-thumb{background:#21262d;border-radius:2px}
.sb-nav-label{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  color:#484f58;padding:8px 8px 6px}
.qi{display:flex;align-items:flex-start;gap:10px;width:100%;text-align:left;
  padding:10px;border:1px solid transparent;border-radius:var(--r8);
  background:transparent;cursor:pointer;color:#7d8590;transition:all .15s;margin-bottom:2px}
.qi:hover{background:var(--sidebar-hover);border-color:#30363d;color:#c9d1d9}
.qi.active{background:rgba(31,111,235,.15);border-color:rgba(31,111,235,.4);color:#58a6ff}
.qi.active .qi-num{background:rgba(31,111,235,.25);color:#58a6ff}
.qi.active .qi-title{color:#e6edf3}
.qi.active .qi-sub{color:#8b949e}
.qi-num{width:22px;height:22px;border-radius:var(--r4);background:#21262d;
  font-family:var(--mono);font-size:10px;font-weight:600;
  display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:1px}
.qi-body{flex:1;min-width:0}
.qi-title{font-size:12px;font-weight:600;color:#c9d1d9;margin-bottom:2px;line-height:1.3}
.qi-sub{font-size:10px;color:#484f58;line-height:1.4}
.sb-foot{padding:12px 16px;border-top:1px solid #21262d;
  display:flex;align-items:center;gap:8px}
.st-dot{width:8px;height:8px;border-radius:50%;background:#484f58;flex-shrink:0;transition:background .3s}
.st-dot.run{background:#f0883e;animation:pulse .9s ease-in-out infinite}
.st-dot.ok{background:#3fb950}
.st-dot.err{background:#f85149}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
.st-txt{font-size:11px;color:#7d8590;flex:1}
.st-ver{font-family:var(--mono);font-size:10px;color:#484f58}
.sb-nav-sep{height:1px;background:#21262d;margin:8px 4px}
.qi-db{display:flex;align-items:center;gap:10px;width:100%;text-align:left;
  padding:10px;border:1px solid #238636;border-radius:var(--r8);
  background:rgba(35,134,54,.08);cursor:pointer;color:#3fb950;
  transition:all .15s;margin-bottom:6px}
.qi-db:hover{background:rgba(35,134,54,.16);border-color:#3fb950;color:#4ae364}
.qi-db.active{background:rgba(35,134,54,.22);border-color:#56d364;color:#56d364}
.qi-db .qi-num{background:rgba(35,134,54,.2);color:#3fb950;border-radius:var(--r4);
  width:22px;height:22px;font-family:var(--mono);font-size:10px;font-weight:600;
  display:flex;align-items:center;justify-content:center;flex-shrink:0}
.qi-db.active .qi-num{background:rgba(35,134,54,.35);color:#56d364}

/* MAIN */
.main{display:flex;flex-direction:column;overflow:hidden;background:var(--bg)}
.topbar{background:var(--surface);border-bottom:1px solid var(--border);
  display:flex;align-items:center;padding:0 24px;min-height:46px;gap:4px;flex-shrink:0;
  overflow-x:auto;box-shadow:var(--s1)}
.topbar::-webkit-scrollbar{height:0}
.tb-label{font-size:11px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;
  color:var(--muted);margin-right:6px;white-space:nowrap}
.tpill{padding:5px 14px;font-size:12px;font-weight:500;color:var(--mid);
  background:transparent;border:1px solid transparent;border-radius:20px;
  cursor:pointer;white-space:nowrap;transition:all .15s}
.tpill:hover{background:var(--surface2);border-color:var(--border);color:var(--ink)}
.tpill.on{background:var(--blue);color:#fff;border-color:var(--blue);font-weight:600;
  box-shadow:0 1px 4px rgba(9,105,218,.3)}
.content{flex:1;overflow-y:auto;padding:28px 32px 40px}
.content::-webkit-scrollbar{width:5px}
.content::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}

/* WELCOME */
.welcome{display:flex;flex-direction:column;align-items:center;justify-content:center;
  height:100%;text-align:center}
.wlc-track{position:relative;width:72px;height:10px;margin-bottom:28px}
.wlc-rail{position:absolute;bottom:0;left:0;right:0;height:3px;background:var(--border);border-radius:2px}
.wlc-train{position:absolute;bottom:2px;left:-44%;width:44%;height:6px;
  background:linear-gradient(90deg,transparent,var(--blue),#0d419d);
  border-radius:3px;animation:choo 2.8s ease-in-out infinite}
@keyframes choo{0%{left:-44%}100%{left:110%}}
.wlc-h{font-size:26px;font-weight:800;color:var(--ink);letter-spacing:-.03em;margin-bottom:10px}
.wlc-p{font-size:13px;color:var(--mid);max-width:360px;line-height:1.7;margin-bottom:32px}
.wlc-cards{display:grid;grid-template-columns:1fr 1fr;gap:12px;width:100%;max-width:440px}
.wlc-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r12);
  padding:16px 18px;text-align:left;box-shadow:var(--s1)}
.wlc-card-icon{font-size:20px;margin-bottom:8px}
.wlc-card-title{font-size:12px;font-weight:700;color:var(--ink);margin-bottom:4px}
.wlc-card-body{font-size:11px;color:var(--mid);line-height:1.5}

/* PAGE HEADER */
.pg-hd{margin-bottom:24px}
.pg-bc{font-size:11px;color:var(--muted);margin-bottom:4px;letter-spacing:.02em}
.pg-t{font-size:22px;font-weight:800;color:var(--ink);letter-spacing:-.03em;margin-bottom:6px}
.pg-d{font-size:13px;color:var(--mid);max-width:680px;line-height:1.6}

/* KPI CARDS */
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));
  gap:16px;margin-bottom:28px}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:var(--r12);
  padding:20px 20px 16px;box-shadow:var(--s2);position:relative;overflow:hidden;
  transition:box-shadow .2s,transform .2s}
.kpi:hover{box-shadow:var(--s3);transform:translateY(-2px)}
.kpi::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:var(--r12) var(--r12) 0 0}
.kpi.blue::before{background:linear-gradient(90deg,#0969da,#388bfd)}
.kpi.green::before{background:linear-gradient(90deg,#1a7f37,#3fb950)}
.kpi.amber::before{background:linear-gradient(90deg,#9a6700,#d1a91a)}
.kpi.red::before{background:linear-gradient(90deg,#cf222e,#ff7b72)}
.kpi.purple::before{background:linear-gradient(90deg,#8250df,#bc8cff)}
.kpi-icon{width:36px;height:36px;border-radius:var(--r8);display:flex;align-items:center;
  justify-content:center;margin-bottom:14px}
.kpi.blue .kpi-icon{background:var(--blue-l);color:var(--blue)}
.kpi.green .kpi-icon{background:var(--green-l);color:var(--green)}
.kpi.amber .kpi-icon{background:var(--amber-l);color:var(--amber)}
.kpi.red .kpi-icon{background:var(--red-l);color:var(--red)}
.kpi.purple .kpi-icon{background:var(--purple-l);color:var(--purple)}
.kpi-icon svg{width:18px;height:18px;fill:currentColor}
.kpi-val{font-size:28px;font-weight:800;font-family:var(--mono);color:var(--ink);
  line-height:1;letter-spacing:-.03em;margin-bottom:4px}
.kpi-label{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--mid)}
.kpi-sub{font-size:11px;color:var(--muted);margin-top:3px}
.kpi-trend{position:absolute;top:16px;right:16px;font-size:11px;font-weight:600;
  display:flex;align-items:center;gap:3px;padding:3px 8px;border-radius:12px}
.kpi-trend.up{background:var(--red-l);color:var(--red)}
.kpi-trend.ok{background:var(--green-l);color:var(--green)}

/* ALERTS */
.alert{display:flex;align-items:flex-start;gap:12px;padding:14px 18px;
  border-radius:var(--r8);margin-bottom:20px;border:1px solid}
.alert.info{background:#ddf4ff;border-color:#54aeff;color:#0550ae}
.alert.warn{background:#fff8c5;border-color:#d4a72c;color:#7d4e00}
.alert.err{background:#ffebe9;border-color:#ff8182;color:#a40000}
.alert.ok{background:#dcffe4;border-color:#56d364;color:#116329}
.alert svg{width:16px;height:16px;fill:currentColor;flex-shrink:0;margin-top:1px}
.alert-txt{font-size:12px;line-height:1.6}
.alert-txt strong{font-weight:700}

/* INNER TABS */
.itabs{display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap}
.itab{padding:6px 16px;font-size:12px;font-weight:500;border:1px solid var(--border);
  border-radius:20px;background:var(--surface);cursor:pointer;color:var(--mid);transition:all .14s}
.itab:hover{border-color:var(--border2);color:var(--ink)}
.itab.on{background:var(--ink);border-color:var(--ink);color:#fff;font-weight:600}

/* TABLE */
.tbl-card{background:var(--surface);border:1px solid var(--border);
  border-radius:var(--r12);overflow:hidden;box-shadow:var(--s2)}
.tbl-bar{display:flex;align-items:center;padding:14px 20px;
  border-bottom:1px solid var(--border);gap:10px;background:var(--surface)}
.tbl-cnt{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--mid)}
.tbl-scroll{overflow:auto;max-height:480px}
.tbl-scroll::-webkit-scrollbar{width:5px;height:5px}
.tbl-scroll::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
table{width:100%;border-collapse:collapse;font-size:12px}
thead{position:sticky;top:0;z-index:2}
thead th{background:#f6f8fa;color:var(--mid);font-weight:700;font-size:10px;
  letter-spacing:.07em;text-transform:uppercase;padding:10px 16px;
  text-align:left;border-bottom:2px solid var(--border);white-space:nowrap}
tbody tr{border-bottom:1px solid #f0f3f6;transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:#f6f8fa}
td{padding:9px 16px;white-space:nowrap;color:var(--body)}
td.mono{font-family:var(--mono);font-size:11px;color:var(--ink)}
td.txt{font-family:var(--font);font-size:12px;color:var(--body)}
.tag{display:inline-flex;align-items:center;padding:2px 9px;border-radius:12px;
  font-size:10px;font-weight:700;letter-spacing:.03em;white-space:nowrap}
.tag-up{background:#ddf4ff;color:#0550ae}
.tag-dn{background:#ffebe9;color:#a40000}
.tag-3rd{background:#fbefff;color:#6e40c9}
.tag-4th{background:#fff8c5;color:#7d4e00}
.tag-n{background:var(--surface2);color:var(--mid)}
.sev-crit{color:#a40000;font-weight:700;font-family:var(--mono);font-size:11px}
.sev-high{color:#d4601a;font-weight:600;font-family:var(--mono);font-size:11px}
.sev-med{color:#9a6700;font-weight:600;font-family:var(--mono);font-size:11px}
.sev-ok{color:#1a7f37;font-family:var(--mono);font-size:11px}
.empty{padding:48px;text-align:center;color:var(--muted)}
.empty-ico{font-size:32px;margin-bottom:8px}

/* CONSECUTIVE BAR */
.km-bar{display:flex;align-items:center;gap:8px}
.km-bg{height:5px;background:var(--surface2);border-radius:3px;width:80px;overflow:hidden}
.km-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,#ff7b72,#cf222e)}
.km-val{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--red)}

/* TERMINAL */
.terminal{background:#0d1117;border-radius:var(--r12);overflow:hidden;box-shadow:var(--s4)}
.term-hd{background:#161b22;padding:12px 18px;display:flex;align-items:center;gap:8px;
  border-bottom:1px solid #21262d}
.term-dots{display:flex;gap:5px}
.td{width:11px;height:11px;border-radius:50%}
.td1{background:#ff5f57}.td2{background:#febc2e}.td3{background:#28c840}
.term-title{font-family:var(--mono);font-size:11px;color:#484f58;margin-left:4px;flex:1}
.term-cnt{font-family:var(--mono);font-size:10px;color:#30363d}
.term-body{padding:16px 20px;font-family:var(--mono);font-size:11px;
  line-height:1.85;max-height:380px;overflow-y:auto;color:#8b949e}
.term-body::-webkit-scrollbar{width:4px}
.term-body::-webkit-scrollbar-thumb{background:#21262d;border-radius:2px}
.lt-ts{color:#30363d;margin-right:8px}
.lt-info{color:#79c0ff}
.lt-warn{color:#e3b341}
.lt-err{color:#f85149}
.lt-done{color:#3fb950;font-weight:600}
.lt-sep{color:#21262d}

/* SPINNER */
.spin{width:14px;height:14px;border:2px solid var(--border);
  border-top-color:var(--blue);border-radius:50%;
  animation:sp .6s linear infinite;display:inline-block;flex-shrink:0}
@keyframes sp{to{transform:rotate(360deg)}}
.loading{display:flex;align-items:center;gap:10px;padding:60px 28px;color:var(--mid);font-size:13px}

/* DB SUMMARY */
.db-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}
.db-tile{background:var(--surface);border:1px solid var(--border);border-radius:var(--r12);
  padding:16px 18px;box-shadow:var(--s1);transition:box-shadow .15s}
.db-tile:hover{box-shadow:var(--s2)}
.db-tile-name{font-family:var(--mono);font-size:11px;color:var(--mid);margin-bottom:8px;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.db-tile-num{font-family:var(--mono);font-size:26px;font-weight:700;color:var(--ink);line-height:1}
.db-tile-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:3px}

.fade{animation:fi .2s ease}
@keyframes fi{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}
</style>
</head>
<body>
<div class="app">

<!-- SIDEBAR -->
<div class="sb">
  <div class="sb-brand">
    <div class="sb-brand-row">
      <div class="sb-badge">
        <svg viewBox="0 0 24 24"><path d="M4 17h2v2H4v-2zm3 0h2v2H7v-2zm3 0h2v2h-2v-2zm3 0h2v2h-2v-2zm3 0h2v2h-2v-2zM2 9l2-4h16l2 4H2zm18 2H4v5h16v-5z"/></svg>
      </div>
      <div>
        <div class="sb-name">TRC Analytics</div>
        <div class="sb-tagline">Inspection Intelligence Platform</div>
      </div>
    </div>
    <div class="sb-org"><div class="sb-org-dot"></div>BSL Division · Central Railway</div>
  </div>

  <div class="sb-pipe">
    <div class="sb-label">Pipeline</div>
    <div class="sb-field">
      <label><svg viewBox="0 0 16 16"><path d="M2 1.75A.75.75 0 0 1 2.75 1h10.5c.41 0 .75.34.75.75v12.5c0 .41-.34.75-.75.75H2.75a.75.75 0 0 1-.75-.75V1.75zm1.5.75v11h9V2.5h-9z"/></svg>Data Folder</label>
      <input class="sb-input" id="folderPath" value="D:\ENGINEER\IndianRailwaysProject\data" placeholder="D:\path\to\data"/>
    </div>
    <div class="sb-field">
      <label><svg viewBox="0 0 16 16"><path d="M8 0a8 8 0 1 0 0 16A8 8 0 0 0 8 0zM1.5 8a6.5 6.5 0 1 1 13 0 6.5 6.5 0 0 1-13 0z"/></svg>Database</label>
      <input class="sb-input" id="dbPath" value="railway.db" placeholder="railway.db"/>
    </div>
    <div class="sb-script" id="sbScript">
      <div class="sb-script-dot"></div>
      <div class="sb-script-txt" id="sbScriptTxt">Checking pipeline script…</div>
    </div>
    <button class="btn-run" id="btnRun" onclick="runPipeline()">
      <svg viewBox="0 0 16 16"><path d="M8 0a8 8 0 1 0 0 16A8 8 0 0 0 8 0zM6.5 4.75l5 3.25-5 3.25V4.75z"/></svg>
      Run Pipeline
    </button>
    <div class="prog-wrap" id="progWrap" style="display:none">
      <div class="prog-bg"><div class="prog-fill" id="progFill"></div></div>
      <div class="prog-lbl" id="progLbl">Processing…</div>
    </div>
  </div>

  <div class="sb-nav">
    <div class="sb-nav-label">Database</div>
    <button class="qi-db" id="qiDB" onclick="openDbSummary()">
      <div class="qi-num">
        <svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C5.246 0 2 1.29 2 3.5v9C2 14.71 5.246 16 8 16s6-1.29 6-3.5v-9C14 1.29 10.754 0 8 0zm4.682 3.5C12.682 4.604 10.584 6 8 6S3.318 4.604 3.318 3.5C3.318 2.396 5.416 1 8 1s4.682 1.396 4.682 2.5z"/></svg>
      </div>
      <div class="qi-body">
        <div class="qi-title">DB Summary</div>
        <div class="qi-sub">Tables, row counts &amp; schema overview</div>
      </div>
    </button>
    <div class="sb-nav-sep"></div>
    <div class="sb-nav-label">Analytical Queries</div>
    <button class="qi" id="qi1" onclick="loadQ(1)">
      <div class="qi-num">Q1</div>
      <div class="qi-body"><div class="qi-title">Worst 20% Locations</div>
      <div class="qi-sub">Critical spots across all 8 defect reports</div></div>
    </button>
    <button class="qi" id="qi2" onclick="loadQ(2)">
      <div class="qi-num">Q2</div>
      <div class="qi-body"><div class="qi-title">Resource Deployment</div>
      <div class="qi-sub">Rail · Sleepers · Fittings · Gap priorities</div></div>
    </button>
    <button class="qi" id="qi3" onclick="loadQ(3)">
      <div class="qi-num">Q3</div>
      <div class="qi-body"><div class="qi-title">False Alert Detection</div>
      <div class="qi-sub">SOD &amp; vegetation — real vs false positives</div></div>
    </button>
    <button class="qi" id="qi4" onclick="loadQ(4)">
      <div class="qi-num">Q4</div>
      <div class="qi-body"><div class="qi-title">Consecutive Defects</div>
      <div class="qi-sub">2+ / 3+ consecutive KMs with bad sleepers</div></div>
    </button>
    <button class="qi" id="qi5" onclick="loadQ(5)">
      <div class="qi-num">Q5</div>
      <div class="qi-body"><div class="qi-title">Repeated in 3 TRC Runs</div>
      <div class="qi-sub">Chronic deficiency locations across runs</div></div>
    </button>
    <div class="sb-nav-sep"></div>
    <div class="sb-nav-label">DOCX Reports</div>
    <button class="qi" id="qi6" onclick="loadQ(6)">
      <div class="qi-num" style="font-size:9px">RPT</div>
      <div class="qi-body"><div class="qi-title">Track Reports (DOCX)</div>
      <div class="qi-sub">Structured tables extracted from Word files</div></div>
    </button>
  </div>

  <div class="sb-foot">
    <div class="st-dot" id="stDot"></div>
    <span class="st-txt" id="stTxt">Ready</span>
    <span class="st-ver">v2.0</span>
  </div>
</div>

<!-- MAIN -->
<div class="main">
  <div class="topbar" id="topbar"></div>
  <div class="content" id="content">
    <div class="welcome fade">
      <div class="wlc-track"><div class="wlc-rail"></div><div class="wlc-train"></div></div>
      <div class="wlc-h">Railway TRC Analytics</div>
      <div class="wlc-p">Select a query from the sidebar to extract actionable insights, or run the pipeline to import new Excel files.</div>
      <div class="wlc-cards">
        <div class="wlc-card"><div class="wlc-card-icon">🚀</div><div class="wlc-card-title">First Time?</div>
          <div class="wlc-card-body">Click <strong>Run Pipeline</strong> to load your Excel files, then click any query.</div></div>
        <div class="wlc-card"><div class="wlc-card-icon">📊</div><div class="wlc-card-title">Have Data?</div>
          <div class="wlc-card-body">Click Q1–Q5 on the left to instantly run analytical queries.</div></div>
        <div class="wlc-card"><div class="wlc-card-icon">📁</div><div class="wlc-card-title">Setup Note</div>
          <div class="wlc-card-body"><code>app.py</code> and <code>railway_pipeline.py</code> must be in the <strong>same folder</strong>.</div></div>
        <div class="wlc-card"><div class="wlc-card-icon">🔴</div><div class="wlc-card-title">Priority System</div>
          <div class="wlc-card-body">Red = critical severity. Amber = elevated. Green = acceptable.</div></div>
      </div>
    </div>
  </div>
</div>
</div>

<script>
/* ══ GLOBALS & UTILS ══ */
const G={q:null,evtSrc:null,pipeLogLines:[]};
const DB=()=>document.getElementById('dbPath').value.trim()||'railway.db';
const $=id=>document.getElementById(id);
const e=s=>String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const enc=s=>encodeURIComponent(s);

/* ── NULL-SAFE HELPERS ──
   The key fix: all numeric display values go through these helpers
   so undefined/null never reaches .toLocaleString() ──────────────── */
const safeNum = v => {
  const n = parseFloat(v);
  return isNaN(n) ? 0 : n;
};
const fmtNum = v => safeNum(v).toLocaleString();
const fmtVal = v => (v === null || v === undefined || v === '') ? '—' : String(v);

const LABELS={
  section_name:'Section',line_direction:'Dir',sheet_name:'Sheet',
  start_location_km:'Start KM',start_location_meter:'Start M',
  location_km:'KM',location_meter:'Meter',location_block:'Block',
  from_km:'From KM',to_km:'To KM',consecutive_kms:'Consec. KMs',
  total_defects:'Total Defects',max_vertical_wear_mm:'Max Wear (mm)',
  average_vertical_wear_mm:'Avg Wear (mm)',max_wear_mm:'Max Lateral (mm)',
  max_value_mm:'Max Value (mm)',value_of_defect_mm:'Gap (mm)',
  gap_mm:'Gap (mm)',wear_mm:'Wear (mm)',type:'Type',
  length_of_track_having_m_defect_length:'Length (m)',
  score:'Score',total:'Total',
  run_date:'Run Date',run_count:'Run Count',run_dates:'Run Dates',
  rail_side:'Rail Side',defect:'Defect',broken:'Broken',cracked:'Cracked',
  spalling:'Spalling',lc:'L-Clip',lb:'L-Bolt',rc:'R-Clip',rb:'R-Bolt',
  km:'KM',m:'Meter',len:'Length (m)',
  location_start_location_km:'Start KM',
  size_of_obstacle_mm_l:'L (mm)',size_of_obstacle_mm_b:'B (mm)',
  size_of_obstacle_mm_h:'H (mm)',
};

function setStatus(cls,txt){$('stDot').className='st-dot '+cls;$('stTxt').textContent=txt;}

function setTabs(tabs,ai,fn){
  $('topbar').innerHTML=(tabs.length>1?'<span class="tb-label">View</span>':'')+
    tabs.map((t,i)=>`<button class="tpill ${i===ai?'on':''}" onclick="${fn}(${i})">${e(t)}</button>`).join('');
}

/* ══ SCRIPT CHECK ══ */
async function checkScript(){
  try{
    const d=await(await fetch('/api/script_status')).json();
    const el=$('sbScript'),tx=$('sbScriptTxt');
    if(d.found){el.className='sb-script ok';tx.textContent='pipeline.py detected ✓';el.title=d.path||'';}
    else{el.className='sb-script err';tx.textContent='railway_pipeline.py not found!';}
  }catch(_){}
}

/* ══ PIPELINE ══ */
function runPipeline(){
  const folder=$('folderPath').value.trim();
  if(!folder){alert('Enter the data folder path.');return;}
  G.pipeLogLines=[];
  $('btnRun').disabled=true;
  $('progWrap').style.display='block';
  $('progFill').style.width='3%';
  $('progLbl').textContent='Starting…';
  setStatus('run','Running pipeline…');
  setTabs(['Live Log','DB Summary'],0,'pipeTab');
  showPipeLog();
  fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({folder,db:DB()})})
  .then(r=>{if(!r.ok)return r.json().then(d=>{throw new Error(d.error||'Error');});})
  .then(()=>{
    if(G.evtSrc)G.evtSrc.close();
    G.evtSrc=new EventSource('/stream');
    G.evtSrc.onmessage=ev=>{
      if(ev.data==='__DONE__'){G.evtSrc.close();donePipe(true);return;}
      if(ev.data==='__ERROR__'){G.evtSrc.close();donePipe(false);return;}
      appendLog(ev.data);
      const p=Math.min(93,parseInt($('progFill').style.width||'3')+0.7);
      $('progFill').style.width=p+'%';
      $('progLbl').textContent=`Processing… (${G.pipeLogLines.length} lines)`;
    };
    G.evtSrc.onerror=()=>{G.evtSrc.close();donePipe(false);};
  })
  .catch(err=>{$('btnRun').disabled=false;setStatus('err','Error');
    $('content').innerHTML=errBanner(err.message);});
}
function donePipe(ok){
  $('btnRun').disabled=false;
  $('progFill').style.width='100%';
  $('progLbl').textContent=ok?'Complete ✓':'Finished with errors';
  setStatus(ok?'ok':'err',ok?'Pipeline complete':'Pipeline errors');
  if(ok)checkScript();
}
function showPipeLog(){
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd"><div class="pg-bc">PIPELINE</div>
    <div class="pg-t">Live Output</div>
    <div class="pg-d">Real-time output from railway_pipeline.py</div></div>
    <div class="terminal">
      <div class="term-hd">
        <div class="term-dots"><div class="td td1"></div><div class="td td2"></div><div class="td td3"></div></div>
        <div class="term-title">railway_pipeline.py</div>
        <div class="term-cnt" id="termCnt">0 lines</div>
      </div>
      <div class="term-body" id="termBody">${G.pipeLogLines.length?G.pipeLogLines.join('\n'):'<span class="lt-sep">// Awaiting output…</span>'}</div>
    </div></div>`;
}
function pipeTab(i){setTabs(['Live Log','DB Summary'],i,'pipeTab');if(i===0)showPipeLog();else loadDbSummary();}
function appendLog(raw){
  const line=fmtLog(raw);G.pipeLogLines.push(line);
  const body=$('termBody');if(!body)return;
  if(body.innerHTML.includes('Awaiting'))body.innerHTML='';
  body.innerHTML+=line+'\n';body.scrollTop=body.scrollHeight;
  const c=$('termCnt');if(c)c.textContent=G.pipeLogLines.length+' lines';
}
function fmtLog(raw){
  const m=raw.match(/^(\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}),\d+) \[(\w+)\] (.*)$/);
  if(!m)return`<span class="lt-sep">${e(raw)}</span>`;
  const[,,ts,lvl,msg]=m;
  let cls=lvl==='WARNING'?'lt-warn':lvl==='ERROR'?'lt-err':
    msg.includes('COMPLETE')||msg.includes('complete')?'lt-done':
    msg.trim().startsWith('=')?'lt-sep':'lt-info';
  return`<span class="${cls}"><span class="lt-ts">${ts}</span>${e(msg)}</span>`;
}

/* ══ DB SUMMARY ══ */
async function loadDbSummary(){
  $('content').innerHTML=loadHtml('Loading database…');
  try{
    const tbls=await(await fetch('/api/tables?db='+enc(DB()))).json();
    if(!tbls.length||tbls.error){$('content').innerHTML=errBanner('No tables found. Run the pipeline first.');return;}
    const tot=tbls.reduce((s,t)=>s+(t.rows||0),0);
    $('content').innerHTML=`<div class="fade">
      <div class="pg-hd"><div class="pg-bc">PIPELINE · RESULTS</div>
      <div class="pg-t">Database Summary</div>
      <div class="pg-d">Tables stored in <code>${e(DB())}</code></div></div>
      <div class="kpis">
        <div class="kpi blue"><div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M0 2.5A1.5 1.5 0 0 1 1.5 1h13A1.5 1.5 0 0 1 16 2.5v11a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 0 13.5v-11z"/></svg></div>
          <div class="kpi-val">${tbls.length}</div><div class="kpi-label">Tables</div></div>
        <div class="kpi green"><div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M0 2.5A1.5 1.5 0 0 1 1.5 1h13A1.5 1.5 0 0 1 16 2.5v2A1.5 1.5 0 0 1 14.5 6h-13A1.5 1.5 0 0 1 0 4.5v-2z"/></svg></div>
          <div class="kpi-val">${tot.toLocaleString()}</div><div class="kpi-label">Total Rows</div></div>
      </div>
      <div class="db-grid">${tbls.map(t=>`
        <div class="db-tile"><div class="db-tile-name">${e(t.table)}</div>
        <div class="db-tile-num">${(t.rows||0).toLocaleString()}</div>
        <div class="db-tile-lbl">rows</div></div>`).join('')}
      </div></div>`;
  }catch(ex){$('content').innerHTML=errBanner(ex.message);}
}

/* ══ QUERY LOADER ══ */
async function openDbSummary(){
  /* Mark DB Summary button active, deactivate all query buttons */
  document.querySelectorAll('.qi').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.qi-db').forEach(b=>b.classList.remove('active'));
  $('qiDB').classList.add('active');
  G.q=null;
  $('topbar').innerHTML='';
  setStatus('run','Loading DB summary…');
  await loadDbSummary();
  setStatus('ok','DB Summary loaded');
}

async function loadQ(n){
  document.querySelectorAll('.qi').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.qi-db').forEach(b=>b.classList.remove('active'));
  $('qi'+n).classList.add('active');
  G.q=n;$('topbar').innerHTML='';
  $('content').innerHTML=loadHtml('Running query Q'+n+'…');
  setStatus('run','Query Q'+n+'…');
  try{
    if(n===1)await rQ1();
    else if(n===2)await rQ2();
    else if(n===3)await rQ3();
    else if(n===4)await rQ4();
    else if(n===5)await rQ5();
    else if(n===6)await rQ6();
    setStatus('ok','Query Q'+n+' done');
  }catch(ex){$('content').innerHTML=errBanner(ex.message);setStatus('err','Error');}
}

/* ── Q1 ── */
let _d1={},_k1=[];
async function rQ1(){
  const r=await fetch(`/api/worst20?db=${enc(DB())}&scope=division`);
  _d1=await r.json();
  if(_d1.error)throw new Error(_d1.error);
  _k1=Object.keys(_d1);
  if(!_k1.length){$('content').innerHTML=errBanner('No data. Run pipeline first.');return;}
  setTabs(_k1,0,'q1t');showQ1(0);
}
function q1t(i){setTabs(_k1,i,'q1t');showQ1(i);}
function showQ1(i){
  const k=_k1[i], d=_d1[k]||{};
  /* FIX: null-safe access with fallback defaults */
  const top20 = d.top20_count ?? 0;
  const total  = d.total ?? 0;
  const unit   = d.unit  ?? '—';
  const pct    = total ? Math.round(top20/total*100) : 0;
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd">
      <div class="pg-bc">Q1 · WORST 20% LOCATIONS</div>
      <div class="pg-t">${e(k)}</div>
      <div class="pg-d">Top 20% critical locations ranked by severity. Prioritise crew deployment here first.</div>
    </div>
    <div class="kpis">
      <div class="kpi red">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M6.457 1.047c.659-1.234 2.427-1.234 3.086 0l6.082 11.378A1.75 1.75 0 0 1 14.082 15H1.918a1.75 1.75 0 0 1-1.543-2.575L6.457 1.047zM9 11H7V9h2v2zm0-3H7V5h2v3z"/></svg></div>
        <div class="kpi-val">${top20.toLocaleString()}</div>
        <div class="kpi-label">Top 20% (Worst)</div>
        <div class="kpi-sub">${pct}% of all records</div>
        <div class="kpi-trend up">↑ Critical</div>
      </div>
      <div class="kpi blue">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M0 1.5A.5.5 0 0 1 .5 1H2a.5.5 0 0 1 .485.379L2.89 3H14.5a.5.5 0 0 1 .491.592l-1.5 8A.5.5 0 0 1 13 12H4a.5.5 0 0 1-.491-.408L2.01 3.607 1.61 2H.5a.5.5 0 0 1-.5-.5z"/></svg></div>
        <div class="kpi-val">${total.toLocaleString()}</div>
        <div class="kpi-label">Total Records</div>
        <div class="kpi-sub">All defect entries</div>
      </div>
      <div class="kpi purple">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M1 2.5A1.5 1.5 0 0 1 2.5 1h3A1.5 1.5 0 0 1 7 2.5v3A1.5 1.5 0 0 1 5.5 7h-3A1.5 1.5 0 0 1 1 5.5v-3z"/></svg></div>
        <div class="kpi-val" style="font-size:16px">${e(unit)}</div>
        <div class="kpi-label">Severity Metric</div>
        <div class="kpi-sub">Ranking criterion</div>
      </div>
    </div>
    ${mkTable(d.cols||[],d.rows||[],(d.cols||[])[5]||'')}
  </div>`;
}

/* ── Q2 ── */
let _d2={},_k2=[];
async function rQ2(){
  const r=await fetch(`/api/resources?db=${enc(DB())}`);
  _d2=await r.json();
  if(_d2.error)throw new Error(_d2.error);
  _k2=Object.keys(_d2);
  if(!_k2.length){$('content').innerHTML=errBanner('No data. Run pipeline first.');return;}
  setTabs(_k2,0,'q2t');showQ2(0);
}
function q2t(i){setTabs(_k2,i,'q2t');showQ2(i);}
function showQ2(i){
  const k=_k2[i], d=_d2[k]||{};
  /* FIX: null-safe */
  const rows   = d.rows   ?? [];
  const thr    = d.threshold !== undefined ? d.threshold : (d.threshold_mm ?? '—');
  const unit   = d.unit   ?? '';
  const thrStr = thr === '—' ? '—' : `${thr} ${unit}`;
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd">
      <div class="pg-bc">Q2 · RESOURCE DEPLOYMENT</div>
      <div class="pg-t">${e(k)}</div>
      <div class="pg-d">${e(d.desc||'')}</div>
    </div>
    <div class="kpis">
      <div class="kpi red">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M11.251.068a.5.5 0 0 1 .227.58L9.677 6.5H13a.5.5 0 0 1 .364.843l-8 8.5a.5.5 0 0 1-.842-.49L6.323 9.5H3a.5.5 0 0 1-.364-.843l8-8.5a.5.5 0 0 1 .615-.09z"/></svg></div>
        <div class="kpi-val">${rows.length.toLocaleString()}</div>
        <div class="kpi-label">Priority Locations</div>
        <div class="kpi-sub">Immediate attention</div>
      </div>
      <div class="kpi amber">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M8 1a2 2 0 0 1 2 2v4H6V3a2 2 0 0 1 2-2zm3 6V3a3 3 0 0 0-6 0v4a2 2 0 0 0-2 2v5a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2z"/></svg></div>
        <div class="kpi-val" style="font-size:18px">${e(thrStr)}</div>
        <div class="kpi-label">80th Pct Threshold</div>
        <div class="kpi-sub">Minimum for priority</div>
      </div>
    </div>
    ${mkTable(d.cols||[],rows,(d.cols||[])[5]||'')}
  </div>`;
}

/* ── Q3 ── */
let _d3={},_k3=[],_i3={};
async function rQ3(){
  const r=await fetch(`/api/false_alerts?db=${enc(DB())}`);
  _d3=await r.json();
  if(_d3.error)throw new Error(_d3.error);
  _k3=Object.keys(_d3);
  if(!_k3.length){$('content').innerHTML=errBanner('No SOD or vegetation data.');return;}
  _k3.forEach(k=>_i3[k]=0);
  setTabs(_k3,0,'q3t');showQ3(0);
}
function q3t(i){setTabs(_k3,i,'q3t');showQ3(i);}
function showQ3(i){
  const k=_k3[i], d=_d3[k]||{};
  /* FIX: null-safe */
  const single     = d.single     ?? 0;
  const persistent = d.persistent ?? 0;
  const tabs       = d.tabs       || {};
  const ik=Object.keys(tabs), ai=_i3[k]||0;
  const td=tabs[ik[ai]]||{cols:[],rows:[]};
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd">
      <div class="pg-bc">Q3 · FALSE ALERT DETECTION</div>
      <div class="pg-t">${e(k)}</div>
      <div class="pg-d">${e(d.desc||'')}</div>
    </div>
    <div class="kpis">
      <div class="kpi amber">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M7.002 11a1 1 0 1 1 2 0 1 1 0 0 1-2 0zM7.1 4.995a.905.905 0 1 1 1.8 0l-.35 3.507a.552.552 0 0 1-1.1 0z"/></svg></div>
        <div class="kpi-val">${single}</div>
        <div class="kpi-label">Likely False Alerts</div>
        <div class="kpi-sub">Seen in 1 run only</div>
        <div class="kpi-trend up">Investigate</div>
      </div>
      <div class="kpi green">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M10.97 4.97a.75.75 0 0 1 1.07 1.05l-3.99 4.99a.75.75 0 0 1-1.08.02L4.324 8.384a.75.75 0 1 1 1.06-1.06l2.094 2.093 3.473-4.425z"/></svg></div>
        <div class="kpi-val">${persistent}</div>
        <div class="kpi-label">Persistent Alerts</div>
        <div class="kpi-sub">Seen in 2+ runs</div>
        <div class="kpi-trend ok">Confirmed</div>
      </div>
    </div>
    <div class="itabs">${ik.map((ik2,j)=>`<button class="itab ${j===ai?'on':''}" onclick="q3Inner(${i},${j})">${e(ik2)}</button>`).join('')}</div>
    ${mkTable(td.cols||[],td.rows||[])}
  </div>`;
}
function q3Inner(ti,ii){_i3[_k3[ti]]=ii;showQ3(ti);}

/* ── Q4 ── */
async function rQ4(){
  const r=await fetch(`/api/consecutive?db=${enc(DB())}&n=2`);
  const d=await r.json();if(d.error)throw new Error(d.error);
  setTabs(['≥ 2 KMs','≥ 3 KMs'],0,'q4t');drawQ4(d);
}
function q4t(i){
  setTabs(['≥ 2 KMs','≥ 3 KMs'],i,'q4t');
  $('content').innerHTML=loadHtml('Computing…');
  fetch(`/api/consecutive?db=${enc(DB())}&n=${i===0?2:3}`).then(r=>r.json()).then(drawQ4);
}
function drawQ4(d){
  /* FIX: null-safe */
  const seqs = d.total_sequences ?? 0;
  const minC = d.min_consecutive ?? 2;
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd">
      <div class="pg-bc">Q4 · CONSECUTIVE DEFECTIVE KMs</div>
      <div class="pg-t">Consecutive Sleeper Defects (≥${minC} KMs)</div>
      <div class="pg-d">${e(d.desc||'')}</div>
    </div>
    <div class="kpis">
      <div class="kpi red">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M8 0a8 8 0 1 0 0 16A8 8 0 0 0 8 0zm.93 6.588-2.29.287-.082.38.45.083c.294.07.352.176.288.469l-.738 3.468c-.194.897.105 1.319.808 1.319.545 0 1.178-.252 1.465-.598l.088-.416c-.2.176-.492.246-.686.246-.275 0-.375-.193-.304-.533L8.93 6.588z"/></svg></div>
        <div class="kpi-val">${seqs.toLocaleString()}</div>
        <div class="kpi-label">Sequences Found</div>
        <div class="kpi-sub">Systematic deterioration zones</div>
      </div>
      <div class="kpi blue">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M1 0 0 1l2.2 3.081a1 1 0 0 0 .815.419h.07a1 1 0 0 1 .708.293l2.675 2.675-2.617 2.654A3.003 3.003 0 0 0 0 13a3 3 0 1 0 5.878-.851l2.654-2.617.968.968-.305.914a1 1 0 0 0 .242 1.023l3.27 3.27a.997.997 0 0 0 1.414 0l1.586-1.586a.997.997 0 0 0 0-1.414l-3.27-3.27a1 1 0 0 0-1.023-.242L10.5 9.5l-.96-.96 2.68-2.643A3.005 3.005 0 0 0 16 3c0-.269-.035-.53-.102-.777l-2.14 2.141L12 4l-.364-1.757L13.777.102a3 3 0 0 0-3.675 3.68L7.462 6.46 4.793 3.793a1 1 0 0 1-.293-.707v-.071a1 1 0 0 0-.419-.814z"/></svg></div>
        <div class="kpi-val">${minC}+</div>
        <div class="kpi-label">Min Consecutive KMs</div>
      </div>
    </div>
    ${mkConsecTable(d.rows||[])}
  </div>`;
}
function mkConsecTable(rows){
  if(!rows.length)return emptyState('No consecutive sequences found.');
  const maxV=Math.max(1,...rows.map(r=>safeNum(r.consecutive_kms)));
  const cols=["section_name","line_direction","from_km","to_km","consecutive_kms","total_defects","run_date"];
  const html=rows.map(row=>`<tr>${cols.map(c=>{
    if(c==='consecutive_kms'){
      const v=safeNum(row[c]),pct=Math.round(v/maxV*100);
      return`<td><div class="km-bar"><div class="km-bg"><div class="km-fill" style="width:${pct}%"></div></div><span class="km-val">${v}</span></div></td>`;
    }
    if(c==='line_direction')return`<td>${dirTag(row[c])}</td>`;
    const v=row[c];const isN=!isNaN(parseFloat(v))&&v!==''&&v!=null;
    return`<td class="${isN?'mono':'txt'}">${e(fmtVal(v))}</td>`;
  }).join('')}</tr>`).join('');
  return`<div class="tbl-card">
    <div class="tbl-bar"><span class="tbl-cnt">${rows.length.toLocaleString()} sequences</span></div>
    <div class="tbl-scroll"><table>
      <thead><tr>${cols.map(c=>`<th>${e(LABELS[c]||c.replace(/_/g,' '))}</th>`).join('')}</tr></thead>
      <tbody>${html}</tbody>
    </table></div></div>`;
}

/* ── Q5 ── */
let _d5={},_k5=[];
async function rQ5(){
  const r=await fetch(`/api/repeated?db=${enc(DB())}`);
  _d5=await r.json();if(_d5.error)throw new Error(_d5.error);
  _k5=Object.keys(_d5.tables||{});
  if(!_k5.length){
    $('content').innerHTML=`<div class="fade">
      <div class="pg-hd"><div class="pg-bc">Q5 · REPEATED IN 3+ RUNS</div>
      <div class="pg-t">Chronic Deficiency Locations</div>
      <div class="pg-d">${e(_d5.desc||'')}</div></div>
      <div class="alert info">
        <svg viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/></svg>
        <div class="alert-txt">No locations found deficient in 3+ consecutive TRC runs. Load more run data.</div>
      </div></div>`;return;
  }
  setTabs(_k5,0,'q5t');showQ5(0);
}
function q5t(i){setTabs(_k5,i,'q5t');showQ5(i);}
function showQ5(i){
  const k=_k5[i], d=(_d5.tables||{})[k]||{};
  /* FIX: null-safe */
  const cnt = d.count ?? 0;
  $('content').innerHTML=`<div class="fade">
    <div class="pg-hd">
      <div class="pg-bc">Q5 · REPEATED IN 3+ RUNS</div>
      <div class="pg-t">Chronic Locations — ${e(k.replace(/_/g,' '))}</div>
      <div class="pg-d">${e(_d5.desc||'')}</div>
    </div>
    <div class="kpis">
      <div class="kpi red">
        <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M11 6a3 3 0 1 1-6 0 3 3 0 0 1 6 0z"/><path fill-rule="evenodd" d="M0 8a8 8 0 1 1 16 0A8 8 0 0 1 0 8zm8-7a7 7 0 0 0-5.468 11.37C3.242 11.226 4.805 10 8 10s4.757 1.225 5.468 2.37A7 7 0 0 0 8 1z"/></svg></div>
        <div class="kpi-val">${cnt.toLocaleString()}</div>
        <div class="kpi-label">Chronic Locations</div>
        <div class="kpi-sub">Deficient in 3+ runs</div>
        <div class="kpi-trend up">Needs Attention</div>
      </div>
    </div>
    ${mkTable(d.cols||[],d.rows||[],'run_count')}
  </div>`;
}

/* ══ Q6 — Track Reports (DOCX rpt_* tables) ══ */
let _d6={},_k6=[];
async function rQ6(){
  const r=await fetch(`/api/reports?db=${enc(DB())}`);
  _d6=await r.json();
  if(_d6.error)throw new Error(_d6.error);
  const meta=_d6._meta||{};
  const tblMap=_d6.tables||{};
  _k6=Object.keys(tblMap);
  if(!meta.rpt_tables_count||!_k6.length){
    $('content').innerHTML=`<div class="fade">
      <div class="pg-hd"><div class="pg-bc">DOCX REPORTS</div>
      <div class="pg-t">Track Reports — Structured DOCX Tables</div>
      <div class="pg-d">Fallback-extracted Word tables stored as rpt_* in the database.</div></div>
      <div class="alert info">
        <svg viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M8.93 6.588l-2.29.287-.082.38.45.083c.294.07.352.176.288.469l-.738 3.468c-.194.897.105 1.319.808 1.319.545 0 1.178-.252 1.465-.598l.088-.416c-.2.176-.492.246-.686.246-.275 0-.375-.193-.304-.533z"/></svg>
        <div class="alert-txt">No <code>rpt_*</code> tables found yet. Run <strong>DOCX TQI Pipeline</strong> on a folder containing structured Word reports (OptionsReport, WearUrgentReport, TrackParametersUrgentReport, etc.).</div>
      </div></div>`;
    return;
  }
  // Build tab list: "Summary" first, then one tab per rpt_ table
  const tabNames=['Summary',..._k6.map(t=>t.replace(/^rpt_/,'').replace(/_t(\d+)$/,'').replace(/_/g,' '))];
  setTabs(tabNames,0,'q6t');showQ6(0);
}
function q6t(i){setTabs(['Summary',..._k6.map(t=>t.replace(/^rpt_/,'').replace(/_t(\d+)$/,'').replace(/_/g,' '))],i,'q6t');showQ6(i);}
function showQ6(i){
  const meta=_d6._meta||{};
  const byFile=_d6.by_file||{cols:[],rows:[]};
  const tblMap=_d6.tables||{};

  if(i===0){
    /* Summary tab */
    const totalRpt=meta.rpt_tables_count||0;
    const totalRows=meta.total_rows||0;
    const tblList=meta.tables||[];
    $('content').innerHTML=`<div class="fade">
      <div class="pg-hd">
        <div class="pg-bc">DOCX REPORTS · STRUCTURED TABLE EXTRACTION</div>
        <div class="pg-t">Track Reports — Word File Tables</div>
        <div class="pg-d">Structured tables extracted from .docx files via the fallback pipeline path (rpt_* tables). These include Options Reports, Wear Reports, Track Parameter reports and similar documents whose headers do not match TQI keywords.</div>
      </div>
      <div class="kpis">
        <div class="kpi blue">
          <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M4 0h8a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V2a2 2 0 0 1 2-2zm0 1a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1H4z"/><path d="M4 1.5h8v1H4zm0 3h8v1H4zm0 3h5v1H4z"/></svg></div>
          <div class="kpi-val">${totalRpt}</div>
          <div class="kpi-label">RPT Tables</div>
          <div class="kpi-sub">Across all Word files</div>
        </div>
        <div class="kpi green">
          <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M0 2.5A1.5 1.5 0 0 1 1.5 1h13A1.5 1.5 0 0 1 16 2.5v2A1.5 1.5 0 0 1 14.5 6h-13A1.5 1.5 0 0 1 0 4.5v-2z"/></svg></div>
          <div class="kpi-val">${totalRows.toLocaleString()}</div>
          <div class="kpi-label">Total Rows</div>
          <div class="kpi-sub">All extracted records</div>
        </div>
        <div class="kpi amber">
          <div class="kpi-icon"><svg viewBox="0 0 16 16"><path d="M14.5 3a.5.5 0 0 1 .5.5v9a.5.5 0 0 1-.5.5h-13a.5.5 0 0 1-.5-.5v-9a.5.5 0 0 1 .5-.5h13zm-13-1A1.5 1.5 0 0 0 0 3.5v9A1.5 1.5 0 0 0 1.5 14h13a1.5 1.5 0 0 0 1.5-1.5v-9A1.5 1.5 0 0 0 14.5 2h-13z"/></svg></div>
          <div class="kpi-val">${new Set(tblList.map(t=>e(t.source_file))).size}</div>
          <div class="kpi-label">Source Files</div>
          <div class="kpi-sub">Unique DOCX files</div>
        </div>
      </div>
      ${byFile.rows&&byFile.rows.length?`
      <div style="margin-bottom:20px">
        <div style="font-size:13px;font-weight:700;color:var(--ink);margin-bottom:12px">Per-File Summary</div>
        ${mkTable(byFile.cols||[],byFile.rows||[])}
      </div>`:''}
      <div style="font-size:13px;font-weight:700;color:var(--ink);margin-bottom:12px">Extracted Tables</div>
      <div class="db-grid">${tblList.map(t=>`
        <div class="db-tile">
          <div class="db-tile-name" title="${e(t.table)}">${e(t.table)}</div>
          <div class="db-tile-num">${(t.rows||0).toLocaleString()}</div>
          <div class="db-tile-lbl">${e(t.report_type||'report')} · ${t.columns||'?'} cols</div>
        </div>`).join('')}
      </div>
    </div>`;
  } else {
    /* Individual table tab */
    const tblName=_k6[i-1];
    const td=tblMap[tblName]||{cols:[],rows:[],total_rows:0};
    const displayName=tblName.replace(/^rpt_/,'').replace(/_t(\d+)$/,'').replace(/_/g,' ');
    $('content').innerHTML=`<div class="fade">
      <div class="pg-hd">
        <div class="pg-bc">DOCX REPORTS · ${e(tblName)}</div>
        <div class="pg-t">${e(displayName)}</div>
        <div class="pg-d">Showing up to 200 rows from <code>${e(tblName)}</code>. Total rows in database: <strong>${(td.total_rows||0).toLocaleString()}</strong>.</div>
      </div>
      ${mkTable(td.cols||[],td.rows||[])}
    </div>`;
  }
}

/* ══ TABLE RENDERER ══ */
function mkTable(cols,rows,sc){
  if(!rows.length)return emptyState('No records found.');
  const nums=rows.map(r=>safeNum(r[sc])).filter(v=>v>0).sort((a,b)=>b-a);
  const p33=nums.length?nums[Math.floor(nums.length*.33)]:Infinity;
  const p66=nums.length?nums[Math.floor(nums.length*.66)]:Infinity;
  const html=rows.map(row=>`<tr>${cols.map(c=>{
    const v=row[c];
    if(c==='line_direction')return`<td>${dirTag(v)}</td>`;
    const isN=!isNaN(parseFloat(v))&&v!==''&&v!=null&&v!=='';
    if(c===sc&&isN){
      const fv=safeNum(v);
      const cls=fv>=p33?'sev-crit':fv>=p66?'sev-high':fv>0?'sev-med':'sev-ok';
      return`<td class="${cls}">${e(fmtVal(v))}</td>`;
    }
    return`<td class="${isN?'mono':'txt'}">${e(fmtVal(v))}</td>`;
  }).join('')}</tr>`).join('');
  return`<div class="tbl-card">
    <div class="tbl-bar"><span class="tbl-cnt">${rows.length.toLocaleString()} records</span></div>
    <div class="tbl-scroll"><table>
      <thead><tr>${cols.map(c=>`<th>${e(LABELS[c]||c.replace(/_/g,' '))}</th>`).join('')}</tr></thead>
      <tbody>${html}</tbody>
    </table></div></div>`;
}
function dirTag(v){
  if(!v)return'<span class="tag tag-n">—</span>';
  const u=String(v).toUpperCase();
  if(u==='UP')return`<span class="tag tag-up">↑ UP</span>`;
  if(u==='DN'||u==='DOWN')return`<span class="tag tag-dn">↓ DN</span>`;
  if(u.includes('III'))return`<span class="tag tag-3rd">${e(v)}</span>`;
  if(u.includes('IV'))return`<span class="tag tag-4th">${e(v)}</span>`;
  return`<span class="tag tag-n">${e(v)}</span>`;
}
function loadHtml(msg='Loading…'){return`<div class="loading"><div class="spin"></div><span>${e(msg)}</span></div>`;}
function errBanner(msg){return`<div class="fade"><div class="alert err">
  <svg viewBox="0 0 16 16"><path d="M6.457 1.047c.659-1.234 2.427-1.234 3.086 0l6.082 11.378A1.75 1.75 0 0 1 14.082 15H1.918a1.75 1.75 0 0 1-1.543-2.575L6.457 1.047zM9 11H7V9h2v2zm0-3H7V5h2v3z"/></svg>
  <div class="alert-txt"><strong>Error:</strong> ${e(msg)}</div>
</div></div>`;}
function emptyState(msg){return`<div class="empty"><div class="empty-ico">📭</div>${e(msg)}</div>`;}

window.addEventListener('load',checkScript);
</script>
</body>
</html>"""

if __name__ == "__main__":
    print("="*55)
    print("  Railway TRC Analytics — v2.0")
    print("  Open: http://localhost:5000")
    print("="*55)
    app.run(debug=False, port=5000, threaded=True)