"""
Railway Inspection Data Processing Pipeline — Final Version
============================================================
FIX: Garbage NULL columns after sheet_name are now eliminated via
     three defence layers:
     1. _is_skip_row — rejects any row where a cell is long + multiline
     2. clean_table  — drops columns with >80% NULL values
     3. process_folder — trims DataFrame to [data_cols + META_COLS] before
                         every write, so no extra column can ever reach SQLite

Column naming improvements (2026-05):
     - extract_metadata() now captures 14 distinct metadata fields
       (railway, division, section, line, trc_no, date, run_no, route,
        rt_code, file_name, start_km, source_file, sheet_name,
        processed_time).
     - _build_multilevel_columns() delegates to flatten_multilevel_headers()
       from column_utils, producing names like loc_km / loc_meter instead
       of merged concatenations.
     - clean_table() uses clean_column_name() for consistent snake_case.

Usage:
  python railway_pipeline.py <folder_path> [db_path]
"""

import re
import sqlite3
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

from column_utils import (
    clean_column_name,
    flatten_multilevel_headers,
    EXTENDED_COLUMN_ALIASES,
)

DB_PATH          = "railway.db"
BLOCK_TRIGGER    = "EXCEPTION REPORT"
HEADER_SCAN_ROWS = 15          # slightly wider scan for richer headers
LOG_FORMAT       = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger(__name__)

# ── Metadata columns appended to every row — ORDER MATTERS ─────────────────
# New clean fields (one column per header field, queryable individually).
# Deprecated merged fields (section_name, line_direction, km_range, etc.)
# have been removed.  Analytics views handle backward-compat if needed.
META_COLS = [
    # Report-header metadata
    "trc_no",
    "run_date",         # renamed from 'date' for consistency with TQI pipeline
    "run_no",
    "route",
    "rt_code",
    "file_name",
    # Geography metadata
    "railway",
    "division",
    "section",
    "start_km",
    "line",
    "section_spd",      # section speed in KMPH
    # System metadata
    "source_file",
    "sheet_name",
    "processed_time",
]

# LEGACY: grouped table routing — kept for backward compatibility, not used by new pipeline
# DATASET_TYPE_MAP = [
#     ("sod exception",           "sod_data"),
#     ("sod_exception",           "sod_data"),
#     ("lip flow",                "lip_flow_data"),
#     ("lip_flow",                "lip_flow_data"),
#     ("vertical wear",           "vertical_wear_data"),
#     ("vertical_wear",           "vertical_wear_data"),
#     ("lateral rail wear",       "lateral_wear_data"),
#     ("lateral wear",            "lateral_wear_data"),
#     ("lateral_wear",            "lateral_wear_data"),
#     ("sleeper defect",          "sleeper_defects_data"),
#     ("sleeper_defect",          "sleeper_defects_data"),
#     ("sleeper",                 "sleeper_defects_data"),
#     ("rail defect",             "rail_defects_data"),
#     ("rail_defect",             "rail_defects_data"),
#     ("fittings",                "fittings_data"),
#     ("fitting",                 "fittings_data"),
#     ("ballast and vegetation",  "ballast_vegetation_data"),
#     ("ballast & vegetation",    "ballast_vegetation_data"),
#     ("ballast_and_vegetation",  "ballast_vegetation_data"),
#     ("ballast",                 "ballast_data"),
#     ("vegetation",              "vegetation_data"),
#     ("sod",                     "sod_data"),
#     ("geometry",                "geometry_data"),
#     ("gauge",                   "gauge_data"),
#     ("squat",                   "squat_data"),
#     ("corrugation",             "corrugation_data"),
#     ("head check",              "head_check_data"),
# ]

_SKIP_PATTERNS = re.compile(
    r"reporting\s*date|itms\s*reports?|^reporting|^iTMS",
    re.IGNORECASE,
)


def _is_numeric(val) -> bool:
    try:
        float(str(val).strip())
        return True
    except (ValueError, TypeError):
        return False


def _cell_lines(val) -> list:
    if pd.isna(val):
        return []
    return [ln.strip() for ln in str(val).split("\n") if ln.strip()]


def _populated(row: pd.Series) -> list:
    return [str(v).strip() for v in row.values
            if pd.notna(v) and str(v).strip() not in ("", "nan")]


def _is_skip_row(pop: list) -> bool:
    if not pop:
        return True
    if all(_SKIP_PATTERNS.search(p) for p in pop):
        return True
    if len(pop) == 1 and len(pop[0]) > 50:
        return True
    # FIX-1: Any multiline + long cell = metadata, skip the row
    for cell in pop:
        if "\n" in cell and len(cell) > 40:
            return True
    return False


# ── Key-value metadata row detector ──────────────────────────────────────────
# Matches cells like:  "TRC NO : 9001"  /  "RAILWAY : Central"  /  "DATE : 20.04.2026"
# Does NOT match:     "PARAMETER"  /  "RECORDED VALUE"  /  "LOC KM"  (no colon+value)
_KV_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9\s\-/\.]*\s*:\s*\S",
    re.IGNORECASE,
)


def _is_metadata_row(pop: list) -> bool:
    """
    Return True when a populated row looks like key-value metadata rather than
    an actual column-header row.

    Detection rule:
        If ≥ 50% of populated cells match the pattern   "WORD(S) : VALUE"
        (a label followed by a colon and a non-blank value) then the row is
        treated as metadata and excluded from column-name building.

    Examples that return True (metadata rows — to be SKIPPED as headers):
        ["TRC NO : 9001",   "DATE : 20.04.2026",  "RUN NO : D"]
        ["RAILWAY : Central", "DIVISION : BSL"]
        ["SECTION : IGP-BSL", "START KM : 137",   "LINE : DN"]

    Examples that return False (real column-header rows — to be KEPT):
        ["PARAMETER", "THRESHOLD", "RECORDED VALUE", "LOC KM", "LOC METER"]
        ["S.NO", "KM FROM", "KM TO", "TQI-S", "TQI-L"]
        ["1", "-16.56", "137", "47.31"]          # numeric data row
    """
    if not pop:
        return False
    kv_count = sum(1 for cell in pop if _KV_RE.match(cell))
    # At least half the populated cells must match for the whole row to be metadata
    threshold = max(1, (len(pop) + 1) // 2)
    return kv_count >= threshold


# ════════════════════════════════════════════════════════════
#  1. detect_blocks
# ════════════════════════════════════════════════════════════
def detect_blocks(df: pd.DataFrame) -> list:
    trigger_rows = []
    for idx, row in df.iterrows():
        row_text = " ".join(str(v) for v in row.values if pd.notna(v))
        if BLOCK_TRIGGER.lower() in row_text.lower():
            trigger_rows.append(idx)
    if not trigger_rows:
        return []
    blocks = []
    for i, start in enumerate(trigger_rows):
        end = trigger_rows[i + 1] if i + 1 < len(trigger_rows) else df.index[-1] + 1
        block = df.loc[start: end - 1].reset_index(drop=True)
        blocks.append(block)
    log.info(f"  Detected {len(blocks)} block(s).")
    return blocks


# ════════════════════════════════════════════════════════════
#  2. extract_metadata
# ════════════════════════════════════════════════════════════

# ── Compiled patterns for header field extraction ──────────────────────────
_META_RE = {
    # Report-header fields
    # Handles both colon and hyphen separators: 'TRC NO : 9001' and 'TRC NO - 9001'
    "trc_no":      re.compile(
        r"TRC\s*NO\s*[:\-]\s*([A-Z0-9]+)", re.IGNORECASE),
    "run_date":    re.compile(
        r"\bDATE\s*[:\-]?\s*([\d]{1,2}[./\-][\d]{1,2}[./\-][\d]{2,4})",
        re.IGNORECASE),
    "run_no":      re.compile(
        r"RUN\s*NO\.?\s*[:\-]?\s*([A-Z0-9\-/]+)", re.IGNORECASE),
    "route":       re.compile(
        r"ROUTE\s*[:\-]?\s*([^\s|\n]+)", re.IGNORECASE),
    "rt_code":     re.compile(
        r"RT[\-\s]?CODE\s*[:\-]?\s*([^\s|\n]+)", re.IGNORECASE),
    "file_name":   re.compile(
        r"FILE\s*NAME\s*[:\-]?\s*([^\s|\n]+)", re.IGNORECASE),
    # Geography fields
    "railway":     re.compile(
        r"RAILWAY\s*[:\-]?\s*([^|\n]+?)(?:\s*\||\s*$)", re.IGNORECASE),
    "division":    re.compile(
        r"DIVISION\s*[:\-]?\s*([^|\n]+?)(?:\s*\||\s*$)", re.IGNORECASE),
    "section":     re.compile(
        r"SECTION\s*[:\-]?\s*([^|\n]+?)(?:\s*\||\s*$)", re.IGNORECASE),
    "start_km":    re.compile(
        r"START\s*KM\s*[:\-]?\s*([\d.]+)", re.IGNORECASE),
    "line":        re.compile(
        r"\bLINE\s*[:\-]?\s*(UP|DN|DOWN|IInd|IIIrd|IVth|3rd|4th|[A-Z]+)\b",
        re.IGNORECASE),
    # Section speed — appears in paragraph headers
    "section_spd": re.compile(
        r"SECTION\s*SPD\s*(?:\(KMPH\))?\s*[:\-]?\s*(\d+)", re.IGNORECASE),
}


def extract_metadata(block: pd.DataFrame) -> dict:
    """
    Scan the first HEADER_SCAN_ROWS rows of an exception-report block and
    extract 12 distinct metadata fields using dedicated regex patterns.

    Returns a flat dict with one key per metadata field.
    No merged/combined fields; every key maps to a clean string value (or empty).
    """
    lines: list[str] = []
    for _, row in block.iloc[:HEADER_SCAN_ROWS].iterrows():
        for val in row.values:
            lines.extend(_cell_lines(val))
    full_text = " | ".join(lines)

    meta: dict[str, str] = {}
    for field, pattern in _META_RE.items():
        m = pattern.search(full_text)
        raw = m.group(1).strip() if m else ""
        # Strip trailing pipe characters, whitespace, or dash-only values
        v = raw.strip(" |\t")
        meta[field] = "" if (not v or re.match(r'^[\-\u2013\u2014]+$', v)) else v

    # Normalise date to YYYY-MM-DD
    if meta.get("run_date"):
        import re as _re
        m = _re.match(r"(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})", meta["run_date"])
        if m:
            d, mo, y = m.group(1), m.group(2), m.group(3)
            if len(y) == 2:
                y = "20" + y
            try:
                meta["run_date"] = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            except ValueError:
                pass

    # Sanitise line field — keep only the direction token
    if meta.get("line"):
        meta["line"] = meta["line"].upper()

    return meta


# ════════════════════════════════════════════════════════════
#  3. extract_table
# ════════════════════════════════════════════════════════════
def _build_multilevel_columns(header_rows: list, n_cols: int) -> list:
    """
    Thin wrapper — delegates to flatten_multilevel_headers() in column_utils.
    Produces names like 'loc_km', 'loc_meter', 'recorded_value'
    instead of raw concatenations like 'LOC_KM_METER'.
    """
    return flatten_multilevel_headers(header_rows, n_cols)


def _is_data_row(pop: list) -> bool:
    """
    Return True if a row looks like a data row rather than a column-header row.

    A row is classified as data when it passes any of these checks:
      1. The first cell is numeric (classic S.No / KM-from tables)
      2. ANY cell in the row is numeric  (exception-report tables whose first
         column is a parameter name like "AGC-M(-)" but later columns like
         "LOC KM" = 137 are numeric)
      3. The row has many cells and looks like values (contains numbers mixed
         with strings, or contains "mm" measurement strings)

    Column-header rows are typically all short text labels — no numbers at all.
    """
    if not pop:
        return False
    # Quick path: first cell is numeric
    if _is_numeric(pop[0]):
        return True
    # Any cell numeric = data row
    if any(_is_numeric(cell) for cell in pop):
        return True
    # Measurement strings: "-11.00 mm", "15.00 mm" etc. → data row
    _MM_RE = re.compile(r"[\d.]+\s*mm", re.IGNORECASE)
    if any(_MM_RE.search(cell) for cell in pop):
        return True
    return False


def _find_header_and_data(block: pd.DataFrame):
    """
    Scan a block (starting from row 1, after the EXCEPTION REPORT trigger row)
    to find the actual column-header rows and where data rows begin.

    Three classes of rows are distinguished:

      METADATA rows  — KEY : VALUE pairs, e.g. "TRC NO : 9001 | DATE : 20.04.2026"
                       → _is_metadata_row() = True  → SKIPPED
      HEADER rows    — Column labels, e.g. "PARAMETER | THRESHOLD | LOC KM"
                       → Not metadata, not data, not skip  → collected
      DATA rows      — Numeric or measurement values in at least one cell
                       → _is_data_row() = True  → marks data_start, loop stops

    Returns:
        header_rows : list of row value-lists  (the actual column-label rows)
        data_start  : int row-index of first data row, or None if not found
    """
    header_rows = []
    data_start  = None
    for i in range(1, len(block)):
        row = block.iloc[i]
        pop = _populated(row)
        if not pop:
            continue                          # blank row — skip
        if _is_skip_row(pop):
            continue                          # long banner / multiline / iTMS
        if _is_metadata_row(pop):
            continue                          # KEY : VALUE row — metadata, NOT a header
        if _is_data_row(pop):
            data_start = i
            break                             # first data row found
        header_rows.append(row.tolist())      # genuine column-label row
    return header_rows, data_start


def extract_table(block: pd.DataFrame):
    header_rows, data_start = _find_header_and_data(block)
    if not header_rows or data_start is None:
        log.warning("  Table header row not found — skipping block.")
        return None
    log.debug(f"  Header rows selected ({len(header_rows)}): {[str(r[:4]) for r in header_rows]}")
    n_cols    = len(block.columns)
    col_names = _build_multilevel_columns(header_rows, n_cols)
    data_df   = block.iloc[data_start:].reset_index(drop=True)
    while len(col_names) < len(data_df.columns):
        col_names.append(f"col_{len(col_names)}")
    data_df.columns = col_names[: len(data_df.columns)]
    # Keep rows that have at least one numeric cell (works for both S.No-type
    # tables AND exception-report tables where col 0 is a string parameter name)
    mask = data_df.apply(
        lambda row: any(_is_numeric(v) for v in row.values), axis=1
    )
    data_df = data_df[mask.values].reset_index(drop=True)
    return data_df if not data_df.empty else None


# ════════════════════════════════════════════════════════════
#  4. clean_table
# ════════════════════════════════════════════════════════════
def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and deduplicate an extracted table DataFrame.

    Steps:
      1. Drop fully-empty rows and columns.
      2. Drop columns whose names are col_N or Unnamed — these are positional
         placeholders that slipped through header detection.
      3. Apply clean_column_name() to every column — produces consistent
         snake_case names (e.g. 'recorded_value', 'loc_km').
      4. Drop columns that became empty strings after cleaning (true garbage).
      5. Deduplicate column names by appending _2, _3, …
      6. Drop columns with > 80% NULL values.
      7. Enforce that the first column is numeric (data rows only).
    """
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.dropna(how="all", axis=1)

    # Drop placeholder columns before name cleaning
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed|^col_\d+$", na=False)]

    # Apply smart column name cleaner
    df.columns = [clean_column_name(c) for c in df.columns]

    # Drop columns that reduced to empty string (garbage headers)
    df = df.loc[:, df.columns != ""]

    # Deduplicate column names
    seen: dict = {}
    new_cols: list = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols

    # Drop mostly-null columns (garbage from metadata leakage)
    null_ratio = df.isnull().mean()
    df = df.loc[:, null_ratio <= 0.8]

    # Keep only data rows — rows that have at least one numeric value.
    # This handles both:
    #   S.No-style tables  (col 0 = 1, 2, 3 ...)
    #   Exception-report tables (col 0 = "AGC-M(-)" but col 3/4 = 137, 47.31)
    # Pure-label rows (residual header rows that slipped through) have no numbers.
    if len(df.columns) > 0:
        mask = df.apply(
            lambda row: any(_is_numeric(v) for v in row.values), axis=1
        )
        df = df[mask.values].reset_index(drop=True)
    return df


# LEGACY: detect_dataset_type — kept for backward compatibility, not called by new pipeline
# def detect_dataset_type(metadata: dict, file_name: str = "") -> str:
#     text = (file_name + " " +
#             metadata.get("full_header_text", "") + " " +
#             metadata.get("defect", "")).lower()
#     for keyword, table_name in DATASET_TYPE_MAP:
#         if keyword in text:
#             return table_name
#     return "general_data"


# ════════════════════════════════════════════════════════════
#  5. NEW: _clean_table_name  — one file → one table
# ════════════════════════════════════════════════════════════
def _clean_table_name(filename: str) -> str:
    """Convert a filename (with or without extension) to a valid SQLite table name.

    Rules:
      - Strip file extension
      - Lowercase
      - Replace spaces, hyphens, dots and non-alphanumeric chars with '_'
      - Collapse consecutive underscores
      - Strip leading/trailing underscores
      - Prefix 't_' if name starts with a digit
      - Truncate to 60 characters
    """
    import os
    stem = os.path.splitext(filename)[0]
    name = stem.strip().lower()
    name = re.sub(r"[^a-z0-9]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "file_table"
    if name[0].isdigit():
        name = "t_" + name
    return name[:60]


# ════════════════════════════════════════════════════════════
#  5b. processed_files registry helpers
# ════════════════════════════════════════════════════════════
def _ensure_processed_files_table(conn: sqlite3.Connection):
    """Create the processed_files registry table and its indices if absent."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_filename TEXT    NOT NULL,
            generated_table TEXT    NOT NULL,
            file_type       TEXT,
            processed_date  TEXT,
            last_modified   TEXT,
            record_count    INTEGER DEFAULT 0,
            status          TEXT    DEFAULT 'ok',
            UNIQUE(source_filename, generated_table)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pf_filename "
        "ON processed_files(source_filename)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pf_table "
        "ON processed_files(generated_table)"
    )
    conn.commit()


def _register_processed_file(
    db_path: str,
    source_filename: str,
    generated_table: str,
    file_type: str,
    last_modified: str,
    record_count: int,
    status: str = "ok",
):
    """Upsert one row into the processed_files registry."""
    from datetime import datetime
    processed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(db_path) as conn:
        _ensure_processed_files_table(conn)
        conn.execute(
            """
            INSERT INTO processed_files
                (source_filename, generated_table, file_type,
                 processed_date, last_modified, record_count, status)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(source_filename, generated_table) DO UPDATE SET
                processed_date = excluded.processed_date,
                last_modified  = excluded.last_modified,
                record_count   = record_count + excluded.record_count,
                status         = excluded.status
            """,
            (
                source_filename, generated_table, file_type,
                processed_date, last_modified, record_count, status,
            ),
        )
        conn.commit()
    log.info(
        f"  [REGISTRY] processed_files updated: "
        f"'{source_filename}' → '{generated_table}' ({record_count} rows)"
    )


# ════════════════════════════════════════════════════════════
#  6. store_to_sql
# ════════════════════════════════════════════════════════════
def _table_columns(conn: sqlite3.Connection, table_name: str) -> list:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    if cur.fetchone() is None:
        return []
    return [r[1] for r in conn.execute(f"PRAGMA table_info('{table_name}')")]


def store_to_sql(df: pd.DataFrame, table_name: str, db_path: str = DB_PATH):
    try:
        with sqlite3.connect(db_path) as conn:
            existing = _table_columns(conn, table_name)
            if not existing:
                # First write — create the table
                df.to_sql(table_name, conn, if_exists="append", index=False)
            else:
                # Subsequent writes — only insert into already-existing columns
                common = [c for c in df.columns if c in existing]
                if not common:
                    log.warning(f"  No common columns for '{table_name}' — skipping.")
                    return
                df[common].dropna(axis=1, how="all").to_sql(
                    table_name, conn, if_exists="append", index=False)
        log.info(f"  Stored {len(df)} row(s) → '{table_name}'")
    except Exception as ex:
        log.error(f"  DB write error for '{table_name}': {ex}")


# ════════════════════════════════════════════════════════════
#  7. process_folder
# ════════════════════════════════════════════════════════════

def process_csv(file_path, db_path):
    tbl = _clean_table_name(file_path.name)
    log.info(f"\n{'='*60}")
    log.info(f"[FOUND CSV] {file_path.name}")
    log.info(f"  PATH : {file_path}")
    log.info(f"  → Generated table : {tbl}")
    variant = "Chord" if "Chord" in file_path.name else "Profile" if "Profile" in file_path.name else "Unknown"
    total_rows = 0
    try:
        last_mod = str(file_path.stat().st_mtime)
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            for chunk in pd.read_csv(
                file_path, sep=';', chunksize=100_000,
                encoding='latin1', low_memory=False
            ):
                chunk.columns = [
                    str(c).strip().lower()
                    .replace(' ', '_').replace('-', '_').replace(':', '')
                    for c in chunk.columns
                ]
                chunk['data_variant'] = variant
                chunk['source_file']  = file_path.name
                chunk.to_sql(tbl, conn, if_exists='append', index=False)
                total_rows += len(chunk)
        log.info(f"  → Inserted : {total_rows} rows")
        log.info(f"  → Updated railway.db")
        _register_processed_file(
            db_path, file_path.name, tbl, "csv", last_mod, total_rows
        )
    except Exception as e:
        log.error(f"  Failed CSV {file_path.name}: {e}")
        _register_processed_file(
            db_path, file_path.name, tbl, "csv", "", 0, "error"
        )


# ════════════════════════════════════════════════════════════
#  7a. _process_single_xlsx  (shared by native XLSX + DOCX-converted)
# ════════════════════════════════════════════════════════════

def _process_single_xlsx(
    file_path: Path,
    db_path: str,
    summary: dict,
    counters: dict,
    source_table_name: str = "",
    source_filename_for_registry: str = "",
    file_type_for_registry: str = "xlsx",
) -> int:
    """
    Run the full block-extraction pipeline on one XLSX file.
    source_table_name : pre-computed clean table name (from caller).
                        If empty, falls back to _clean_table_name(file_path.stem).
    source_filename_for_registry : original source file name (e.g. the .docx name)
                                   written into processed_files; defaults to file_path.name.
    Updates summary dict and counters dict in-place.
    Returns the number of rows inserted (0 = file was skipped).
    """
    tbl_base = source_table_name or _clean_table_name(file_path.stem)
    reg_name = source_filename_for_registry or file_path.name
    last_mod = str(file_path.stat().st_mtime) if file_path.exists() else ""

    engine = "openpyxl" if file_path.suffix.lower() == ".xlsx" else "xlrd"
    try:
        xl = pd.ExcelFile(file_path, engine=engine)
    except Exception as ex:
        log.error(f"  Cannot open '{file_path.name}': {ex}")
        log.warning(f"  SKIPPED: {file_path.name}  reason=failed to open")
        counters["skipped"] = counters.get("skipped", 0) + 1
        _register_processed_file(
            db_path, reg_name, tbl_base, file_type_for_registry,
            last_mod, 0, "error"
        )
        return 0

    file_rows = 0
    for sheet_name in xl.sheet_names:
        log.info(f"  Sheet: '{sheet_name}'")
        try:
            raw_df = xl.parse(sheet_name, header=None, dtype=str)
        except Exception as ex:
            log.error(f"  Cannot read sheet '{sheet_name}': {ex}")
            log.warning(f"  SKIPPED sheet '{sheet_name}'  reason=parse error")
            continue

        if raw_df.empty:
            log.info(f"  Sheet '{sheet_name}' is empty — skipping.")
            continue

        blocks = detect_blocks(raw_df)
        if not blocks:
            log.warning(
                f"  Sheet '{sheet_name}': no EXCEPTION REPORT blocks found "
                f"— skipping sheet."
            )
            continue

        for b_idx, block in enumerate(blocks):
            log.info(f"  Block {b_idx + 1}/{len(blocks)} ...")
            counters["total_blocks"] = counters.get("total_blocks", 0) + 1
            try:
                metadata = extract_metadata(block)
                log.info(
                    f"    railway='{metadata.get('railway')}'  "
                    f"division='{metadata.get('division')}'  "
                    f"section='{metadata.get('section')}'  "
                    f"line='{metadata.get('line')}'  "
                    f"trc='{metadata.get('trc_no')}'  "
                    f"date='{metadata.get('date')}'"
                )

                table_df = extract_table(block)
                if table_df is None or table_df.empty:
                    log.warning("    No usable table — skipping block.")
                    continue

                table_df = clean_table(table_df)
                if table_df.empty:
                    log.warning("    Empty after cleaning — skipping block.")
                    continue

                # ── Attach clean metadata columns (one field per header key) ──
                table_df["trc_no"]         = metadata.get("trc_no",      "") or None
                table_df["run_date"]       = metadata.get("run_date",    "") or None
                table_df["run_no"]         = metadata.get("run_no",      "") or None
                table_df["route"]          = metadata.get("route",       "") or None
                table_df["rt_code"]        = metadata.get("rt_code",     "") or None
                table_df["file_name"]      = metadata.get("file_name",   "") or None
                table_df["railway"]        = metadata.get("railway",     "") or None
                table_df["division"]       = metadata.get("division",    "") or None
                table_df["section"]        = metadata.get("section",     "") or None
                table_df["start_km"]       = metadata.get("start_km",    "") or None
                table_df["line"]           = metadata.get("line",        "") or None
                table_df["section_spd"]    = metadata.get("section_spd", "") or None
                table_df["source_file"]    = file_path.name
                table_df["sheet_name"]     = sheet_name
                table_df["processed_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Guarantee column order — data cols first, META_COLS last
                data_cols = [c for c in table_df.columns if c not in META_COLS]
                table_df  = table_df[data_cols + META_COLS]

                # NEW: use file-derived table name (one file → one table)
                tbl = tbl_base
                log.info(
                    f"    → table : '{tbl}'  rows: {len(table_df)}  "
                    f"cols: {len(data_cols)} data + {len(META_COLS)} meta"
                )
                store_to_sql(table_df, tbl, db_path)

                file_rows += len(table_df)
                counters["total_rows"] = counters.get("total_rows", 0) + len(table_df)
                summary[tbl] = summary.get(tbl, 0) + len(table_df)

            except Exception as ex:
                log.error(
                    f"    Block {b_idx + 1} failed: {ex}", exc_info=True
                )

    if file_rows > 0:
        log.info(f"  → Inserted   : {file_rows} row(s)")
        log.info(f"  → Updated railway.db")
        counters["ok"] = counters.get("ok", 0) + 1
        _register_processed_file(
            db_path, reg_name, tbl_base, file_type_for_registry,
            last_mod, file_rows, "ok"
        )
    else:
        log.warning(
            f"  SKIPPED: {file_path.name}  reason=no processable blocks found"
        )
        counters["skipped"] = counters.get("skipped", 0) + 1
        _register_processed_file(
            db_path, reg_name, tbl_base, file_type_for_registry,
            last_mod, 0, "skipped"
        )
    return file_rows


def process_folder(folder_path: str, db_path: str = DB_PATH):
    folder = Path(folder_path)

    # Enable WAL mode for better concurrent read performance
    with sqlite3.connect(db_path) as _wconn:
        _wconn.execute("PRAGMA journal_mode=WAL")
        _ensure_processed_files_table(_wconn)

    # ── Validate folder ───────────────────────────────────────────────────
    if not folder.exists():
        log.error(f"Folder does not exist: {folder_path}")
        return
    if not folder.is_dir():
        log.error(f"Path is not a directory: {folder_path}")
        return

    # ── Recursive discovery (files only) ─────────────────────────────────
    all_files = [f for f in folder.rglob("*") if f.is_file()]

    # Upfront discovery summary
    ext_counts: dict = {}
    for f in all_files:
        ext = f.suffix.lower() if f.suffix else "(no ext)"
        ext_counts[ext] = ext_counts.get(ext, 0) + 1

    log.info("=" * 60)
    log.info(f"SCANNING : {folder_path}")
    log.info(f"Total files found (recursive): {len(all_files)}")
    for ext, cnt in sorted(ext_counts.items()):
        log.info(f"  {ext:12s}: {cnt} file(s)")
    log.info("=" * 60)

    # ── Counters for final summary ────────────────────────────────────────
    total_blocks  = 0
    total_rows    = 0
    summary: dict = {}
    csv_ok        = 0
    docx_ok       = 0
    xlsx_ok       = 0
    xlsx_skipped  = 0

    # ── CSV ───────────────────────────────────────────────────────────────
    csv_files = [f for f in all_files if f.suffix.lower() == ".csv"]
    log.info(f"CSV files found : {len(csv_files)}")
    for f in csv_files:
        log.info(f"  -> {f}")
        process_csv(f, db_path)
        csv_ok += 1

    # ── DOCX → XLSX → Pipeline ────────────────────────────────────────────
    from docx_to_xlsx_converter import convert_docx_to_xlsx, is_conversion_current

    docx_all   = [f for f in all_files
                  if f.suffix.lower() == ".docx"
                  and not str(f).startswith(str(folder / "converted_xlsx"))]
    docx_temp  = [f for f in docx_all if f.name.startswith("~")]
    docx_files = [f for f in docx_all if not f.name.startswith("~")]

    converted_dir = folder / "converted_xlsx"
    converted_dir.mkdir(exist_ok=True)

    log.info(f"DOCX files found: {len(docx_files)}  "
             f"(skipped temp/lock: {len(docx_temp)})")
    for f in docx_temp:
        log.warning(f"  SKIPPED (temp/lock file): {f.name}")

    docx_counters: dict = {"ok": 0, "skipped": 0,
                            "total_blocks": 0, "total_rows": 0}

    for f in docx_files:
        log.info(f"\n{'='*60}")
        log.info(f"[FOUND DOCX] {f.name}")
        log.info(f"  PATH : {f}")
        log.info(f"  → Converting to XLSX")

        xlsx_out = converted_dir / (f.stem + "_converted.xlsx")

        if xlsx_out.exists() and is_conversion_current(f, xlsx_out):
            log.info(f"  → XLSX already current, reusing: {xlsx_out.name}")
        else:
            xlsx_out = convert_docx_to_xlsx(f, folder)
            if xlsx_out is None:
                log.warning(f"  → Conversion failed — skipping {f.name}")
                docx_counters["skipped"] += 1
                continue
            log.info(f"  → XLSX generated: {xlsx_out.name}")

        # Derive table name from the ORIGINAL .docx stem (not the _converted xlsx)
        docx_table_name = _clean_table_name(f.stem)
        log.info(f"  → Generated table: {docx_table_name}")
        log.info(f"  → Processing generated XLSX")
        rows = _process_single_xlsx(
            xlsx_out, db_path, summary, docx_counters,
            source_table_name=docx_table_name,
            source_filename_for_registry=f.name,
            file_type_for_registry="docx",
        )
        if rows > 0:
            log.info(f"  → Inserted      : {rows} rows")
            log.info(f"  → Updated railway.db")
            docx_ok += 1
        else:
            docx_ok += 1   # conversion succeeded even if 0 rows matched blocks

    total_blocks += docx_counters.get("total_blocks", 0)
    total_rows   += docx_counters.get("total_rows", 0)

    # ── Native XLSX / XLS ────────────────────────────────────────────────
    excel_exts = {".xlsx", ".xls"}
    excel_all  = [f for f in all_files
                  if f.suffix.lower() in excel_exts
                  and not str(f).startswith(str(converted_dir))]
    excel_temp = [f for f in excel_all if f.name.startswith("~")]
    xlsx_files = [f for f in excel_all if not f.name.startswith("~")]

    log.info(f"Excel files found: {len(xlsx_files)}  "
             f"(skipped temp/lock: {len(excel_temp)})")
    for f in excel_temp:
        log.warning(f"  SKIPPED (temp/lock file): {f.name}")

    if not xlsx_files:
        log.info(
            "No native Excel (.xlsx / .xls) files found under the specified path.\n"
            "  DOCX files were converted and processed above."
        )
    else:
        xlsx_counters: dict = {"ok": 0, "skipped": 0,
                               "total_blocks": 0, "total_rows": 0}
        for file_path in xlsx_files:
            log.info(f"\n{'='*60}")
            log.info(f"[FOUND XLSX] {file_path.name}")
            log.info(f"  PATH : {file_path}")
            xlsx_table_name = _clean_table_name(file_path.stem)
            log.info(f"  → Generated table: {xlsx_table_name}")
            log.info(f"  → Processing directly")
            rows = _process_single_xlsx(
                file_path, db_path, summary, xlsx_counters,
                source_table_name=xlsx_table_name,
                source_filename_for_registry=file_path.name,
                file_type_for_registry="xlsx",
            )
            if rows > 0:
                log.info(f"  → Inserted      : {rows} rows")
                log.info(f"  → Updated railway.db")
                xlsx_ok += 1
            else:
                xlsx_skipped += 1

        total_blocks += xlsx_counters.get("total_blocks", 0)
        total_rows   += xlsx_counters.get("total_rows", 0)

    # ── Final Summary ─────────────────────────────────────────────────────
    log.info(f"\n{'='*60}")
    log.info("PIPELINE COMPLETE")
    log.info(f"  CSV   files processed  : {csv_ok}")
    log.info(f"  DOCX  files found      : {len(docx_files)}")
    log.info(f"  DOCX  files processed  : {docx_ok}")
    log.info(f"  Excel files found      : {len(xlsx_files)}")
    log.info(f"  Excel files processed  : {xlsx_ok}")
    log.info(f"  Excel files skipped    : {xlsx_skipped}")
    log.info(f"  Total blocks processed : {total_blocks}")
    log.info(f"  Total rows inserted    : {total_rows}")
    if summary:
        log.info("  Tables written:")
        for tbl, rows in sorted(summary.items()):
            log.info(f"    {tbl:<42} {rows:>6} rows")
    log.info(f"  Database               : {db_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage:   python railway_pipeline.py <folder_path> [db_path]")
        print("Example: python railway_pipeline.py ./data railway.db")
        sys.exit(1)
    process_folder(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else DB_PATH)