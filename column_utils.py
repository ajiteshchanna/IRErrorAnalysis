"""
column_utils.py
===============
Shared column-name cleaning and multi-level header flattening utilities
for the Indian Railway ETL pipelines.

Imported by:
    railway_pipeline.py     (exception-report XLSX ingestion)
    docx_tqi_pipeline.py    (DOCX-native TQI ingestion)

Public API:
    clean_column_name(raw)                          -> str
    flatten_multilevel_headers(header_rows, n_cols) -> list[str]
    EXTENDED_COLUMN_ALIASES                         -> dict[str, str]
"""

from __future__ import annotations
import re
import pandas as pd


# ════════════════════════════════════════════════════════════════════════════
#  Garbage sentinel set
# ════════════════════════════════════════════════════════════════════════════

_GARBAGE = frozenset({"", "nan", "none", "null", "unnamed", "-", "–", "—"})


# ════════════════════════════════════════════════════════════════════════════
#  Extended canonical alias map
# ════════════════════════════════════════════════════════════════════════════

EXTENDED_COLUMN_ALIASES: dict[str, str] = {
    # ── Serial number variants ───────────────────────────────────────────
    "s.no":              "s_no",
    "s. no.":            "s_no",
    "s.no.":             "s_no",
    "sno":               "s_no",
    "sr.no":             "s_no",
    "sr. no":            "s_no",
    "sr":                "s_no",
    "serial":            "s_no",
    "slno":              "s_no",
    "sl.no":             "s_no",
    "sl no":             "s_no",

    # ── KM range ─────────────────────────────────────────────────────────
    "km from":           "km_from",
    "km_from":           "km_from",
    "from km":           "km_from",
    "from":              "km_from",
    # NOTE: "start km" intentionally NOT aliased to km_from here.
    # In exception-report context it is the START KM metadata field.
    # TQI tables use "km from" / "from km" aliases for the range column.
    "km to":             "km_to",
    "km_to":             "km_to",
    "to km":             "km_to",
    "to":                "km_to",
    "end km":            "km_to",

    # ── Location columns (exception reports) ─────────────────────────────
    "loc km":            "loc_km",
    "loc_km":            "loc_km",
    "location km":       "loc_km",
    "location_km":       "loc_km",
    "loc meter":         "loc_meter",
    "loc_meter":         "loc_meter",
    "location meter":    "loc_meter",
    "location_meter":    "loc_meter",
    "loc m":             "loc_meter",
    "km":                "km",

    # ── Report-header metadata columns ───────────────────────────────────
    "trc no":            "trc_no",
    "trc_no":            "trc_no",
    "trc number":        "trc_no",
    "run date":          "run_date",
    "run_date":          "run_date",
    "date":              "run_date",
    "run no":            "run_no",
    "run_no":            "run_no",
    "run number":        "run_no",
    "route":             "route",
    "rt code":           "rt_code",
    "rt_code":           "rt_code",
    "rt-code":           "rt_code",
    "file name":         "file_name",
    "file_name":         "file_name",
    "start km":          "start_km",
    "start_km":          "start_km",

    # ── Exception-report data columns ────────────────────────────────────
    "parameter":                "parameter",
    "threshold":                "threshold",
    "recorded value":           "recorded_value",
    "recorded_value":           "recorded_value",
    "value":                    "recorded_value",

    # ── Speed ─────────────────────────────────────────────────────────────
    "spd":                      "spd",
    "speed":                    "spd",
    "speed (kmph)":             "spd",
    "speed kmph":               "spd",
    "permissible":              "spd",
    # section_spd: unified alias (not section_spd_kmph) for queryability
    "section spd":              "section_spd",
    "section_spd":              "section_spd",
    "section spd (kmph)":       "section_spd",
    "section_spd_kmph":         "section_spd",
    "section spd kmph":         "section_spd",

    # ── Unevenness indices ────────────────────────────────────────────────
    "uni-1":             "uni_1",
    "uni_1":             "uni_1",
    "uni1":              "uni_1",
    "uni -1":            "uni_1",
    "unevenness-1":      "uni_1",
    "uni-2":             "uni_2",
    "uni_2":             "uni_2",
    "uni2":              "uni_2",
    "uni -2":            "uni_2",
    "unevenness-2":      "uni_2",

    # ── Alignment indices ─────────────────────────────────────────────────
    "ali-1":             "ali_1",
    "ali_1":             "ali_1",
    "ali1":              "ali_1",
    "ali -1":            "ali_1",
    "alignment-1":       "ali_1",
    "ali-2":             "ali_2",
    "ali_2":             "ali_2",
    "ali2":              "ali_2",
    "ali -2":            "ali_2",
    "alignment-2":       "ali_2",

    # ── TQI indices ───────────────────────────────────────────────────────
    "tqi-s":             "tqi_s",
    "tqi_s":             "tqi_s",
    "tqis":              "tqi_s",
    "tqi s":             "tqi_s",
    "tqi (s)":           "tqi_s",
    "tqi-l":             "tqi_l",
    "tqi_l":             "tqi_l",
    "tqil":              "tqi_l",
    "tqi l":             "tqi_l",
    "tqi (l)":           "tqi_l",
    "tqi-c":             "tqi_c",
    "tqi_c":             "tqi_c",
    "tqic":              "tqi_c",
    "tqi c":             "tqi_c",
    "tqi (c)":           "tqi_c",

    # ── Gauge / cross-level / twist / curvature ───────────────────────────
    "gauge":             "gauge",
    "gauge (mm)":        "gauge",
    "cross-level":       "cross_level",
    "cross level":       "cross_level",
    "cl":                "cross_level",
    "twist":             "twist",
    "twist (mm)":        "twist",
    "curvature":         "curvature",
    "curve":             "curvature",
}


# ════════════════════════════════════════════════════════════════════════════
#  clean_column_name()
# ════════════════════════════════════════════════════════════════════════════

def clean_column_name(raw: str) -> str:
    """
    Convert any raw column header string into a clean snake_case identifier.

    Rules (applied in order):
      1. Strip whitespace.
      2. Return "" for garbage values (nan, None, Unnamed, empty, "-" etc.).
      3. Lowercase.
      4. Remove bracketed unit suffixes cleanly: "(KMPH)" → "_kmph".
      5. Replace all non-alphanumeric characters (-, /, space, .) with "_".
      6. Collapse consecutive underscores.
      7. Strip leading/trailing underscores.
      8. Lookup in EXTENDED_COLUMN_ALIASES (both original-lower and snake form).
      9. If still empty after cleaning, return "".

    Examples:
        "PARAMETER"           → "parameter"
        "RECORDED VALUE"      → "recorded_value"
        "LOC KM"              → "loc_km"
        "RT-CODE"             → "rt_code"
        "START KM"            → "start_km"
        "SECTION SPD (KMPH)"  → "section_spd_kmph"
        "Unnamed: 3"          → ""
        "nan"                 → ""
        ""                    → ""
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""

    # Garbage check (before lowercasing)
    low = s.lower()
    if low in _GARBAGE:
        return ""
    # "Unnamed: N" pattern
    if re.match(r"^unnamed", low):
        return ""

    # Try alias on the original lowercase form first
    alias = EXTENDED_COLUMN_ALIASES.get(low)
    if alias:
        return alias

    # Convert bracketed units: "(KMPH)" → "_kmph"
    s = re.sub(r"\(([^)]+)\)", lambda m: "_" + m.group(1), s)

    # All non-alphanumeric → underscore
    s = re.sub(r"[^a-zA-Z0-9]", "_", s.lower())
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s).strip("_")

    if not s or s in _GARBAGE:
        return ""

    # Second alias lookup after snake conversion
    alias = EXTENDED_COLUMN_ALIASES.get(s)
    return alias if alias else s


# ════════════════════════════════════════════════════════════════════════════
#  flatten_multilevel_headers()
# ════════════════════════════════════════════════════════════════════════════

def flatten_multilevel_headers(
    header_rows: list,
    n_cols: int,
) -> list[str]:
    """
    Intelligently flatten multi-level column header rows into clean snake_case names.

    Args:
        header_rows : list of row-value lists (each row is a header level).
                      E.g. [["LOC", "LOC", "PARAM"], ["KM", "METER", ""]]
        n_cols      : number of columns to produce.

    Returns:
        List of n_cols clean column name strings.
        Falls back to "col_N" for positions where no valid name can be formed.

    Algorithm:
        1. Pad / trim every header row to exactly n_cols.
        2. Forward-fill parent cells across child columns (so "LOC" propagates
           to both "KM" and "METER" positions).
           Exception: the LAST header row is NOT forward-filled (leaf values).
        3. For each column position, collect the unique non-garbage parts
           top-to-bottom.  Consecutive repeated values are collapsed.
        4. Join with "_" and run through clean_column_name().
        5. If the result is empty, fall back to "col_N".
        6. Deduplicate: if a name appears twice, suffix _2, _3, …

    Examples:
        [["LOC", "LOC"], ["KM", "METER"]]         → ["loc_km", "loc_meter"]
        [["PARAMETER"]]                            → ["parameter"]
        [["SECTION SPD (KMPH)"]]                   → ["section_spd_kmph"]
        [["LOC", "LOC"], ["KM", "KM"]]             → ["loc_km", "loc_km_2"]
    """
    # ── Step 1: pad / trim every row ─────────────────────────────────────────
    padded: list[list] = []
    for row_vals in header_rows:
        row = list(row_vals)
        row = row[:n_cols] + [None] * max(0, n_cols - len(row))
        padded.append(row)

    # ── Step 2: forward-fill all but the last row ─────────────────────────────
    ff_rows: list[list] = []
    for i, row in enumerate(padded):
        sr = pd.Series(row, dtype=object).replace({"": None, "nan": None, "None": None})
        if i < len(padded) - 1:
            sr = sr.ffill()
        ff_rows.append(sr.tolist())

    # ── Step 3 & 4: build name per column ────────────────────────────────────
    result: list[str] = []
    for col_idx in range(n_cols):
        parts: list[str] = []
        prev: str | None = None
        for row in ff_rows:
            raw_val = row[col_idx] if col_idx < len(row) else None
            if raw_val is None:
                continue
            cell_str = str(raw_val).strip()
            # Skip multiline + long cells (metadata bleed-through)
            if "\n" in cell_str and len(cell_str) > 40:
                continue
            cleaned = clean_column_name(cell_str)
            if cleaned and cleaned != prev:
                parts.append(cleaned)
                prev = cleaned

        combined = "_".join(parts) if parts else ""
        # Final alias lookup on combined result
        alias = EXTENDED_COLUMN_ALIASES.get(combined, combined)
        result.append(alias if alias else f"col_{col_idx}")

    # ── Step 5: deduplicate ───────────────────────────────────────────────────
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for name in result:
        if name in seen:
            seen[name] += 1
            deduped.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            deduped.append(name)

    return deduped
