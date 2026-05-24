"""
docx_tqi_analytics.py
======================
Standalone CLI analytics runner for TQI data ingested by docx_tqi_pipeline.py.

Queries railway.db and prints reports for:
  1. Worst track sections by TQI (short, long, composite)
  2. Speed vs TQI correlation summary
  3. KM ranges repeated across multiple runs (chronic problem zones)
  4. Run-by-run comparison table
  5. Consecutive poor KM zones

Usage:
    python docx_tqi_analytics.py [db_path]

    Examples:
        python docx_tqi_analytics.py railway.db
        python docx_tqi_analytics.py           # defaults to railway.db
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

DB_PATH = "railway.db"


# ════════════════════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════════════════════

def _conn(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    return c


def _view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
        (view_name,)
    ).fetchone()
    return row is not None


def _tbl_exists(conn: sqlite3.Connection, tbl_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (tbl_name,)
    ).fetchone()
    return row is not None


def _has_col(conn: sqlite3.Connection, view_or_table: str, col: str) -> bool:
    try:
        cols = [
            r[1] for r in conn.execute(
                f'PRAGMA table_info("{view_or_table}")'
            )
        ]
        return col in cols
    except Exception:
        return False


def _sep(title: str = "", char: str = "═", width: int = 72) -> None:
    if title:
        pad  = max(0, width - len(title) - 4)
        left = pad // 2
        right = pad - left
        print(f"\n{char * left}  {title}  {char * right}")
    else:
        print(char * width)


def _print_df(df: pd.DataFrame, max_rows: int = 30) -> None:
    if df.empty:
        print("  (no data)")
        return
    pd.set_option("display.max_columns",  None)
    pd.set_option("display.max_rows",     max_rows)
    pd.set_option("display.width",        120)
    pd.set_option("display.float_format", lambda x: f"{x:.3f}")
    print(df.head(max_rows).to_string(index=False))
    if len(df) > max_rows:
        print(f"  … {len(df) - max_rows} more rows not shown.")


# ════════════════════════════════════════════════════════════════════════════
#  Analytics Queries
# ════════════════════════════════════════════════════════════════════════════

def q1_worst_tqi(conn: sqlite3.Connection) -> None:
    """Q1 — Worst 20% track KM ranges by TQI indices."""
    _sep("Q1 — Worst Track Sections by TQI")

    for col, label in [
        ("tqi_s", "Short-chord TQI (TQI-S)"),
        ("tqi_l", "Long-chord TQI  (TQI-L)"),
        ("tqi_c", "Composite TQI   (TQI-C)"),
    ]:
        view = "v_tqi_all" if _view_exists(conn, "v_tqi_all") else None
        if not view:
            print("  [WARN] v_tqi_all view not found. Run pipeline first.")
            break

        if not _has_col(conn, view, col):
            print(f"  [SKIP] Column '{col}' not present in data.")
            continue

        print(f"\n  ── {label} — Bottom 20 KM zones ──")
        try:
            sel_cols = ", ".join(
                c for c in ["km_from", "km_to", col, "section",
                             "direction", "trc_no", "run_no", "run_date",
                             "source_file"]
                if _has_col(conn, view, c)
            )
            df = pd.read_sql(
                f"""
                SELECT {sel_cols}
                FROM   {view}
                WHERE  {col} IS NOT NULL
                  AND  {col} != ''
                ORDER BY CAST({col} AS REAL) ASC
                LIMIT 20
                """,
                conn,
            )
            _print_df(df)
        except Exception as exc:
            print(f"  [ERROR] {exc}")


def q2_speed_vs_tqi(conn: sqlite3.Connection) -> None:
    """Q2 — Speed vs TQI correlation summary."""
    _sep("Q2 — Speed vs TQI Correlation")

    view = "v_tqi_all"
    if not _view_exists(conn, view):
        print("  [WARN] v_tqi_all view not found.")
        return

    if not _has_col(conn, view, "tqi_s") or not _has_col(conn, view, "spd"):
        print("  [SKIP] tqi_s or spd column not available.")
        return

    try:
        df = pd.read_sql(
            f"""
            SELECT
                CAST(spd    AS REAL) AS speed_kmph,
                CAST(tqi_s  AS REAL) AS tqi_s,
                CAST(tqi_l  AS REAL) AS tqi_l,
                CAST(tqi_c  AS REAL) AS tqi_c,
                section,
                direction
            FROM {view}
            WHERE spd   IS NOT NULL AND spd   != ''
              AND tqi_s IS NOT NULL AND tqi_s != ''
            """,
            conn,
        )

        if df.empty:
            print("  (no data with both speed and TQI)")
            return

        print(f"\n  Records with both speed & TQI data: {len(df)}")
        print("\n  Average TQI by speed bucket:")
        df["speed_bucket"] = pd.cut(
            df["speed_kmph"],
            bins=[0, 50, 75, 100, 130, 200],
            labels=["≤50", "51-75", "76-100", "101-130", ">130"],
        )
        summary = (
            df.groupby("speed_bucket", observed=True)
            .agg(
                records=("tqi_s", "count"),
                avg_tqi_s=("tqi_s", "mean"),
                avg_tqi_l=("tqi_l", "mean"),
                avg_tqi_c=("tqi_c", "mean"),
            )
            .reset_index()
        )
        _print_df(summary)

    except Exception as exc:
        print(f"  [ERROR] {exc}")


def q3_chronic_km_zones(conn: sqlite3.Connection) -> None:
    """Q3 — KM ranges that appear as poor across multiple runs."""
    _sep("Q3 — Chronic Problem KM Zones (3+ Runs)")

    view = "v_tqi_by_km"
    if not _view_exists(conn, view):
        print("  [WARN] v_tqi_by_km view not found.")
        return

    try:
        df = pd.read_sql(
            f"""
            SELECT *
            FROM   {view}
            WHERE  run_count >= 3
            ORDER BY avg_tqi_s ASC
            LIMIT 30
            """,
            conn,
        )
        if df.empty:
            print("  (no KM zones appear in 3+ separate runs yet)")
        else:
            _print_df(df)
    except Exception as exc:
        print(f"  [ERROR] {exc}")


def q4_run_comparison(conn: sqlite3.Connection) -> None:
    """Q4 — Run-by-run average TQI comparison."""
    _sep("Q4 — Run-by-Run TQI Comparison")

    view = "v_tqi_by_run"
    if not _view_exists(conn, view):
        print("  [WARN] v_tqi_by_run view not found.")
        return

    try:
        df = pd.read_sql(
            f"SELECT * FROM {view} ORDER BY run_date DESC, section",
            conn,
        )
        _print_df(df, max_rows=50)
    except Exception as exc:
        print(f"  [ERROR] {exc}")


def q5_consecutive_poor_km(
    conn: sqlite3.Connection,
    min_consecutive: int = 3,
    tqi_col: str = "tqi_s",
    threshold: float | None = None,
) -> None:
    """
    Q5 — Consecutive KM zones with poor TQI.

    Uses the v_tqi_all view to find runs of consecutive KM positions where
    TQI falls below a threshold (default: bottom 30th percentile).
    """
    _sep(f"Q5 — Consecutive Poor KM Zones (≥{min_consecutive} KMs)")

    view = "v_tqi_all"
    if not _view_exists(conn, view):
        print("  [WARN] v_tqi_all not found.")
        return

    if not _has_col(conn, view, tqi_col) or not _has_col(conn, view, "km_from"):
        print(f"  [SKIP] Need '{tqi_col}' and 'km_from' columns.")
        return

    try:
        df = pd.read_sql(
            f"""
            SELECT
                km_from,
                {tqi_col},
                section,
                direction,
                run_no,
                run_date
            FROM {view}
            WHERE km_from IS NOT NULL AND km_from != ''
              AND {tqi_col} IS NOT NULL AND {tqi_col} != ''
            """,
            conn,
        )

        if df.empty:
            print("  (no data)")
            return

        df["km_from"]  = pd.to_numeric(df["km_from"],  errors="coerce")
        df[tqi_col]    = pd.to_numeric(df[tqi_col],    errors="coerce")
        df = df.dropna(subset=["km_from", tqi_col])

        # Determine threshold
        if threshold is None:
            threshold = float(df[tqi_col].quantile(0.30))
        print(f"  TQI threshold (30th pct): {threshold:.3f}")

        poor = df[df[tqi_col] <= threshold].copy()

        sequences: list[dict] = []
        for (sec, dirn, rno, rdate), grp in poor.groupby(
            ["section", "direction", "run_no", "run_date"]
        ):
            kms = sorted(grp["km_from"].unique())
            if not kms:
                continue
            start, length = kms[0], 1
            for i in range(1, len(kms)):
                if kms[i] - kms[i - 1] <= 1.01:   # allow small float tolerance
                    length += 1
                else:
                    if length >= min_consecutive:
                        sequences.append({
                            "section":     sec,
                            "direction":   dirn,
                            "run_no":      rno,
                            "run_date":    rdate,
                            "from_km":     round(start, 3),
                            "to_km":       round(kms[i - 1], 3),
                            "km_count":    length,
                            f"avg_{tqi_col}": round(
                                grp[
                                    grp["km_from"].between(start, kms[i - 1])
                                ][tqi_col].mean(), 3
                            ),
                        })
                    start, length = kms[i], 1
            if length >= min_consecutive:
                sequences.append({
                    "section":     sec,
                    "direction":   dirn,
                    "run_no":      rno,
                    "run_date":    rdate,
                    "from_km":     round(start, 3),
                    "to_km":       round(kms[-1], 3),
                    "km_count":    length,
                    f"avg_{tqi_col}": round(
                        grp[grp["km_from"].between(start, kms[-1])][tqi_col].mean(), 3
                    ),
                })

        if not sequences:
            print(f"  (no sequences of ≥{min_consecutive} consecutive poor KMs found)")
        else:
            result = pd.DataFrame(sequences).sort_values(
                ["km_count", f"avg_{tqi_col}"], ascending=[False, True]
            )
            print(f"\n  Found {len(result)} consecutive poor-KM sequence(s):")
            _print_df(result)

    except Exception as exc:
        print(f"  [ERROR] {exc}")


def q6_repeated_defect_locations(conn: sqlite3.Connection) -> None:
    """Q6 — Locations where poor TQI recurs across multiple TRC runs."""
    _sep("Q6 — Repeated Poor TQI Locations Across Runs")

    view = "v_tqi_all"
    if not _view_exists(conn, view):
        print("  [WARN] v_tqi_all not found.")
        return

    needed = ["km_from", "tqi_s", "section", "run_no"]
    missing = [c for c in needed if not _has_col(conn, view, c)]
    if missing:
        print(f"  [SKIP] Missing columns: {missing}")
        return

    try:
        df = pd.read_sql(
            f"""
            SELECT km_from, km_to, section, direction,
                   run_no, tqi_s, run_date
            FROM {view}
            WHERE km_from IS NOT NULL AND km_from != ''
              AND tqi_s   IS NOT NULL AND tqi_s   != ''
            """,
            conn,
        )

        df["km_from"] = pd.to_numeric(df["km_from"], errors="coerce")
        df["tqi_s"]   = pd.to_numeric(df["tqi_s"],   errors="coerce")
        df = df.dropna(subset=["km_from", "tqi_s"])

        # Round KM to nearest 0.5 for grouping
        df["km_key"] = (df["km_from"] * 2).round() / 2

        grp = (
            df.groupby(["section", "direction", "km_key"])
            .agg(
                run_count=("run_no",  "nunique"),
                avg_tqi_s=("tqi_s",   "mean"),
                min_tqi_s=("tqi_s",   "min"),
                run_dates=("run_date", lambda x: ", ".join(sorted(set(str(v) for v in x)))),
            )
            .reset_index()
        )

        chronic = grp[grp["run_count"] >= 2].sort_values(
            ["run_count", "avg_tqi_s"], ascending=[False, True]
        )
        if chronic.empty:
            print("  (no KM location appears poor across 2+ runs yet)")
        else:
            chronic["avg_tqi_s"] = chronic["avg_tqi_s"].round(3)
            chronic["min_tqi_s"] = chronic["min_tqi_s"].round(3)
            _print_df(chronic)

    except Exception as exc:
        print(f"  [ERROR] {exc}")


def q7_processed_files_summary(conn: sqlite3.Connection) -> None:
    """Q7 — Summary of processed files in the registry."""
    _sep("Q7 — Processed Files Registry (docx_tqi)")

    if not _tbl_exists(conn, "processed_files"):
        print("  [WARN] processed_files registry not found.")
        return

    try:
        df = pd.read_sql(
            """
            SELECT
                source_filename,
                generated_table,
                processed_date,
                record_count,
                status
            FROM processed_files
            WHERE file_type = 'docx_tqi'
            ORDER BY processed_date DESC
            """,
            conn,
        )
        if df.empty:
            print("  (no docx_tqi files in registry — run pipeline first)")
        else:
            print(f"  Total docx_tqi entries: {len(df)}")
            _print_df(df, max_rows=50)
    except Exception as exc:
        print(f"  [ERROR] {exc}")


# ════════════════════════════════════════════════════════════════════════════
#  Example SQL Queries (printed as reference)
# ════════════════════════════════════════════════════════════════════════════

EXAMPLE_SQL = """
-- ── Example SQL Queries for railway.db TQI Data ──────────────────────────

-- 1. Worst 20 KMs by short-chord TQI:
SELECT km_from, km_to, tqi_s, section, direction, run_date
FROM v_tqi_all
WHERE tqi_s IS NOT NULL AND tqi_s != ''
ORDER BY CAST(tqi_s AS REAL) ASC
LIMIT 20;

-- 2. Average TQI per run (trend analysis):
SELECT section, direction, run_no, run_date,
       COUNT(*) AS km_count,
       AVG(CAST(tqi_s AS REAL)) AS avg_tqi_s,
       MIN(CAST(tqi_s AS REAL)) AS min_tqi_s
FROM v_tqi_all
GROUP BY section, direction, run_no, run_date
ORDER BY run_date DESC;

-- 3. Chronic problem zones (same KM poor in 3+ runs):
SELECT km_from, km_to, section, direction,
       run_count, avg_tqi_s, min_tqi_s
FROM v_tqi_by_km
WHERE run_count >= 3
ORDER BY avg_tqi_s ASC;

-- 4. Speed vs defect — KMs with low TQI under high-speed operations:
SELECT km_from, km_to, spd, tqi_s, section, direction, run_date
FROM v_tqi_all
WHERE CAST(spd AS REAL) > 100
  AND CAST(tqi_s AS REAL) < 50
ORDER BY CAST(tqi_s AS REAL) ASC
LIMIT 30;

-- 5. Which source files contributed each table:
SELECT source_filename, generated_table, record_count, processed_date, status
FROM processed_files
WHERE file_type = 'docx_tqi'
ORDER BY processed_date DESC;

-- 6. Consecutive poor TQI (manual version using window functions):
WITH ranked AS (
    SELECT km_from, tqi_s, section, direction, run_no,
           ROW_NUMBER() OVER (PARTITION BY section, direction, run_no ORDER BY CAST(km_from AS REAL)) AS rn,
           CAST(km_from AS REAL) - ROW_NUMBER() OVER (PARTITION BY section, direction, run_no ORDER BY CAST(km_from AS REAL)) AS grp
    FROM v_tqi_all
    WHERE CAST(tqi_s AS REAL) < 60
)
SELECT section, direction, run_no,
       MIN(km_from) AS from_km, MAX(km_from) AS to_km,
       COUNT(*) AS consecutive_kms,
       AVG(CAST(tqi_s AS REAL)) AS avg_tqi_s
FROM ranked
GROUP BY section, direction, run_no, grp
HAVING COUNT(*) >= 3
ORDER BY consecutive_kms DESC, avg_tqi_s ASC;

-- 7. All available TQI tables and their sizes:
SELECT generated_table, record_count, source_filename, processed_date
FROM processed_files
WHERE file_type = 'docx_tqi' AND status = 'ok'
ORDER BY record_count DESC;
"""


def print_example_sql() -> None:
    _sep("Example SQL Queries")
    print(EXAMPLE_SQL)


# ════════════════════════════════════════════════════════════════════════════
#  Main
# ════════════════════════════════════════════════════════════════════════════

def run_all_analytics(db_path: str = DB_PATH) -> None:
    """Run all 7 analytics queries against the database."""
    if not Path(db_path).exists():
        print(f"[FATAL] Database not found: {db_path}")
        print("        Run: python docx_tqi_pipeline.py <folder_path> first.")
        sys.exit(1)

    print()
    _sep("RAILWAY TQI ANALYTICS REPORT", char="═", width=72)
    print(f"  Database: {db_path}")
    print(f"  Generated by: docx_tqi_analytics.py")
    _sep(char="─")

    conn = _conn(db_path)
    try:
        q7_processed_files_summary(conn)
        q1_worst_tqi(conn)
        q2_speed_vs_tqi(conn)
        q3_chronic_km_zones(conn)
        q4_run_comparison(conn)
        q5_consecutive_poor_km(conn)
        q6_repeated_defect_locations(conn)
        print_example_sql()
    finally:
        conn.close()

    _sep(char="═")
    print("  Report complete.")
    _sep(char="═")


if __name__ == "__main__":
    _db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    run_all_analytics(_db)
