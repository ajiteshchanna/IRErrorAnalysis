"""inspect_db.py — quick database inspector for railway.db
Shows the processed_files registry (if present) and all table row/column counts.
"""
import sqlite3

DB = "railway.db"
conn = sqlite3.connect(DB)

tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]

print(f"\n=== railway.db  ({len(tables)} tables) ===\n")

# ── processed_files registry ────────────────────────────────────────────────
if "processed_files" in tables:
    print("── PROCESSED FILES REGISTRY ─────────────────────────────────────────")
    rows = conn.execute(
        "SELECT source_filename, generated_table, file_type, "
        "processed_date, record_count, status FROM processed_files "
        "ORDER BY processed_date DESC"
    ).fetchall()
    print(f"  {'SOURCE FILE':<45} {'TABLE':<40} {'TYPE':<6} {'ROWS':>8}  STATUS")
    print(f"  {'-'*45} {'-'*40} {'-'*6} {'-'*8}  {'------'}")
    for r in rows:
        src, tbl, ftype, pdate, cnt, status = r
        print(f"  {str(src):<45} {str(tbl):<40} {str(ftype):<6} {cnt:>8}  {status}")
    print(f"\n  Total tracked files: {len(rows)}\n")
else:
    print("  [INFO] No processed_files registry found (run pipeline first).\n")

# ── All tables ──────────────────────────────────────────────────────────────
print("── ALL TABLES ──────────────────────────────────────────────────────────")
for t in tables:
    try:
        cnt  = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')  ]
        print(f"\n  TABLE : {t}")
        print(f"  ROWS  : {cnt}")
        print(f"  COLS  : {cols}")
        if cnt > 0 and t != "processed_files":
            sample = conn.execute(f'SELECT * FROM "{t}" LIMIT 1').fetchall()
            print(f"  SAMPLE: {sample}")
    except Exception as e:
        print(f"\n  TABLE : {t}  [ERROR: {e}]")

conn.close()
print("\n=== DONE ===\n")
