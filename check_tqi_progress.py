"""Quick mid-run DB state checker."""
import sqlite3
conn = sqlite3.connect('railway.db')

tbls = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tqi_%'"
).fetchall()]
print(f"TQI tables created so far: {len(tbls)}")
for t in sorted(tbls):
    cnt  = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')] 
    print(f"  {t}: {cnt} rows | cols={cols[:8]}")

try:
    reg = conn.execute(
        "SELECT source_filename, generated_table, record_count, status "
        "FROM processed_files WHERE file_type='docx_tqi' "
        "ORDER BY processed_date DESC LIMIT 30"
    ).fetchall()
    print(f"\ndocx_tqi registry entries: {len(reg)}")
    for r in reg:
        print(f"  [{r[3]}] {r[0][:50]} -> {r[1][:45]} ({r[2]} rows)")
except Exception as e:
    print(f"Registry error: {e}")

conn.close()
