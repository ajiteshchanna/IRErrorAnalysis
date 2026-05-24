import sqlite3
conn = sqlite3.connect('railway.db')
tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]
print("TABLES:", len(tables))
for t in tables:
    cnt = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')  ]
    print(f"  TABLE={t}  ROWS={cnt}  COLS={cols}")
    if cnt > 0 and t != 'processed_files':
        s = conn.execute(f'SELECT * FROM "{t}" LIMIT 1').fetchone()
        print(f"    SAMPLE={s}")
conn.close()
print("DONE")
