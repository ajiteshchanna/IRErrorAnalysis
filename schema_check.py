import sqlite3
conn = sqlite3.connect('railway.db')
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print(f'Total tables: {len(tables)}')
print()
for t in tables:
    try:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')] 
        print(f'TABLE: {t}')
        print(f'  ROWS: {cnt}')
        print(f'  COLS ({len(cols)}): {cols}')
        if cnt > 0 and t != 'processed_files':
            sample = conn.execute(f'SELECT * FROM "{t}" LIMIT 1').fetchall()
            print(f'  SAMPLE: {sample}')
        print()
    except Exception as e:
        print(f'  ERROR: {e}')
conn.close()
