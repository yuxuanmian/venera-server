import sqlite3
import os

base = os.path.expandvars(r"%APPDATA%\com.github.wgh136\venera")
for name in ["network_favorite_cache.db", "local_favorite.db", "history.db"]:
    path = os.path.join(base, name)
    print("====", name, "====")
    if not os.path.exists(path):
        print("not found")
        continue
    conn = sqlite3.connect(path)
    tables = conn.execute("select name, sql from sqlite_master where type='table'").fetchall()
    for t, sql in tables:
        print("TABLE", t)
        print(sql)
        try:
            rows = conn.execute(f"select * from {t} limit 3").fetchall()
            cols = [d[0] for d in conn.execute(f"select * from {t} limit 1").description]
            print("COLS", cols)
            for r in rows:
                print(r)
        except Exception as e:
            print("ERR", e)
    conn.close()
