import sqlite3
import os

base = os.path.expandvars(r"%APPDATA%\com.github.wgh136\venera")
path = os.path.join(base, "network_favorite_cache.db")
conn = sqlite3.connect(path)
for table in ["comic_check_state", "favorite_items", "favorite_membership"]:
    try:
        n = conn.execute(f"select count(*) from {table}").fetchone()[0]
        print(table, n)
    except Exception as e:
        print(table, "ERR", e)
conn.close()
