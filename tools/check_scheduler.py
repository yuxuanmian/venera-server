import sqlite3

conn = sqlite3.connect(r"D:\project\tool\venera-project\venera-server\data\venera.db")
print("mirror", conn.execute("select count(*) from mirror").fetchone()[0])
print("jobs", conn.execute("select count(*) from jobs").fetchone()[0])
print("chunks", conn.execute("select count(*) from chunks").fetchone()[0])
print("jobs_by_state", conn.execute("select state, count(*) from jobs group by state").fetchall())
print("jobs_by_source", conn.execute("select source, count(*) from jobs group by source").fetchall())
print("chunk_job_counts", conn.execute("select chunk_id, count(*) from jobs group by chunk_id order by chunk_id").fetchall()[:5])
conn.close()
