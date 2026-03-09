import sqlite3

conn = sqlite3.connect("startup_events.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    business_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload_json TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS idea_stats (
    business_id TEXT PRIMARY KEY,
    submitted_count INTEGER DEFAULT 0,
    validation_requested_count INTEGER DEFAULT 0,
    validation_completed_count INTEGER DEFAULT 0,
    validation_failed_count INTEGER DEFAULT 0,
    feedback_viewed_count INTEGER DEFAULT 0,
    last_event_timestamp TEXT
)
""")

conn.commit()
conn.close()

print("Baza i tabele su uspešno kreirane.")