"""Matching observations belong to a file identity, never its current filename.

A provider response is reusable only for identical inputs. Failed attempts are
recorded but never cached as misses. No credentials are stored in this journal.
"""
import hashlib
import json
import time
from . import VERSION


def digest(value):
    def encode(item):
        return sorted(item, key=str) if isinstance(item, (set, frozenset)) else str(item)
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=encode).encode()).hexdigest()


def install(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS video_match_steps (
        file_id TEXT NOT NULL, stage TEXT NOT NULL, input_key TEXT NOT NULL,
        state TEXT NOT NULL, result_json TEXT, error TEXT,
        updated_at REAL NOT NULL, expires_at REAL NOT NULL DEFAULT 0,
        PRIMARY KEY(file_id,stage,input_key),
        FOREIGN KEY(file_id) REFERENCES file_assets(id) ON DELETE CASCADE)''')
    conn.execute('CREATE INDEX IF NOT EXISTS video_match_steps_expiry ON video_match_steps(expires_at)')
    # Existing connections do not universally enable SQLite foreign keys.
    conn.execute('''CREATE TRIGGER IF NOT EXISTS video_match_identity_deleted
        AFTER DELETE ON file_assets BEGIN
        DELETE FROM video_match_steps WHERE file_id=OLD.id; END''')


class Journal:
    def __init__(self, db, file_id):
        self.db, self.file_id = db, file_id

    def key(self, inputs):
        return digest([VERSION, inputs])

    def cached(self, stage, inputs):
        with self.db.get_conn() as conn:
            row = conn.execute('''SELECT result_json FROM video_match_steps
                WHERE file_id=? AND stage=? AND input_key=? AND state='complete'
                AND expires_at>?''', (self.file_id, stage, self.key(inputs), time.time())).fetchone()
        return json.loads(row[0]) if row else None

    def write(self, stage, inputs, *, result=None, error=None, ttl=86400):
        from app.core.queue_changes import assert_current
        now = time.time()
        with self.db.get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            assert_current(conn)
            if not conn.execute('SELECT 1 FROM file_assets WHERE id=?',(self.file_id,)).fetchone():
                from app.core.queue_changes import SupersededWork
                raise SupersededWork('Video identity was removed during matching')
            conn.execute('''INSERT INTO video_match_steps
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(file_id,stage,input_key) DO UPDATE SET
                state=excluded.state,result_json=excluded.result_json,error=excluded.error,
                updated_at=excluded.updated_at,expires_at=excluded.expires_at''',
                (self.file_id, stage, self.key(inputs), 'failed' if error else 'complete',
                 json.dumps(result, default=str), str(error)[:1000] if error else None,
                 now, now + ttl if not error else 0))
            # Bound observations per file/stage; retain current and recent evidence.
            conn.execute('''DELETE FROM video_match_steps WHERE file_id=? AND stage=?
                AND input_key NOT IN (SELECT input_key FROM video_match_steps
                WHERE file_id=? AND stage=? ORDER BY updated_at DESC LIMIT 8)''',
                (self.file_id, stage, self.file_id, stage))

    def call(self, stage, inputs, callback):
        cached = self.cached(stage, inputs)
        if cached is not None:
            return cached
        try:
            result = callback()
        except Exception as exc:
            self.write(stage, inputs, error=exc)
            raise
        self.write(stage, inputs, result=result)
        return result
