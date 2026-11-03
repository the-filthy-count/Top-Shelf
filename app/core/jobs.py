"""Persistent per-file processing checkpoints and bounded retry scheduling."""
import time

MAX_ATTEMPTS = 3


def install(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS processing_jobs (
        file_id TEXT PRIMARY KEY, state TEXT NOT NULL, phase TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL, error TEXT)''')

    for table in ('processed_files', 'processed_movies'):
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            continue
        conn.execute(f"""CREATE TRIGGER IF NOT EXISTS jobs_{table}_removed
            AFTER DELETE ON {table} BEGIN
              UPDATE processing_jobs SET state='cancelled',phase='Queue record removed',
                updated_at=CAST(strftime('%s', 'now') AS REAL)
              WHERE file_id IN (SELECT id FROM file_assets WHERE queue_table='{table}' AND queue_id=OLD.id)
                AND state IN ('pending', 'running');
            END""")


def enqueue(conn, file_id):
    """An explicit new run may retry terminal jobs; never reset an active job."""
    conn.execute('''INSERT INTO processing_jobs(file_id,state,phase,updated_at)
        VALUES(?,'pending','Queued',?) ON CONFLICT(file_id) DO UPDATE SET
        state='pending',phase='Queued',attempts=0,available_at=0,
        updated_at=excluded.updated_at,error=NULL
        WHERE processing_jobs.state IN ('complete','failed')''',
        (file_id, time.time()))


def claim(conn, file_id):
    now = time.time()
    return conn.execute('''UPDATE processing_jobs SET state='running',phase='Reading media',
        attempts=attempts+1,updated_at=? WHERE file_id=? AND state='pending' AND available_at<=?''',
        (now, file_id, now)).rowcount == 1


def checkpoint(conn, file_id, phase):
    conn.execute('''UPDATE processing_jobs SET phase=?,updated_at=?
        WHERE file_id=? AND state='running' ''', (phase, time.time(), file_id))


def finish(conn, file_id, outcome, error=None):
    row = conn.execute("SELECT attempts FROM processing_jobs WHERE file_id=? AND state='running'", (file_id,)).fetchone()
    if not row:
        return
    attempts = row[0]
    deferred = outcome == 'deferred'
    retry = deferred or (outcome == 'error' and attempts < MAX_ATTEMPTS)
    if deferred:
        state, phase, delay = 'pending', 'Waiting for download', 300
        attempts = max(0, attempts - 1)
    elif retry:
        state, phase, delay = 'pending', 'Retry scheduled', min(900, 60 * max(1, attempts) ** 2)
    else:
        state, phase, delay = 'failed' if outcome == 'error' else 'complete', outcome, 0
    now = time.time()
    conn.execute('''UPDATE processing_jobs SET state=?,phase=?,available_at=?,updated_at=?,error=?,attempts=?
        WHERE file_id=? AND state='running' ''',
        (state, phase, now + delay, now, str(error) if error else None, attempts, file_id))


def recover(conn):
    """Call once after claiming the process's exclusive source-directory locks."""
    now = time.time()
    conn.execute('''UPDATE processing_jobs SET state=CASE WHEN attempts<? THEN 'pending' ELSE 'failed' END,
        phase='Interrupted by restart',error='Previous worker stopped before completion',
        available_at=?,updated_at=? WHERE state='running' ''', (MAX_ATTEMPTS, now + 60, now))
