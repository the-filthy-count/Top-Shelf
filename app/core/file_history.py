"""Durable file identities and append-only observations, independent of queue rows."""
import json
import uuid


def install(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS file_assets (
        id TEXT PRIMARY KEY, queue_table TEXT NOT NULL, queue_id INTEGER NOT NULL,
        original_path TEXT NOT NULL, original_known INTEGER NOT NULL,
        current_path TEXT NOT NULL, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(queue_table, queue_id))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS file_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, file_id TEXT NOT NULL,
        kind TEXT NOT NULL, old_path TEXT, new_path TEXT, detail TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    for action in ('UPDATE', 'DELETE'):
        conn.execute(f"""CREATE TRIGGER IF NOT EXISTS file_events_no_{action.lower()}
            BEFORE {action} ON file_events BEGIN
            SELECT RAISE(ABORT, 'File history is append-only'); END""")
    conn.execute('CREATE INDEX IF NOT EXISTS file_events_asset ON file_events(file_id,id)')
    conn.execute('''CREATE TABLE IF NOT EXISTS file_operations (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, source TEXT NOT NULL,
        destination TEXT, state TEXT NOT NULL, source_stat TEXT, error TEXT,
        started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TEXT)''')
    for table in ('processed_files', 'processed_movies'):
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():
            continue
        columns = {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}
        current = "COALESCE(NULLIF(destination, ''), filename)" if 'destination' in columns else 'filename'
        inserted = "COALESCE(NULLIF(NEW.destination, ''), NEW.filename)" if 'destination' in columns else 'NEW.filename'
        # A migrated path is an observation, never a fabricated original name.
        conn.execute(f'''INSERT OR IGNORE INTO file_assets
            (id,queue_table,queue_id,original_path,original_known,current_path)
            SELECT lower(hex(randomblob(16))), ?, id, filename, 0, {current} FROM {table}''', (table,))
        conn.execute(f'DROP TRIGGER IF EXISTS history_{table}_insert')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS history_{table}_insert AFTER INSERT ON {table}
            BEGIN
              INSERT OR IGNORE INTO file_assets(id,queue_table,queue_id,original_path,original_known,current_path)
              VALUES(lower(hex(randomblob(16))),'{table}',NEW.id,NEW.filename,1,{inserted});
              INSERT INTO file_events(file_id,kind,new_path)
              SELECT id,'registered',NEW.filename FROM file_assets WHERE queue_table='{table}' AND queue_id=NEW.id;
            END''')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS history_{table}_rename AFTER UPDATE OF filename ON {table}
            WHEN OLD.filename IS NOT NEW.filename BEGIN
              INSERT INTO file_events(file_id,kind,old_path,new_path)
              SELECT id,'queue.rename',OLD.filename,NEW.filename FROM file_assets WHERE queue_table='{table}' AND queue_id=NEW.id;
              UPDATE file_assets SET current_path=NEW.filename WHERE queue_table='{table}' AND queue_id=NEW.id;
            END''')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS history_{table}_status AFTER UPDATE OF status ON {table}
            WHEN OLD.status IS NOT NEW.status BEGIN
              INSERT INTO file_events(file_id,kind,new_path,detail)
              SELECT id,'queue.status',NEW.filename,NEW.status FROM file_assets WHERE queue_table='{table}' AND queue_id=NEW.id;
            END''')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS history_{table}_delete AFTER DELETE ON {table}
            BEGIN
              INSERT INTO file_events(file_id,kind,old_path)
              SELECT id,'queue.record_removed',OLD.filename FROM file_assets WHERE queue_table='{table}' AND queue_id=OLD.id;
            END''')

        if 'destination' in columns:
            conn.execute(f"""CREATE TRIGGER IF NOT EXISTS history_{table}_destination
                AFTER UPDATE OF destination ON {table}
                WHEN NEW.destination IS NOT NULL AND NEW.destination != ''
                     AND OLD.destination IS NOT NEW.destination BEGIN
                  INSERT INTO file_events(file_id,kind,old_path,new_path)
                  SELECT id,'file.destination',current_path,NEW.destination FROM file_assets
                  WHERE queue_table='{table}' AND queue_id=NEW.id;
                  UPDATE file_assets SET current_path=NEW.destination
                  WHERE queue_table='{table}' AND queue_id=NEW.id;
                END""")


def record_operation(connect, source, destination, stat):
    operation_id = uuid.uuid4().hex
    with connect() as conn:
        conn.execute('INSERT INTO file_operations(id,kind,source,destination,state,source_stat) VALUES(?,?,?,?,?,?)',
                     (operation_id,'move',str(source),str(destination),'prepared',json.dumps(stat)))
        conn.commit()
    return operation_id


def finish_operation(connect, operation_id, error=None):
    with connect() as conn:
        conn.execute('UPDATE file_operations SET state=?,error=?,finished_at=CURRENT_TIMESTAMP WHERE id=?',
                     ('failed' if error else 'complete',str(error) if error else None,operation_id))
        conn.commit()
