"""Versioned desired state and execution history for stable queue file IDs.

The current revision is distinct from the revision owned by a worker. A late
completion can never replace a newer request. Filesystem recovery remains the
responsibility of the existing journalled filing/recovery implementations.
"""
import json
import time
import database as db


def install(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS command_subjects(id TEXT PRIMARY KEY,label TEXT NOT NULL)')
    conn.execute('''CREATE TABLE IF NOT EXISTS queue_changes (
        file_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, action TEXT NOT NULL,
        desired_json TEXT NOT NULL, state TEXT NOT NULL, running_revision INTEGER,
        result_json TEXT, error TEXT, updated_at REAL NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS queue_change_history (
        file_id TEXT NOT NULL, revision INTEGER NOT NULL, action TEXT NOT NULL,
        desired_json TEXT NOT NULL, state TEXT NOT NULL, result_json TEXT, error TEXT,
        created_at REAL NOT NULL, updated_at REAL NOT NULL,
        PRIMARY KEY(file_id, revision))''')

    columns = {row[1] for row in conn.execute('PRAGMA table_info(queue_changes)')}
    for name, declaration in (('available_at', 'REAL NOT NULL DEFAULT 0'),
                              ('attempts', 'INTEGER NOT NULL DEFAULT 0')):
        if name not in columns:
            conn.execute(f'ALTER TABLE queue_changes ADD COLUMN {name} {declaration}')


def request(conn, file_id, action, desired, *, approved=True, expected_revision=None):
    if not conn.execute('SELECT 1 FROM file_assets WHERE id=?', (file_id,)).fetchone() and not conn.execute('SELECT 1 FROM command_subjects WHERE id=?',(file_id,)).fetchone():
        raise ValueError('Unknown queue file identity')
    old = conn.execute('SELECT revision,state FROM queue_changes WHERE file_id=?', (file_id,)).fetchone()
    previous = old[0] if old else 0
    if expected_revision is not None and previous != expected_revision:
        raise ValueError('This file has newer changes; refresh before saving')
    revision, now = previous + 1, time.time()
    state = 'pending' if approved else 'draft'
    payload = json.dumps(desired, sort_keys=True)
    if old and old[1] in ('draft', 'pending', 'failed', 'blocked'):
        conn.execute("UPDATE queue_change_history SET state='superseded',updated_at=? WHERE file_id=? AND revision=?", (now,file_id,previous))
    conn.execute('''INSERT INTO queue_changes(file_id,revision,action,desired_json,state,updated_at)
        VALUES(?,?,?,?,?,?) ON CONFLICT(file_id) DO UPDATE SET revision=excluded.revision,
        action=excluded.action,desired_json=excluded.desired_json,state=excluded.state,
        result_json=NULL,error=NULL,available_at=0,attempts=0,updated_at=excluded.updated_at''', (file_id,revision,action,payload,state,now))
    conn.execute('INSERT INTO queue_change_history VALUES(?,?,?,?,?,NULL,NULL,?,?)', (file_id,revision,action,payload,state,now,now))
    _event(conn,(file_id,"change.requested",json.dumps({"revision":revision,"action":action,"desired":desired})))
    return revision


def _event(conn, values):
    if conn.execute('SELECT 1 FROM file_assets WHERE id=?',(values[0],)).fetchone():
        conn.execute('INSERT INTO file_events(file_id,kind,detail) VALUES(?,?,?)',values)


def claim(conn, file_id, revision):
    now = time.time()
    won = conn.execute('''UPDATE queue_changes SET running_revision=?,state='running',attempts=attempts+1,updated_at=?
        WHERE file_id=? AND revision=? AND state='pending' AND running_revision IS NULL AND available_at<=?''',
        (revision,now,file_id,revision,now)).rowcount == 1
    if won:
        conn.execute("UPDATE queue_change_history SET state='running',updated_at=? WHERE file_id=? AND revision=?", (now,file_id,revision))
    return won


def finish(conn, file_id, revision, state, result=None, error=None):
    if state not in ('completed','failed','blocked','cancelled'):
        raise ValueError('Invalid completion state')
    now, result_json = time.time(), json.dumps(result or {}, default=str)
    active = conn.execute("SELECT 1 FROM queue_change_history WHERE file_id=? AND revision=? AND state='running'",(file_id,revision)).fetchone()
    if not active:
        return
    _event(conn,
                 (file_id,'change.'+state,json.dumps({'revision':revision,'result':result or {},'error':error},default=str)))
    conn.execute('''UPDATE queue_change_history SET state=?,result_json=?,error=?,updated_at=?
        WHERE file_id=? AND revision=? AND state='running' ''', (state,result_json,error,now,file_id,revision))
    conn.execute('''UPDATE queue_changes SET running_revision=NULL,
        state=CASE WHEN revision=? THEN ? ELSE state END,
        result_json=CASE WHEN revision=? THEN ? ELSE result_json END,
        error=CASE WHEN revision=? THEN ? ELSE error END,updated_at=?
        WHERE file_id=? AND running_revision=?''', (revision,state,revision,result_json,revision,error,now,file_id,revision))


_handlers = {}


def register(action, handler):
    """Register code by name. Executable closures are never command payloads."""
    _handlers[action] = handler


def dispatch(file_id, revision):
    """Claim a saved revision and execute its saved input through a named handler."""
    with db.get_conn() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT action,desired_json FROM queue_changes WHERE file_id=? AND revision=?',
                           (file_id,revision)).fetchone()
        if not row:
            raise ValueError('Command was superseded')
        handler = _handlers.get(row[0])
        if handler is None:
            raise ValueError('No registered worker for '+row[0])
        if not claim(conn,file_id,revision):
            raise ValueError('Command is no longer ready')
        payload = json.loads(row[1])
    try:
        assert_actionable(file_id)
        with owner(file_id,revision):
            result = handler(file_id,payload) or {}
        failed = result.get('ok') is False or result.get('status') in ('error','no_dir')
        state = ('blocked' if result.get('status') == 'no_dir' else 'failed') if failed else 'completed'
        with db.get_conn() as conn:
            finish(conn,file_id,revision,state,result,result.get('error'))
        return result
    except BaseException as exc:
        with db.get_conn() as conn:
            finish(conn,file_id,revision,'failed',error=str(exc))
        raise


def execute(file_id, action, desired, callback):
    """Nested effects belong to their owner; top-level effects load saved inputs."""
    if _current_owner() and _current_owner()[0] == file_id:
        if action in ('relocate','recover'):
            annotate(file_id,last_effect={'action':action,'desired':desired})
        else:
            annotate(file_id, operation=action, **desired)
        return callback()
    if action not in _handlers:
        raise ValueError('No registered worker for '+action)
    with db.get_conn() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT state,running_revision FROM queue_changes WHERE file_id=?',(file_id,)).fetchone()
        if row and (row[0] == 'running' or (row[0] == 'pending' and action != 'recover') or row[1] is not None):
            raise ValueError('This file already has a pending change')
        revision = request(conn,file_id,action,desired)
    from app.core.action_commands import execute_saved
    return execute_saved(file_id,revision)


def list_changes(limit=100, offset=0):
    limit, offset = max(1,min(int(limit),250)), max(0,int(offset))
    with db.get_conn() as conn:
        rows = conn.execute("""SELECT c.*,a.id AS asset_id,a.current_path,a.original_path,
            a.queue_table,p.filename,p.status AS observed_state,p.phash,p.media_size_bytes,
            d.parsed_json,d.overrides_json FROM file_assets a
            JOIN processed_files p ON a.queue_table='processed_files' AND a.queue_id=p.id
            LEFT JOIN queue_changes c ON c.file_id=a.id
            LEFT JOIN queue_search_details d ON d.file_id=a.id
            WHERE (p.status NOT IN ('filed','removed') OR c.file_id IS NOT NULL)
            AND p.filename NOT LIKE '.%' AND p.filename NOT LIKE '%/.%'
            UNION ALL SELECT c.*,a.id,a.current_path,a.original_path,a.queue_table,p.filename,p.status,NULL,p.media_size_bytes,
            d.parsed_json,d.overrides_json FROM file_assets a
            JOIN processed_movies p ON a.queue_table='processed_movies' AND a.queue_id=p.id
            LEFT JOIN queue_changes c ON c.file_id=a.id
            LEFT JOIN queue_search_details d ON d.file_id=a.id
            WHERE (p.status NOT IN ('filed','removed') OR c.file_id IS NOT NULL)
            AND p.filename NOT LIKE '.%' AND p.filename NOT LIKE '%/.%'
            UNION ALL SELECT c.*,s.id,s.label,s.label,'command',s.label,c.state,NULL,NULL,NULL,NULL
            FROM command_subjects s JOIN queue_changes c ON c.file_id=s.id
            ORDER BY updated_at DESC,asset_id LIMIT ? OFFSET ?""",(limit,offset)).fetchall()
    return [{**dict(r),'file_id':r['asset_id'],'state':r['state'] or 'draft', 'revision':r['revision'] or 0,
             'desired':json.loads(r['desired_json'] or '{}'), 'result':json.loads(r['result_json'] or '{}'),
             'fields':{**json.loads(r['parsed_json'] or '{}'), **json.loads(r['overrides_json'] or '{}')}} for r in rows]


# A manual job already owns the outer change; nested filing enriches it rather
# than manufacturing a second command for the same file.
from contextvars import ContextVar
from contextlib import contextmanager
_owner = ContextVar('queue_change_owner', default=None)

def _current_owner():
    value = _owner.get()
    return value[:2] if value and value[2] == str(db.DB_PATH) else None


@contextmanager
def owner(file_id, revision):
    token = _owner.set((file_id, revision, str(db.DB_PATH)))
    try:
        yield
    finally:
        _owner.reset(token)


def annotate(file_id, **fields):
    active = _current_owner()
    if not active or active[0] != file_id:
        return
    with db.get_conn() as conn:
        row = conn.execute('SELECT desired_json FROM queue_change_history WHERE file_id=? AND revision=?',active).fetchone()
        if not row:
            return
        desired = {**json.loads(row[0]), **fields}
        encoded = json.dumps(desired, default=str)
        conn.execute('UPDATE queue_change_history SET desired_json=? WHERE file_id=? AND revision=?',(encoded,*active))
        conn.execute('UPDATE queue_changes SET desired_json=? WHERE file_id=? AND revision=?',(encoded,*active))


def schedule_processing(conn, file_id):
    row = conn.execute('SELECT action,state FROM queue_changes WHERE file_id=?',(file_id,)).fetchone()
    if row and row[1] == 'cancelled':
        return False
    if row and row[1] in ('pending','running','draft'):
        return row[0] == 'process'
    request(conn,file_id,'process',{'operation':'match_and_file'})
    return True


def start_processing(conn, file_id):
    if not schedule_processing(conn,file_id):
        return False
    row = conn.execute('SELECT revision FROM queue_changes WHERE file_id=?',(file_id,)).fetchone()
    if not claim(conn,file_id,row[0]):
        return False
    _owner.set((file_id,row[0],str(db.DB_PATH)))
    return True


def finish_processing(conn, file_id, outcome, error):
    active = _current_owner()
    if not active or active[0] != file_id:
        return
    row = conn.execute('SELECT attempts,revision FROM queue_changes WHERE file_id=?', (file_id,)).fetchone()
    attempts = row[0] if row else 0
    state = 'completed' if outcome in ('filed','complete','completed') else ('failed' if outcome in ('error','failed') else 'blocked')
    finish(conn,file_id,active[1],state,{'status':outcome},error or (None if state=='completed' else outcome))
    if row and row[1] == active[1] and (outcome == 'deferred' or (outcome == 'error' and attempts < 3)):
        delay = 300 if outcome == 'deferred' else min(900,60*max(1,attempts)**2)
        next_attempts = max(0,attempts-1) if outcome == 'deferred' else attempts
        revision = request(conn,file_id,'process',{'operation':'match_and_file'})
        conn.execute('UPDATE queue_changes SET attempts=?,available_at=? WHERE file_id=? AND revision=?',
                     (next_attempts,time.time()+delay,file_id,revision))
    _owner.set(None)


def recover(conn):
    """Interrupted effects need reconciliation, never blind automatic replay."""
    now = time.time()
    for row in conn.execute('SELECT * FROM queue_changes WHERE running_revision IS NOT NULL').fetchall():
        desired = json.loads(row['desired_json'])
        safe = row['action'] == 'process' and desired.get('operation') == 'match_and_file'
        state = 'pending' if safe else 'blocked'
        error = 'Interrupted before effects; retry scheduled' if safe else 'Interrupted; inspect the current file before retrying'
        conn.execute('UPDATE queue_change_history SET state=?,error=?,updated_at=? WHERE file_id=? AND revision=?',
                     (state,error,now,row['file_id'],row['running_revision']))
        conn.execute('UPDATE queue_changes SET state=CASE WHEN revision=running_revision THEN ? ELSE state END,running_revision=NULL,error=?,updated_at=? WHERE file_id=?',
                     (state,error,now,row['file_id']))
        if not safe:
            conn.execute("UPDATE processing_jobs SET state='failed',error=? WHERE file_id=?",(error,row['file_id']))
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='file_assets'").fetchone():
        # Upgrade old retry rows once. Subsequent scheduling reads only commands.
        for job in conn.execute("""SELECT j.* FROM processing_jobs j
                JOIN file_assets a ON a.id=j.file_id
                LEFT JOIN queue_changes c ON c.file_id=j.file_id
                WHERE c.file_id IS NULL AND j.state='pending'""").fetchall():
            request(conn,job['file_id'],'process',{'operation':'match_and_file'})
            conn.execute('UPDATE queue_changes SET attempts=?,available_at=? WHERE file_id=?',
                         (job['attempts'],job['available_at'],job['file_id']))
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='manual_queue_jobs'").fetchone():
        return
    conn.execute("UPDATE manual_queue_jobs SET status='failed',error='Interrupted; inspect the current file before retrying' WHERE status='running'")
    # Existing pending jobs gain a durable desired-state revision on upgrade.
    for row in conn.execute("SELECT id,job_type,payload_json,status,error FROM manual_queue_jobs WHERE status IN ('pending','failed') ORDER BY id").fetchall():
        payload = json.loads(row['payload_json'])
        if not payload.get('file_id') and payload.get('filename'):
            filename = payload['filename']
            table = 'processed_movies' if filename.startswith('JAV/') else 'processed_files'
            key = 'jav://' + filename[4:] if table == 'processed_movies' else filename
            asset = conn.execute(f"SELECT a.id FROM file_assets a JOIN {table} p ON a.queue_id=p.id AND a.queue_table=? WHERE p.filename=?",(table,key)).fetchone()
            if asset:
                payload['file_id'] = asset[0]
        if payload.get('file_id') and not payload.get('_change_revision'):
            payload['_change_revision'] = request(conn,payload['file_id'],row['job_type'],payload)
            conn.execute('UPDATE manual_queue_jobs SET payload_json=? WHERE id=?',(json.dumps(payload),row['id']))
            if row['status'] == 'failed':
                conn.execute("UPDATE queue_changes SET state='failed',error=? WHERE file_id=?",(row['error'],payload['file_id']))
                conn.execute("UPDATE queue_change_history SET state='failed',error=? WHERE file_id=? AND revision=?",(row['error'],payload['file_id'],payload['_change_revision']))


def relocate(filename, new_filename, moves):
    """Journal a queue pre-sort/tag operation, including its sidecars, as a unit."""
    from pathlib import Path
    from app.utils.fs import safe_move
    from download_safety import require_complete_download
    db.upsert_file(filename)
    asset = db.file_identity(filename)
    moves = [(Path(src),Path(dst)) for src,dst in moves]
    def apply():
        completed=[]
        try:
            require_complete_download(moves[0][0], shared_root=True)
            for src,dst in moves:
                if src.is_symlink() or dst.exists() or dst.is_symlink():
                    raise ValueError('Queue path changed; relocation cancelled')
                dst.parent.mkdir(parents=True,exist_ok=True)
                safe_move(src,dst,replace_existing=False)
                completed.append((src,dst))
            if not db.rename_file(filename,new_filename):
                raise ValueError('Queue identity could not be renamed')
            return {'status':'completed','destination':str(moves[0][1])}
        except BaseException:
            for src,dst in reversed(completed):
                if dst.exists() and not src.exists():
                    safe_move(dst,src,replace_existing=False)
            raise
    return execute(asset['id'],'relocate',{'filename':filename,'new_filename':new_filename,
        'destination':str(moves[0][1]),'moves':[[str(a),str(b)] for a,b in moves]},apply)


def annotate_current(**fields):
    active = _current_owner()
    if active and active[0]:
        annotate(active[0],**fields)


def adopt_processing(file_id):
    """Carry the claimed revision into async tasks and their to_thread calls."""
    with db.get_conn() as conn:
        row = conn.execute('SELECT running_revision FROM queue_changes WHERE file_id=?',(file_id,)).fetchone()
    if row and row[0] is not None:
        _owner.set((file_id,row[0],str(db.DB_PATH)))


def release_processing():
    _owner.set(None)


def assert_actionable(file_id):
    """Never replay a saved command onto a reused path after filing/removal."""
    if not file_id:
        return
    with db.get_conn() as conn:
        if conn.execute('SELECT 1 FROM command_subjects WHERE id=?',(file_id,)).fetchone():
            return
        asset=conn.execute('SELECT queue_table,queue_id FROM file_assets WHERE id=?',(file_id,)).fetchone()
        if not asset or asset[0] not in ('processed_files','processed_movies'):
            raise ValueError('File identity no longer exists')
        row=conn.execute(f'SELECT status FROM {asset[0]} WHERE id=?',(asset[1],)).fetchone()
        if not row or row[0] in ('filed','removed'):
            raise ValueError('This file was already filed or removed; review its history before making another change')


def _dispatch_relocation(file_id, payload):
    current = db.file_identity(payload['filename'])
    if not current or current['id'] != file_id:
        raise ValueError('Relocation source identity changed')
    return relocate(payload['filename'],payload['new_filename'],payload['moves'])


register('relocate',_dispatch_relocation)


def run_claimed(file_id, revision):
    if _current_owner() != (file_id,revision):
        raise ValueError('Worker does not own this command')
    with db.get_conn() as conn:
        row=conn.execute("SELECT action,desired_json FROM queue_change_history WHERE file_id=? AND revision=? AND state='running'",
                         (file_id,revision)).fetchone()
    if not row:
        raise ValueError('Command is no longer running')
    return _handlers[row[0]](file_id,json.loads(row[1]))
