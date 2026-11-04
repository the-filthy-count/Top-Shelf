"""Keep queued manual filing attached to the selected file across renames."""
import database as db
import time
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
_active_filing = ContextVar("active_manual_filing", default=None)
from app.core.queue_recovery import resolve_target


def resolve_payload(payload):
    filename, kind = resolve_target(payload.get('file_id'), payload.get('filename') or '')
    if kind != 'scene':
        raise ValueError('Manual scene action refers to a movie queue entry')
    return filename


def bind_payload(payload):
    bound = dict(payload)
    filename = resolve_payload(bound)
    if not filename:
        raise ValueError('Manual filing requires a filename')
    bound['filename'] = filename
    if not bound.get('file_id'):
        jav = filename.startswith('JAV/')
        key = 'jav://' + filename[4:] if jav else filename
        table = 'processed_movies' if jav else 'processed_files'
        asset = db.file_identity(key, table)
        if asset is None:
            (db.upsert_movie if jav else db.upsert_file)(key)
            asset = db.file_identity(key, table)
        bound['file_id'] = asset['id']
    return bound


@contextmanager
def filing_activity(payload):
    """Claim the file atomically against recovery and automatic workers."""
    from app.core.recovery import _lock
    from app.core import jobs, queue_changes as changes
    own_revision = None
    with _lock:
        bound = bind_payload(payload)
        with db.get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            # Recheck using this transaction (no nested connection context).
            file_id = bound['file_id']
            asset = conn.execute('SELECT queue_table,queue_id FROM file_assets WHERE id=?', (file_id,)).fetchone()
            table = asset[0] if asset else ''
            if table not in ('processed_files', 'processed_movies'):
                raise ValueError('File identity no longer exists')
            row = conn.execute(f'SELECT filename,status FROM {table} WHERE id=?', (asset[1],)).fetchone()
            if not row or row[1] == 'removed':
                raise ValueError('This queue record was removed; refresh the queue')
            filename = row[0]
            if table == 'processed_movies':
                if not filename.startswith('jav://'):
                    raise ValueError('Manual scene action refers to a movie queue entry')
                filename = 'JAV/' + filename[6:]
            if _active_filing.get() == (file_id,str(db.DB_PATH)):
                raise ValueError('This file is already being processed; retry when it finishes')
            active = changes._current_owner()
            current = conn.execute('SELECT revision,running_revision FROM queue_changes WHERE file_id=?',(file_id,)).fetchone()
            if active and active[0] == file_id:
                if not current or current[1] != active[1]:
                    raise ValueError('Manual filing no longer owns this command')
            else:
                if current and current[1] is not None:
                    raise ValueError('This file is already being processed; retry when it finishes')
                own_revision = changes.request(conn,file_id,'manual_activity',bound)
                if not changes.claim(conn,file_id,own_revision):
                    raise ValueError('This file is already being processed; retry when it finishes')
            jobs.enqueue(conn, file_id)
            conn.execute("UPDATE processing_jobs SET state='running',phase='Manual filing',updated_at=? WHERE file_id=?",
                         (time.time(),file_id))
            conn.commit()
    outcome, error = 'complete', None
    token = _active_filing.set((file_id,str(db.DB_PATH)))
    try:
        with changes.owner(file_id,own_revision) if own_revision else nullcontext():
            yield filename
    except BaseException as exc:
        outcome, error = 'failed', str(exc) or 'Manual filing interrupted'
        raise
    finally:
        _active_filing.reset(token)
        with db.get_conn() as conn:
            if own_revision:
                changes.finish(conn,file_id,own_revision,'failed' if error else 'completed',error=error)
            # Failed manual jobs retain their chosen metadata in manual_queue_jobs.
            # Do not turn them into automatic fingerprint retries.
            conn.execute("""UPDATE processing_jobs SET state=?,phase=?,error=?,updated_at=?
                WHERE file_id=? AND state='running'""",
                (outcome, 'Manual filing ' + outcome, error, time.time(), file_id))
            conn.commit()
