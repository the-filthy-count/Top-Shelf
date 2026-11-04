"""Named durable commands for download, recovery and library resources.

The existing queue_changes tables are the authority. Subjects supply identities
for work which does not refer to an inbox file. No credentials or callbacks are
stored: handlers obtain current configuration when executing saved arguments.
"""
import functools
import hashlib
import inspect
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
import database as db
from app.core import queue_changes as changes

_active = ContextVar('domain_command_worker', default=False)
_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix='saved-command')


def _encode(value):
    if isinstance(value, Path):
        return {'__command_path__':str(value)}
    if isinstance(value, (list,tuple)):
        return [_encode(v) for v in value]
    if isinstance(value, dict):
        return {'__command_map__':{k:_encode(v) for k,v in value.items()}}
    if value is None or isinstance(value,(str,int,float,bool)):
        return value
    raise TypeError('Command arguments must be serializable')


def _decode(value):
    if isinstance(value,dict):
        if set(value)=={'__command_map__'}:
            return {k:_decode(v) for k,v in value['__command_map__'].items()}
        if set(value)=={'__command_response__'}:
            from starlette.responses import JSONResponse
            response=value['__command_response__']
            return JSONResponse(response['body'],status_code=response['status'])
        if value == {'__current_settings__':True}:
            return db.get_settings()
        if set(value)=={'__command_path__'}:
            return Path(value['__command_path__'])
        return {k:_decode(v) for k,v in value.items()}
    if isinstance(value,list):
        return [_decode(v) for v in value]
    return value


def _dispatch(subject,revision):
    db.close_thread_connection()
    try:
        return changes.dispatch(subject,revision)
    finally:
        db.close_thread_connection()


def command(action):
    def decorate(fn):
        signature=inspect.signature(fn)
        def handler(subject,payload):
            token=_active.set(True)
            try:
                result=fn(**_decode(payload['arguments']))
                from starlette.responses import JSONResponse
                if isinstance(result,JSONResponse):
                    return {'value':{'__command_response__':{'status':result.status_code,'body':json.loads(result.body)}},
                            'ok':result.status_code<400, 'error':json.loads(result.body).get('error') if result.status_code>=400 else None}
                envelope={'value':_encode(result)}
                if isinstance(result,dict) and (result.get('error') or result.get('ok') is False):
                    envelope.update(ok=False,error=result.get('error') or 'Action failed')
                return envelope
            finally:
                _active.reset(token)
        changes.register(action,handler)
        @functools.wraps(fn)
        def submit(*args,**kwargs):
            # Internal steps execute as part of the already persisted parent.
            if _active.get() or changes._current_owner():
                return fn(*args,**kwargs)
            bound=signature.bind(*args,**kwargs)
            arguments={key:({'__current_settings__':True} if key in ('s','settings') else _encode(value))
                       for key,value in bound.arguments.items()}
            digest=hashlib.sha256(json.dumps(arguments,sort_keys=True).encode()).hexdigest()
            subject='action:'+action+':'+digest
            with db.get_conn() as conn:
                conn.execute('BEGIN IMMEDIATE')
                label=action.removeprefix('service.').replace('_',' ').strip()
                target=next((str(v) for k,v in bound.arguments.items() if k not in ('s','settings') and isinstance(v,(str,int,Path))), '')
                conn.execute('INSERT OR IGNORE INTO command_subjects(id,label) VALUES(?,?)',(subject,label+(' · '+target if target else '')))
                current=conn.execute('SELECT revision,state FROM queue_changes WHERE file_id=?',(subject,)).fetchone()
                if current and current[1] == 'blocked':
                    raise ValueError('Interrupted action requires review before retrying')
                if current and current[1] in ('pending','running'):
                    raise ValueError('This action is already queued or running')
                revision=changes.request(conn,subject,action,{'arguments':arguments})
            result=_pool.submit(_dispatch,subject,revision).result()
            return _decode(result.get('value'))
        return submit
    return decorate


def drain():
    """Resume saved, not-yet-started commands; interrupted effects stay blocked."""
    with db.get_conn() as conn:
        rows=conn.execute("""SELECT c.file_id,c.revision FROM queue_changes c
            JOIN command_subjects s ON s.id=c.file_id
            WHERE c.state='pending' AND c.running_revision IS NULL ORDER BY c.updated_at""").fetchall()
    for row in rows:
        if row[0] and not _active.get():
            try:
                _dispatch(row[0],row[1])
            except Exception:
                import logging
                logging.getLogger(__name__).exception('Saved action failed: %s',row[0])


def retry(subject,revision):
    with db.get_conn() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row=conn.execute('SELECT * FROM queue_changes WHERE file_id=?',(subject,)).fetchone()
        if not row or row['revision']!=revision or row['state'] not in ('failed','blocked'):
            raise ValueError('Action changed; refresh first')
        new=changes.request(conn,subject,row['action'],json.loads(row['desired_json']),expected_revision=revision)
    _pool.submit(_dispatch,subject,new)
    return {'queued':True}


def execute_saved(subject,revision):
    if _active.get() or changes._current_owner():
        return changes.dispatch(subject,revision)
    return _pool.submit(_dispatch,subject,revision).result()


def shutdown():
    _pool.shutdown(wait=False,cancel_futures=True)
