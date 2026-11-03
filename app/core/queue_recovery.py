"""Queue removal and restoration keep files, identity and queue status together."""
from pathlib import Path

import database as db
from app.core.recovery import RecoveryStore, _signature
from download_safety import require_complete_download, PartialDownloadPending, VIDEO_EXTENSIONS

_TABLES = {'processed_files', 'processed_movies'}


def store():
    return RecoveryStore(db.file_operation_connection, Path(db.DB_PATH).resolve().parent / 'file-recovery')


def _root(filename, settings, queue_kind="scene"):
    parts = filename.replace('\\', '/').split('/')
    scope = parts[0].lower() if len(parts) > 1 else ''
    key = 'movies_source_dir' if queue_kind == 'movie' else {'jav': 'jav_download_watch_dir', 'vices': 'vice_download_watch_dir'}.get(scope, 'source_dir')
    raw = (settings.get(key) or '').strip()
    if not raw:
        raise ValueError('Original inbox is no longer configured')
    return Path(raw).expanduser().resolve()


def _check_context(conn, context, restoring=False):
    table = context['table']
    if table not in _TABLES:
        raise ValueError('Unknown queue type')
    root = _root(context['filename'], db.get_settings(), context.get('queue_kind', 'scene'))
    if not root.is_dir() or str(root) != context['root']:
        raise ValueError('Original inbox changed or is unavailable')
    for raw in context['paths']:
        path = Path(raw)
        if path.resolve() != path:
            raise ValueError('Queue path traverses a symlink')
        path.relative_to(root)
    row = conn.execute(f'SELECT id,filename,status,destination FROM {table} WHERE id=?', (context['record']['id'],)).fetchone()
    restored_duplicate = bool(restoring and row and context.get('duplicate_destination')
                              and row[2] == 'filed' and row[3] == context['duplicate_destination'])
    if row and (row[1] != context['record']['filename'] or (not restored_duplicate and row[2] != ('removed' if restoring else context['record']['status']))):
        raise ValueError('Queue record changed; refresh and retry')
    if not restoring and not context.get('allow_running'):
        active = conn.execute("SELECT 1 FROM processing_jobs WHERE file_id=? AND state='running'",
                              (context['file_id'],)).fetchone()
        if active:
            raise ValueError('This file is being processed; try again after it finishes')
    other = conn.execute(f'SELECT id FROM {table} WHERE filename=? AND id!=?',
                         (context['record']['filename'], context['record']['id'])).fetchone()
    if other:
        raise ValueError('Another queue record now uses this filename')


def archive(filename, video, root, table, queue_key, queue_kind="scene", expected_file_id=None, *, allow_running=False, duplicate_destination=None):
    if table not in _TABLES:
        raise ValueError('Unknown queue type')
    root, video = Path(root).resolve(), Path(video)
    try:
        require_complete_download(video, shared_root=video.parent == root)
    except PartialDownloadPending as exc:
        raise ValueError(str(exc)) from exc
    recovery = store()
    # Keep recovery media out of all configured source/library scan roots.
    settings = db.get_settings()
    candidates = [root]
    for key, value in settings.items():
        if key in {'source_dir', 'movies_source_dir', 'series_dir', 'features_dir', 'jav_dir',
                   'download_watch_dir', 'movie_download_watch_dir', 'vice_download_watch_dir',
                   'jav_download_watch_dir', 'vice_download_stage_dir', 'jav_download_stage_dir'} and value and str(value).strip():
            candidates.append(Path(value).expanduser().resolve())
    candidates.extend(Path(row['path']).expanduser().resolve() for row in db.get_directories() if row.get('path'))
    if any(recovery.root == candidate or candidate in recovery.root.parents for candidate in candidates):
        raise ValueError('Recovery storage must be outside configured media directories')
    if expected_file_id:
        current = db.file_identity(queue_key, table)
        with db.get_conn() as conn:
            row = conn.execute(f'SELECT status FROM {table} WHERE filename=?', (queue_key,)).fetchone()
        if not current or current['id'] != expected_file_id or not row or row[0] == 'removed':
            raise ValueError('File identity changed; refresh the queue')
    if table == 'processed_movies':
        db.upsert_movie(queue_key)
    else:
        db.upsert_file(queue_key)
    asset = db.file_identity(queue_key, table)
    paths = [video]
    shared_sidecars = any(p != video and p.stem == video.stem and p.suffix.lower() in VIDEO_EXTENSIONS
                          for p in video.parent.iterdir())
    for suffix in (() if shared_sidecars else ('.nfo', '-thumb.jpg', '-poster.jpg')):
        path = video.with_name(video.stem + suffix)
        if path.is_symlink():
            raise ValueError('A sidecar is a symlink; removal was cancelled')
        if path.is_file():
            paths.append(path)
    with db.get_conn() as conn:
        record = dict(conn.execute(f'SELECT * FROM {table} WHERE id=?', (asset['queue_id'],)).fetchone())
        job = conn.execute('SELECT state FROM processing_jobs WHERE file_id=?', (asset['id'],)).fetchone()
        if job and job[0] == 'running' and not allow_running:
            raise ValueError('This file is being processed; try again after it finishes')
    context = {'filename': filename, 'table': table, 'record': record, 'root': str(root),
               'paths': [str(path.resolve()) for path in paths], 'file_id': asset['id'], 'queue_kind': queue_kind,
               'source_stats': {str(path.resolve()): _signature(path) for path in paths}}

    if allow_running:
        context['allow_running'] = True
        context['restore_status'] = 'pending'
    if duplicate_destination:
        context['duplicate_destination'] = duplicate_destination

    def commit(conn, context):
        _check_context(conn, context)
        conn.execute(f"UPDATE {table} SET status='removed' WHERE id=?", (record['id'],))
        conn.execute("UPDATE processing_jobs SET state='cancelled',phase='Moved to recovery' WHERE file_id=?", (asset['id'],))

    with db.file_operation_connection() as conn:
        _check_context(conn, context)
    return recovery.archive(paths, asset['id'], context, commit, _check_context)


def restore(operation_id):
    import json
    from app.core.recovery import _row
    with db.file_operation_connection() as conn:
        context = json.loads(_row(conn, operation_id)['manifest'])['context']
    if context.get('kind') == 'vice':
        from app.core.vice_recovery import restore as restore_vice
        return restore_vice(operation_id)
    if context.get('kind') == 'library_video':
        from app.core.video_recovery import restore as restore_video
        return restore_video(operation_id)
    if context.get('kind') == 'library':
        from app.core.library_recovery import restore as restore_library
        return restore_library(operation_id)

    def validate(conn, context):
        _check_context(conn, context, restoring=True)

    def commit(conn, context):
        _check_context(conn, context, restoring=True)
        table, record = context['table'], context['record']
        existing = conn.execute(f'SELECT id FROM {table} WHERE id=?', (record['id'],)).fetchone()
        if existing:
            if context.get('duplicate_destination'):
                conn.execute(f'UPDATE {table} SET destination=? WHERE id=?', (record.get('destination'), record['id']))
            conn.execute(f'UPDATE {table} SET status=?,error=? WHERE id=?',
                         (context.get('restore_status', record['status']), record.get('error'), record['id']))
        else:
            allowed = {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}
            record = {**record, 'status': context.get('restore_status', record['status'])}
            columns = [column for column in record if column in allowed]
            conn.execute(f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join("?" for _ in columns)})',
                         [record[column] for column in columns])
        conn.execute("UPDATE processing_jobs SET state='complete',phase='Restored; ready for processing' WHERE file_id=?", (context['file_id'],))

    return store().restore(operation_id, validate, commit)


def resolve_target(file_id, filename, queue_kind='scene'):
    """An ID never falls back to a different file occupying an old filename."""
    if not file_id:
        return filename, 'movie' if queue_kind == 'movie' else 'scene'
    with db.get_conn() as conn:
        asset = conn.execute('SELECT queue_table,queue_id FROM file_assets WHERE id=?', (file_id,)).fetchone()
        if asset is None or asset[0] not in _TABLES:
            raise ValueError('File identity not found; refresh the queue')
        row = conn.execute(f'SELECT filename,status FROM {asset[0]} WHERE id=?', (asset[1],)).fetchone()
        if row is None or row[1] == 'removed':
            raise ValueError('This queue record was removed; refresh the queue')
        name = row[0]
        if asset[0] == 'processed_movies':
            return ('JAV/' + name[6:], 'scene') if name.startswith('jav://') else (name, 'movie')
        return name, 'scene'


def archive_pipeline(video, filename, *, movie=False, duplicate_destination=None):
    """Internal worker operation; the worker owns this file's active job."""
    name = 'JAV/' + filename[6:] if filename.startswith('jav://') else filename
    kind = 'movie' if movie and not name.startswith('JAV/') else 'scene'
    table = 'processed_movies' if movie or name.startswith('JAV/') else 'processed_files'
    key = 'jav://' + name[4:] if name.startswith('JAV/') else filename
    return archive(name, video, _root(name, db.get_settings(), kind), table, key, kind, allow_running=True, duplicate_destination=duplicate_destination)
