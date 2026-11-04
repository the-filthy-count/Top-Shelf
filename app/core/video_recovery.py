"""Recoverable removal of a single library video, sidecars and index rows."""
import uuid
from pathlib import Path

import database as db
from app.core.library_recovery import roots, _query, _insert
from app.core.queue_recovery import store
from app.core.recovery import _signature
from download_safety import require_complete_download, PartialDownloadPending, VIDEO_EXTENSIONS


def _validate_path(path):
    path = Path(path).expanduser().absolute()
    bases = [Path(row['path']).expanduser().resolve() for row in roots()]
    owners = [base for base in bases if base in path.parents and base.is_dir()]
    if path.resolve() != path or not owners or path.suffix.lower() not in VIDEO_EXTENSIONS:
        raise ValueError('Video must be under an available library root without symlinks')
    return path, max(owners, key=lambda base: len(base.parts))


def _records(conn, path):
    indexed = _query(conn, 'SELECT * FROM library_files WHERE destination=? ORDER BY id', (str(path),))
    processed = {}
    for table in ('processed_files', 'processed_movies'):
        processed[table] = _query(conn, f'SELECT * FROM {table} WHERE destination=? ORDER BY id', (str(path),))
    return indexed, processed


def indexed_path(row_id):
    with db.file_operation_connection() as conn:
        row = conn.execute('SELECT destination FROM library_files WHERE id=? AND is_removed=0', (int(row_id),)).fetchone()
    if not row or not row[0]:
        raise ValueError('Library file identity is no longer available; refresh and retry')
    return row[0]


def archive(raw, *, library_file_id=None):
    if library_file_id is not None:
        raw = indexed_path(library_file_id)
    video, root = _validate_path(raw)
    if not video.is_file():
        raise ValueError('Video is missing; no records were removed')
    try:
        require_complete_download(video, shared_root=video.parent == root)
    except PartialDownloadPending as exc:
        raise ValueError(str(exc)) from exc
    recovery = store()
    configured = [Path(row['path']).expanduser().resolve() for row in roots()]
    configured += [Path(value).expanduser().resolve() for key, value in db.get_settings().items()
                   if key.endswith('_dir') and isinstance(value, str) and value.strip()]
    if any(base == recovery.root or base in recovery.root.parents for base in configured):
        raise ValueError('Recovery storage must be outside configured media directories')
    paths = [video]
    siblings = list(video.parent.iterdir())
    shared = any(p != video and p.stem == video.stem and p.suffix.lower() in VIDEO_EXTENSIONS for p in siblings)
    if not shared:
        names = {video.stem + suffix for suffix in ('.nfo', '.srt', '.vtt', '.ass', '.jpg', '.jpeg', '.png', '.webp',
                                                   '-thumb.jpg', '-thumb.png', '-poster.jpg', '-poster.webp')}
        for path in siblings:
            if path.name not in names:
                continue
            if path.is_symlink():
                raise ValueError('Sidecar is a symlink; removal cancelled')
            if path.is_file():
                paths.append(path)
    with db.file_operation_connection() as conn:
        indexed, processed = _records(conn, video)
        if library_file_id is not None and not any(row['id'] == int(library_file_id) for row in indexed):
            raise ValueError('Library file identity changed before removal')
        asset = conn.execute('SELECT id FROM file_assets WHERE current_path=? ORDER BY first_seen LIMIT 1', (str(video),)).fetchone()
    context = {'kind': 'library_video', 'filename': video.name, 'video': str(video), 'root': str(root),
               'file_id': asset[0] if asset else 'video-' + uuid.uuid4().hex,
               'indexed': indexed, 'processed': processed,
               'source_stats': {str(path): _signature(path) for path in paths}}

    def validate(conn, ctx):
        _validate_path(video)
        if _records(conn, video) != (indexed, processed):
            raise ValueError('Video records changed; refresh and retry')
        for table, records in processed.items():
            for record in records:
                if conn.execute('''SELECT 1 FROM processing_jobs j JOIN file_assets a ON a.id=j.file_id
                    WHERE a.queue_table=? AND a.queue_id=? AND j.state='running' ''', (table, record['id'])).fetchone():
                    raise ValueError('Video is being processed; try again when processing finishes')
    def commit(conn, ctx):
        validate(conn, ctx)
        for row in indexed:
            conn.execute('DELETE FROM library_files WHERE id=?', (row['id'],))
        for table, records in processed.items():
            for row in records:
                conn.execute(f"UPDATE {table} SET status='removed' WHERE id=?", (row['id'],))
                conn.execute("""UPDATE processing_jobs SET state='cancelled',phase='Moved to recovery'
                    WHERE file_id IN (SELECT id FROM file_assets WHERE queue_table=? AND queue_id=?)""", (table, row['id']))
    return recovery.archive(paths, context['file_id'], context, commit, validate)


def restore(operation_id):
    def validate(conn, ctx):
        path, root = _validate_path(ctx['video'])
        if str(root) != ctx['root']:
            raise ValueError('Original library root changed')
        for row in ctx['indexed']:
            if conn.execute('SELECT 1 FROM library_files WHERE id=? OR destination=?', (row['id'], row['destination'])).fetchone():
                raise ValueError('Library record is already in use')
        for table, records in ctx['processed'].items():
            for row in records:
                expected = {**row, 'status': 'removed'}
                current = _query(conn, f'SELECT * FROM {table} WHERE id=?', (row['id'],))
                # Timestamps can change when the status is updated by a trigger.
                if not current or any(current[0].get(k) != v for k, v in expected.items() if k != 'updated_at'):
                    raise ValueError('Processing record changed or was removed; automatic restore is unsafe')
    def commit(conn, ctx):
        validate(conn, ctx)
        for row in ctx['indexed']:
            _insert(conn, 'library_files', row)
        for table, records in ctx['processed'].items():
            for row in records:
                conn.execute(f'UPDATE {table} SET status=? WHERE id=?', (row['status'], row['id']))
                conn.execute("""UPDATE processing_jobs SET state='complete',phase='Restored library video'
                    WHERE file_id IN (SELECT id FROM file_assets WHERE queue_table=? AND queue_id=?)""", (table, row['id']))
    return store().restore(operation_id, validate, commit)


from app.core.action_commands import command as _durable_command
archive = _durable_command('video_recovery.archive')(archive)
restore = _durable_command('video_recovery.restore')(restore)
