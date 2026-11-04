"""Recover library entity trees together with their index records."""
import os
import stat
import uuid
from pathlib import Path

import database as db
from app.core.library_deletion import checked_folder
from app.core.queue_recovery import store
from app.core.recovery import _signature


def roots():
    settings = db.get_settings()
    values = [settings.get(key) for key in ('series_dir', 'features_dir', 'vices_dir', 'jav_dir')]
    values += [row.get('path') for row in db.get_directories()]
    return [{'path': str(value)} for value in values if value and str(value).strip()]


def _query(conn, sql, params):
    cursor = conn.execute(sql, params)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


def _files(conn, folder):
    # Literal prefix: '%' and '_' in names are not SQL wildcards.
    prefix = str(folder) + os.sep
    return _query(conn, 'SELECT * FROM library_files WHERE destination=? OR substr(destination,1,?)=? ORDER BY id',
                  (str(folder), len(prefix), prefix))


def _insert(conn, table, record):
    columns = list(record)
    conn.execute(f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join("?" for _ in columns)})', list(record.values()))


def archive(row_id):
    row = db.favourite_get(row_id)
    if not row:
        raise ValueError('Library entry no longer exists')
    if not (row.get('path') or '').strip():
        raise ValueError('Library entry has no folder')
    folder = checked_folder(row['path'], roots())
    recovery = store()
    configured = [Path(r['path']).expanduser().resolve() for r in roots()]
    settings = db.get_settings()
    configured += [Path(v).expanduser().resolve() for k, v in settings.items()
                   if k.endswith('_dir') and isinstance(v, str) and v.strip()]
    if any(base == recovery.root or base in recovery.root.parents for base in configured):
        raise ValueError('Recovery storage must be outside configured media directories')
    files, directories = [], []
    def failed(error):
        raise error
    for parent, dirs, names in os.walk(folder, followlinks=False, onerror=failed):
        directories.append(str(Path(parent)))
        for name in dirs + names:
            child = Path(parent)/name
            if child.is_symlink() or name.lower().endswith('.part'):
                raise ValueError('Folder contains a symlink or partial download; removal cancelled')
        for name in names:
            child = Path(parent)/name
            if not stat.S_ISREG(child.lstat().st_mode):
                raise ValueError('Folder contains a non-regular file')
            files.append(child)
    with db.file_operation_connection() as conn:
        record = _query(conn, 'SELECT * FROM favourite_entities WHERE id=?', (row_id,))[0]
        indexed = _files(conn, folder)
        biography = _query(conn, 'SELECT * FROM performer_bio_fields WHERE row_id=? ORDER BY field', (row_id,))
    context = {'kind': 'library', 'folder': str(folder), 'filename': row['folder_name'],
               'record': record, 'indexed': indexed, 'biography': biography, 'directories': directories,
               'file_id': 'library-' + uuid.uuid4().hex,
               'source_stats': {str(path): _signature(path) for path in files}}

    def validate(conn, ctx):
        checked_folder(ctx['folder'], roots())
        prefix = str(folder) + os.sep
        if conn.execute('SELECT 1 FROM favourite_entities WHERE id!=? AND substr(path,1,?)=?',
                        (row_id, len(prefix), prefix)).fetchone():
            raise ValueError('Folder contains another indexed library entity; remove it separately first')
        current = _query(conn, 'SELECT * FROM favourite_entities WHERE id=?', (row_id,))
        if (not current or current[0] != record or _files(conn, folder) != indexed
                or _query(conn, 'SELECT * FROM performer_bio_fields WHERE row_id=? ORDER BY field', (row_id,)) != biography):
            raise ValueError('Library records changed; refresh and retry')

    def commit(conn, ctx):
        validate(conn, ctx)
        # rmdir refuses any newly arrived file. Never recursively remove an
        # unexpected file that arrived while the bundle was being moved.
        for path in reversed(directories):
            Path(path).rmdir()
        for item in indexed:
            conn.execute('DELETE FROM library_files WHERE id=?', (item['id'],))
        conn.execute('DELETE FROM performer_bio_fields WHERE row_id=?', (row_id,))
        conn.execute('DELETE FROM favourite_entities WHERE id=?', (row_id,))

    try:
        return recovery.archive(files, context['file_id'], context, commit, validate)
    except Exception:
        # Preserve originally empty directories too after a rolled-back removal.
        for raw in directories:
            path = Path(raw)
            if path.resolve() == path:
                path.mkdir(parents=True, exist_ok=True)
        raise


def restore(operation_id):
    recovery = store()
    started = []
    def validate(conn, context):
        if context.get('kind') != 'library':
            raise ValueError('Not a library recovery record')
        folder = Path(context['folder'])
        bases = [Path(r['path']).expanduser().resolve() for r in roots()]
        if (folder.resolve() != folder or any(folder == base or folder in base.parents for base in bases)
                or not any(base in folder.parents and base.is_dir() for base in bases)):
            raise ValueError('Original library root changed or is unavailable')
        if folder.exists() or folder.is_symlink():
            raise ValueError('Original library folder is occupied')
        record = context['record']
        if conn.execute('SELECT 1 FROM favourite_entities WHERE id=? OR path=?', (record['id'], record['path'])).fetchone():
            raise ValueError('Library identity or path is already in use')
        if conn.execute('SELECT 1 FROM performer_bio_fields WHERE row_id=?', (record['id'],)).fetchone():
            raise ValueError('Performer biography records are already in use')
        for item in context['indexed']:
            if conn.execute('SELECT 1 FROM library_files WHERE id=? OR destination=?', (item['id'], item['destination'])).fetchone():
                raise ValueError('Library file record is already in use')
        started.append(context)
    def commit(conn, context):
        for raw in context['directories']:
            path = Path(raw)
            if path.resolve() != path:
                raise ValueError('Restore path traverses a symlink')
            path.mkdir(parents=True, exist_ok=True)
        _insert(conn, 'favourite_entities', context['record'])
        for field in context.get('biography', []):
            _insert(conn, 'performer_bio_fields', field)
        for item in context['indexed']:
            _insert(conn, 'library_files', item)
    try:
        return recovery.restore(operation_id, validate, commit)
    except Exception:
        # Only remove empty paths created after we verified the folder absent.
        # Occupied destinations rejected during validation are never touched.
        if started:
            for raw in reversed(started[0]['directories']):
                path = Path(raw)
                if path.resolve() == path:
                    try:
                        path.rmdir()
                    except OSError:
                        pass
        raise


from app.core.action_commands import command as _durable_command
archive = _durable_command('library_recovery.archive')(archive)
restore = _durable_command('library_recovery.restore')(restore)
