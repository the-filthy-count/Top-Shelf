"""Recover a Vice folder with its settings-backed identity and library index."""
import json
import os
import stat
import uuid
from pathlib import Path

import database as db
from app.core.library_deletion import checked_folder
from app.core.library_recovery import roots, _files, _insert
from app.core.queue_recovery import store
from app.core.recovery import _signature


def _vices(conn):
    row = conn.execute("SELECT value FROM settings WHERE key='vices_json'").fetchone()
    value = json.loads(row[0] if row else '[]')
    if not isinstance(value, list) or any(not isinstance(v, dict) for v in value):
        raise ValueError('Invalid Vice configuration')
    return value


def _write(conn, vices, extra_tags=()):
    # Keep existing enriched tag records when possible; no network calls belong
    # inside a filesystem/DB commit. Resolution remains the provider's job.
    row = conn.execute("SELECT value FROM settings WHERE key='monitored_tags'").fetchone()
    prior = json.loads(row[0] if row else '[]') + list(extra_tags)
    flat, seen = [], set()
    for vice in vices:
        for tag in vice.get('tags', []):
            if not isinstance(tag, dict):
                continue
            key = str(tag.get('id') or tag.get('name') or '').lower()
            if not key or key in seen:
                continue
            seen.add(key)
            enriched = next((old for old in prior if isinstance(old, dict) and
                            (str(old.get('id') or '').lower() == key or
                             (tag.get('name') and str(old.get('name') or '').lower() == str(tag['name']).lower()))), tag)
            flat.append(enriched)
    for key, value in (('vices_json', vices), ('monitored_tags', flat)):
        conn.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', (key, json.dumps(value)))


def _invalidate():
    db._settings_cache = None
    db._settings_cache_ts = 0.0


def archive(vice_id, raw_folder):
    folder = checked_folder(raw_folder, roots())
    raw_root = (db.get_settings().get('vices_dir') or '').strip()
    if not raw_root or folder.parent != Path(raw_root).expanduser().resolve():
        raise ValueError('Vice folder must be a direct child of the configured Vice library')
    recovery = store()
    with db.file_operation_connection() as conn:
        vices = _vices(conn)
        matches = [v for v in vices if str(v.get('id')) == str(vice_id)]
        if len(matches) != 1:
            raise ValueError('Vice identity is missing or ambiguous')
        vice = matches[0]
        indexed = _files(conn, folder)
        saved_tags = conn.execute("SELECT value FROM settings WHERE key='monitored_tags'").fetchone()
        saved_tags = json.loads(saved_tags[0] if saved_tags else '[]')
    paths, directories = [], []
    def failed(exc):
        raise exc
    for parent, dirs, files in os.walk(folder, followlinks=False, onerror=failed):
        directories.append(str(Path(parent)))
        for name in dirs + files:
            path = Path(parent)/name
            if path.is_symlink() or name.lower().endswith('.part'):
                raise ValueError('Vice folder contains a symlink or partial download')
        for name in files:
            path = Path(parent)/name
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError('Vice folder contains a non-regular file')
            paths.append(path)
    context = {'kind': 'vice', 'folder': str(folder), 'filename': vice['name'], 'vice': vice,
               'position': vices.index(vice), 'root': str(folder.parent), 'saved_tags': saved_tags, 'indexed': indexed, 'directories': directories,
               'file_id': 'vice-' + uuid.uuid4().hex,
               'source_stats': {str(path): _signature(path) for path in paths}}
    def validate(conn, ctx):
        checked_folder(folder, roots())
        if _vices(conn) != vices or _files(conn, folder) != indexed:
            raise ValueError('Vice or index changed; refresh and retry')
    def commit(conn, ctx):
        validate(conn, ctx)
        for raw in reversed(directories):
            Path(raw).rmdir()
        for record in indexed:
            conn.execute('DELETE FROM library_files WHERE id=?', (record['id'],))
        _write(conn, [v for v in vices if str(v.get('id')) != str(vice_id)])
    try:
        return recovery.delete(paths, context['file_id'], context, commit, validate)
    except Exception:
        for raw in directories:
            path = Path(raw)
            if path.resolve() == path:
                path.mkdir(parents=True, exist_ok=True)
        raise
    finally:
        _invalidate()


def restore(operation_id):
    started = []
    def validate(conn, ctx):
        folder = Path(ctx['folder']); vice = ctx['vice']
        current_root = (db.get_settings().get('vices_dir') or '').strip()
        if not current_root or str(Path(current_root).expanduser().resolve()) != ctx['root']:
            raise ValueError('Original Vice library root changed')
        bases = [Path(r['path']).expanduser().resolve() for r in roots()]
        if (folder.resolve() != folder or any(folder == base or folder in base.parents for base in bases)
                or not any(base in folder.parents and base.is_dir() for base in bases)):
            raise ValueError('Original Vice library root changed or is unavailable')
        if folder.exists() or folder.is_symlink():
            raise ValueError('Original Vice folder is occupied')
        if any(str(v.get('id')) == str(vice['id']) or str(v.get('name', '')).casefold() == str(vice['name']).casefold() for v in _vices(conn)):
            raise ValueError('Vice identity or name is already in use')
        for row in ctx['indexed']:
            if conn.execute('SELECT 1 FROM library_files WHERE id=? OR destination=?', (row['id'], row['destination'])).fetchone():
                raise ValueError('Library index record is already in use')
        started.append(ctx)
    def commit(conn, ctx):
        current = _vices(conn)
        if any(str(v.get('id')) == str(ctx['vice']['id']) or str(v.get('name', '')).casefold() == str(ctx['vice']['name']).casefold() for v in current):
            raise ValueError('Vice identity or name changed during restore')
        for raw in ctx['directories']:
            path = Path(raw)
            if path.resolve() != path:
                raise ValueError('Restore path traverses a symlink')
            path.mkdir(parents=True, exist_ok=True)
        for row in ctx['indexed']:
            _insert(conn, 'library_files', row)
        current.insert(min(ctx['position'], len(current)), ctx['vice'])
        _write(conn, current, ctx.get('saved_tags', []))
    try:
        return store().restore(operation_id, validate, commit)
    except Exception:
        if started:
            for raw in reversed(started[0]['directories']):
                path = Path(raw)
                if path.resolve() == path:
                    try:
                        path.rmdir()
                    except OSError:
                        pass
        raise
    finally:
        _invalidate()


from app.core.action_commands import command as _durable_command
archive = _durable_command('vice_recovery.archive')(archive)
restore = _durable_command('vice_recovery.restore')(restore)
