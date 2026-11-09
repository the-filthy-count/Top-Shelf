"""Recoverable removal of queue files, with durable manifests and guarded restore."""
import json
import threading
import uuid
from pathlib import Path

from app.utils.fs import move_exclusive

_lock = threading.RLock()


def install(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS file_recovery (
        id TEXT PRIMARY KEY, file_id TEXT NOT NULL, state TEXT NOT NULL,
        manifest TEXT NOT NULL, error TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    conn.execute('CREATE INDEX IF NOT EXISTS file_recovery_asset ON file_recovery(file_id,created_at)')


def _signature(path):
    st = Path(path).lstat()
    return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns]


def _row(conn, operation_id):
    result = conn.execute('SELECT id,file_id,state,manifest,error FROM file_recovery WHERE id=?', (operation_id,)).fetchone()
    if result is None:
        raise ValueError('Recovery record not found')
    return dict(zip(('id', 'file_id', 'state', 'manifest', 'error'), result))


def _save(conn, operation_id, state, manifest, error=None):
    conn.execute('''UPDATE file_recovery SET state=?,manifest=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?''',
                 (state, json.dumps(manifest), str(error) if error else None, operation_id))
    conn.commit()


def recover_interrupted(conn):
    conn.execute("UPDATE file_recovery SET state='purge_failed',error='Permanent deletion interrupted; retry to remove remaining retained files' WHERE state='purging'")
    # Never guess which side of a filesystem/DB boundary a crashed worker reached.
    conn.execute("UPDATE file_recovery SET state='blocked',error='Operation interrupted; review retained paths before restoring' WHERE state IN ('removing','restoring')")


class RecoveryStore:
    def __init__(self, connect, root, move=move_exclusive):
        self.connect = connect
        self.root = Path(root).resolve()
        self.move = move

    def delete(self, paths, file_id, context, commit, validate=None):
        """Delete in place. Retain an audit event, never a recovery copy."""
        with _lock, self.connect() as conn:
            if validate is not None:
                validate(conn, context)
            entries = []
            for raw in paths:
                path = Path(raw)
                if path.is_symlink() or not path.is_file():
                    raise ValueError('Deletion requires existing regular files')
                signature = _signature(path)
                expected = context.get('source_stats', {}).get(str(path.resolve()))
                if expected is not None and signature != expected:
                    raise OSError('Source identity changed before deletion')
                entries.append((path, signature))
            if not entries and context.get('kind') not in ('library', 'vice'):
                raise ValueError('No files to delete')
            freed = 0
            deleted = []
            try:
                conn.execute('BEGIN IMMEDIATE')
                for path, signature in entries:
                    if path.is_symlink() or _signature(path) != signature:
                        raise OSError('Source changed before deletion')
                    path.unlink()
                    deleted.append(str(path))
                    freed += signature[2]
                commit(conn, context)
                conn.execute("UPDATE file_assets SET current_path='' WHERE id=?", (file_id,))
                conn.execute('INSERT INTO file_events(file_id,kind,detail) VALUES(?,?,?)',
                             (file_id, 'file.deleted', json.dumps({'paths': deleted, 'freed_bytes': freed})))
                conn.commit()
            except Exception as exc:
                conn.rollback()
                conn.execute('INSERT INTO file_events(file_id,kind,detail) VALUES(?,?,?)',
                             (file_id, 'file.delete_failed', json.dumps({'deleted_paths': deleted, 'error': str(exc)})))
                conn.commit()
                raise
            return {'deleted': True, 'recoverable': False, 'freed_bytes': freed}

    def archive(self, paths, file_id, context, commit, validate=None):
        """Commit receives the journal connection only after every file is safe."""
        with _lock:
            if validate is not None:
                with self.connect() as conn:
                    validate(conn, context)
            operation_id = uuid.uuid4().hex
            folder = self.root / operation_id
            entries = []
            for index, raw in enumerate(paths):
                path = Path(raw)
                if path.is_symlink() or not path.is_file():
                    raise ValueError('Recovery requires existing regular files')
                expected = context.get('source_stats', {}).get(str(path.resolve()))
                if expected is not None and _signature(path) != expected:
                    raise OSError('Source identity changed before removal')
                entries.append({'source': str(path.resolve()), 'stored': str(folder / f'{index:04d}'),
                                'original_stat': _signature(path), 'stored_stat': None})
            if not entries and context.get('kind') not in ('library', 'vice'):
                raise ValueError('No files to remove')
            manifest = {'context': context, 'entries': entries}
            folder.mkdir(parents=True, exist_ok=False)
            with self.connect() as conn:
                conn.execute('INSERT INTO file_recovery(id,file_id,state,manifest) VALUES(?,?,?,?)',
                             (operation_id, file_id, 'removing', json.dumps(manifest)))
                conn.commit()
                completed = []
                try:
                    for entry in entries:
                        if _signature(entry['source']) != entry['original_stat']:
                            raise OSError('Source changed before removal')
                        self.move(Path(entry['source']), Path(entry['stored']), expected=entry['original_stat'])
                        completed.append(entry)
                        entry['stored_stat'] = _signature(entry['stored'])
                        _save(conn, operation_id, 'removing', manifest)
                    conn.execute('BEGIN IMMEDIATE')
                    commit(conn, context)
                    conn.execute("UPDATE file_assets SET current_path=? WHERE id=?", (str(folder), file_id) if not entries else (entries[0]['stored'], file_id))
                    conn.execute("INSERT INTO file_events(file_id,kind,old_path,new_path,detail) VALUES(?,?,?,?,?)",
                                 (file_id, 'file.recovered_removal', entries[0]['source'] if entries else context['folder'], str(folder), operation_id))
                    _save(conn, operation_id, 'archived', manifest)
                except Exception as exc:
                    conn.rollback()
                    rollback_errors = []
                    for entry in reversed(completed):
                        try:
                            self.move(Path(entry['stored']), Path(entry['source']), expected=entry['stored_stat'])
                        except Exception as rollback_error:
                            rollback_errors.append(str(rollback_error))
                    if any(Path(entry['stored']).exists() for entry in entries):
                        rollback_errors.append('Some retained files remain; review the manifest')
                    state = 'blocked' if rollback_errors else 'rolled_back'
                    _save(conn, operation_id, state, manifest, str(exc) + ('; rollback: ' + '; '.join(rollback_errors) if rollback_errors else ''))
                    raise
            return {'recovery_id': operation_id, 'retained_bytes': sum(e['original_stat'][2] for e in entries)}

    def restore(self, operation_id, validate, commit):
        with _lock, self.connect() as conn:
            record = _row(conn, operation_id)
            if record['state'] != 'archived':
                raise ValueError('This operation is not ready for automatic restore')
            manifest = json.loads(record['manifest'])
            validate(conn, manifest['context'])
            for entry in manifest['entries']:
                stored, source = Path(entry['stored']), Path(entry['source'])
                if stored.resolve().parent != self.root / operation_id or stored.is_symlink():
                    raise ValueError('Invalid recovery storage path')
                if not stored.is_file() or _signature(stored) != entry['stored_stat']:
                    raise ValueError('Retained file changed or is missing')
                if source.resolve() != source:
                    raise ValueError('Restore destination traverses a symlink')
                if source.exists() or source.is_symlink():
                    raise FileExistsError(f'Restore destination is occupied: {source.name}')
            _save(conn, operation_id, 'restoring', manifest)
            completed = []
            try:
                for entry in reversed(manifest['entries']):
                    self.move(Path(entry['stored']), Path(entry['source']), expected=entry['stored_stat'])
                    completed.append(entry)
                    entry['restored_stat'] = _signature(entry['source'])
                conn.execute('BEGIN IMMEDIATE')
                commit(conn, manifest['context'])
                file_id = record['file_id']
                conn.execute('UPDATE file_assets SET current_path=? WHERE id=?', (manifest['entries'][0]['source'] if manifest['entries'] else manifest['context']['folder'], file_id))
                conn.execute("INSERT INTO file_events(file_id,kind,new_path,detail) VALUES(?,?,?,?)",
                             (file_id, 'file.restored', manifest['entries'][0]['source'] if manifest['entries'] else manifest['context']['folder'], operation_id))
                _save(conn, operation_id, 'restored', manifest)
            except Exception as exc:
                conn.rollback()
                rollback_errors = []
                for entry in reversed(completed):
                    try:
                        self.move(Path(entry['source']), Path(entry['stored']), expected=entry['restored_stat'])
                        entry['stored_stat'] = _signature(entry['stored'])
                    except Exception as rollback_error:
                        rollback_errors.append(str(rollback_error))
                _save(conn, operation_id, 'blocked' if rollback_errors else 'archived', manifest,
                      str(exc) + ('; rollback: ' + '; '.join(rollback_errors) if rollback_errors else ''))
                raise
            return manifest['context']

    def purge(self, operation_id):
        """Explicit permanent removal touches only verified recovery-storage paths."""
        with _lock, self.connect() as conn:
            record = _row(conn, operation_id)
            if record['state'] not in ('archived', 'purge_failed'):
                raise ValueError('This recovery record cannot be permanently deleted')
            manifest = json.loads(record['manifest'])
            paths = []
            for entry in manifest['entries']:
                path = Path(entry['stored'])
                if path.resolve().parent != self.root / operation_id or path.is_symlink():
                    raise ValueError('Invalid recovery storage path')
                if not path.exists() and record['state'] == 'purge_failed':
                    continue
                if not path.is_file() or _signature(path) != entry['stored_stat']:
                    raise ValueError('Retained file changed or is missing')
                paths.append(path)
            _save(conn, operation_id, 'purging', manifest)
            freed = 0
            try:
                for path in paths:
                    size = path.stat().st_size
                    path.unlink()
                    freed += size
                conn.execute("INSERT INTO file_events(file_id,kind,detail) VALUES(?,?,?)",
                             (record['file_id'], 'file.recovery_purged', operation_id))
                _save(conn, operation_id, 'purged', manifest)
            except Exception as exc:
                conn.rollback()
                _save(conn, operation_id, 'purge_failed', manifest, exc)
                raise
            try:
                (self.root / operation_id).rmdir()
            except OSError:
                pass
            return {'ok': True, 'freed_bytes': freed}

    def list(self, limit=100, before=''):
        cursor = before.split('|', 1) if before else None
        if cursor is not None and len(cursor) != 2:
            raise ValueError('Invalid recovery cursor')
        with self.connect() as conn:
            rows = conn.execute('''SELECT id,file_id,state,manifest,error,created_at FROM file_recovery
                WHERE state IN ('archived','blocked','removing','restoring','purging','purge_failed')
                AND (? IS NULL OR (created_at,id) < (?,?)) ORDER BY created_at DESC,id DESC LIMIT ?''',
                (before or None, *(cursor or ['', '']), max(1, min(int(limit), 500)))).fetchall()
        return [dict(id=r[0], file_id=r[1], state=r[2], filename=json.loads(r[3])['context']['filename'],
                     retained_bytes=sum(e['original_stat'][2] for e in json.loads(r[3])['entries']),
                     error=r[4], created_at=r[5]) for r in rows]
