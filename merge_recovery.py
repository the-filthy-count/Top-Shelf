"""Durable, resumable performer merges. Sources survive until the DB commit."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil


def _save(path, data):
    temp = path.with_suffix('.tmp')
    with temp.open('w') as out:
        json.dump(data, out)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as src:
        for block in iter(lambda: src.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _files(root):
    # Fail closed on unreadable directories; never silently omit media.
    result = []
    for directory, dirs, files in os.walk(root, followlinks=False,
                                          onerror=lambda err: (_ for _ in ()).throw(err)):
        for name in dirs + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError(f'Merge does not follow symlinks: {path}')
        result.extend(Path(directory) / name for name in files)
    return sorted(result)


def execute(journal_dir, request, prepare, commit, video_extensions):
    """prepare returns immutable rows/destination; commit must be transactional/idempotent.

    Repeating the same request resumes its existing journal, including after a
    crash between DB commit and checkpoint. No source is deleted before commit.
    """
    journal_dir = Path(journal_dir)
    journal_dir.mkdir(parents=True, exist_ok=True)
    operation = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()[:24]
    path = journal_dir / (operation + '.json')
    with (journal_dir / 'merge.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('Another merge is running. Retry when it completes.')
        if path.exists():
            plan = json.loads(path.read_text())
        else:
            plan = prepare()
            plan.update(operation_id=operation, phase='copying', files=[], cleanup=[], copied=0)
            dest = Path(plan['destination']).resolve()
            rows = plan['rows']
            roots = [Path(row['path']).expanduser().resolve() for row in rows]
            for i, root in enumerate(roots):
                if not root.is_dir():
                    raise ValueError(f'Source folder is missing: {root}')
                for other in roots[i+1:]:
                    if root == other or root in other.parents or other in root.parents:
                        raise ValueError('Merge source folders must be distinct and non-overlapping')
                if root != dest and (root in dest.parents or dest in root.parents):
                    raise ValueError('Destination must not overlap a source folder')
            if dest.exists() and dest != roots[0]:
                raise ValueError('Destination already exists')
            reserved = set()
            for i, root in enumerate(roots):
                for source in _files(root):
                    relative = source.relative_to(root)
                    copied = i == 0 or source.suffix.lower() in video_extensions
                    stat = source.stat()
                    info = dict(source=str(source), size=stat.st_size, mtime_ns=stat.st_mtime_ns)
                    if copied:
                        target = dest / relative if i == 0 else dest / source.name
                        number = 2
                        while str(target) in reserved or (target.exists() and target != source):
                            target = dest / f'{source.stem} ({number}){source.suffix}'
                            number += 1
                        if len(target.name.encode()) > 255:
                            raise ValueError(f'Destination filename is too long: {target.name}')
                        reserved.add(str(target))
                        info.update(destination=str(target), sha256=_digest(source))
                        plan['files'].append(info)
                    # Retain in-place primary files. Everything else is cleaned
                    # only if it still matches the preflight snapshot.
                    if root != dest:
                        plan['cleanup'].append(info)
            ancestor = dest
            while not ancestor.exists():
                ancestor = ancestor.parent
            needed = sum(item['size'] for item in plan['files'] if item['source'] != item['destination'])
            if shutil.disk_usage(ancestor).free < needed:
                raise ValueError('Not enough space to retain source files while verifying the merge')
            plan['source_roots'] = [str(root) for root in roots if root != dest]
            plan['phase'] = 'copying'
            _save(path, plan)
        if plan['phase'] == 'complete':
            return dict(ok=True, row_id=plan['primary_id'], path=plan['destination'],
                        operation_id=operation, resumed=True)
        try:
            if plan['phase'] == 'copying':
                for idx, item in enumerate(plan['files']):
                    source, target = Path(item['source']), Path(item['destination'])
                    if target.exists():
                        if _digest(target) != item['sha256']:
                            raise ValueError(f'Destination changed; refusing to overwrite: {target}')
                        leftover = target.with_name('.' + target.name + '.' + operation + '.partial')
                        if source != target and leftover.exists():
                            leftover.unlink()
                    else:
                        stat = source.stat()
                        if stat.st_size != item['size'] or stat.st_mtime_ns != item['mtime_ns']:
                            raise ValueError(f'Source changed during merge: {source}')
                        target.parent.mkdir(parents=True, exist_ok=True)
                        temp = target.with_name('.' + target.name + '.' + operation + '.partial')
                        shutil.copyfile(source, temp)
                        shutil.copystat(source, temp)
                        if _digest(temp) != item['sha256']:
                            raise OSError(f'Copy verification failed: {source}')
                        with temp.open('rb') as copied:
                            os.fsync(copied.fileno())
                        # Link publishes without overwriting an unexpected file.
                        os.link(temp, target)
                        temp.unlink()
                        fd = os.open(target.parent, os.O_RDONLY)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                    plan['copied'] = idx + 1
                    _save(path, plan)
                # Recheck all published files before committing paths.
                for item in plan['files']:
                    if _digest(Path(item['destination'])) != item['sha256']:
                        raise OSError('Destination verification failed before commit')
                for item in plan['cleanup']:
                    source = Path(item['source'])
                    stat = source.stat()
                    if stat.st_size != item['size'] or stat.st_mtime_ns != item['mtime_ns']:
                        raise ValueError(f'Source changed before commit: {source}')
                commit(plan)
                plan['phase'] = 'cleanup'
                _save(path, plan)
            warnings = []
            for item in plan['cleanup']:
                source = Path(item['source'])
                if not source.exists():
                    continue
                stat = source.stat()
                if stat.st_size != item['size'] or stat.st_mtime_ns != item['mtime_ns']:
                    warnings.append(f'Preserved changed source: {source}')
                    continue
                if item.get('destination') and _digest(Path(item['destination'])) != item['sha256']:
                    warnings.append(f'Preserved source because destination changed: {source}')
                    continue
                source.unlink()
            for raw in plan['source_roots']:
                root = Path(raw)
                if not root.exists():
                    continue
                for directory, _, _ in os.walk(root, topdown=False):
                    try:
                        Path(directory).rmdir()
                    except OSError:
                        warnings.append(f'Preserved non-empty folder: {directory}')
            plan['phase'] = 'complete' if not warnings else 'cleanup'
            plan['warnings'] = warnings
            plan.pop('error', None)
            _save(path, plan)
            return dict(ok=True, row_id=plan['primary_id'], path=plan['destination'],
                        operation_id=operation, warnings=warnings)
        except Exception as exc:
            plan['error'] = str(exc)
            _save(path, plan)
            raise RuntimeError(f'Merge {operation} paused: {exc}. Retry the same merge to resume.') from exc
