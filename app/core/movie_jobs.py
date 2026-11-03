"""Persistent movie/JAV work independent of the in-memory watcher queue."""
from pathlib import Path
import database as db
from download_safety import PartialDownloadPending


def run(filename, src, dest_kind, process):
    settings = db.get_settings()
    if src is None:
        raw = (settings.get('movies_source_dir') or '').strip()
        if not raw:
            return {'status': 'error', 'error': 'Movies source directory is not configured'}
        src = Path(raw).expanduser()
    else:
        src = Path(src).expanduser()
    jav = (settings.get('jav_download_watch_dir') or '').strip()
    is_jav = bool(jav) and src.resolve() == Path(jav).expanduser().resolve()
    key = 'jav://' + Path(filename).as_posix() if is_jav else filename
    db.upsert_movie(key)
    job_id = db.processing_job_start(key, 'processed_movies')
    if not job_id:
        return {'status': 'busy'}
    outcome, error = 'error', 'Worker interrupted'
    try:
        path = src / filename
        if path.resolve() == src.resolve() or src.resolve() not in path.resolve().parents:
            raise ValueError('Movie path escapes inbox')
        if not path.is_file():
            raise FileNotFoundError('Movie source is unavailable')
        db.processing_job_checkpoint(job_id, 'Matching movie')
        result = process(filename, src=src, dest_kind=dest_kind)
        if not result:
            with db.get_conn() as conn:
                row = conn.execute('SELECT status,error FROM processed_movies WHERE filename=?', (key,)).fetchone()
            result = {'status': row[0] if row else 'error', 'error': row[1] if row else 'Movie record disappeared'}
        outcome, error = result.get('status', 'error'), result.get('error')
        return result
    except PartialDownloadPending as exc:
        outcome, error = 'deferred', str(exc)
        return {'status': outcome, 'error': error}
    except Exception as exc:
        outcome, error = 'error', str(exc)
        db.update_movie(key, status='error', error=error)
        raise
    finally:
        db.processing_job_finish(job_id, outcome, error)
