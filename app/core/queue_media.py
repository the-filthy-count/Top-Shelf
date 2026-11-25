"""Shared, change-validated technical observations for every queued video."""
import json
import threading
import database as db

_locks = [threading.Lock() for _ in range(32)]


def read(filename, video, *, duration_probe=None, stream_probe=None, phash_probe=None, fingerprint_probe=None):
    # Serialise overlapping UI/thumbnail requests for the same record.
    with _locks[hash(filename) % len(_locks)]:
        movie = filename.startswith(('JAV/', 'movie://', 'jav://'))
        table = 'processed_movies' if movie else 'processed_files'
        key = ('jav://' + filename[4:] if filename.startswith('JAV/') else filename.removeprefix('movie://'))
        if movie:
            db.upsert_movie(key)
            signature = db.media_file_signature(video)
            with db.get_conn() as conn:
                old = conn.execute(f'SELECT media_signature FROM {table} WHERE filename=?',(key,)).fetchone()
                try:
                    reusable = json.loads(old[0])[:4] == json.loads(signature)[:4]
                except (ValueError, TypeError, IndexError):
                    reusable = False
                if not reusable:
                    conn.execute(f'UPDATE {table} SET pipeline_match_json=NULL,phash=NULL,media_duration=NULL,media_size_bytes=NULL,media_codec=NULL,media_width=NULL,media_height=NULL WHERE filename=?',(key,))
                conn.execute(f'UPDATE {table} SET media_signature=? WHERE filename=?',(signature,key))
                conn.commit()
        else:
            db.upsert_file(key)
            signature = db.validate_media_cache(key, video)
        with db.get_conn() as conn:
            row = conn.execute(f'SELECT * FROM {table} WHERE filename=?', (key,)).fetchone()
        values = dict(row) if row else {}
        def persist(updates):
            # Commit each successful stage so a later probe/hash failure cannot
            # discard already completed analysis. Never publish stale values.
            assert_signature(video, signature)
            if updates:
                with db.get_conn() as conn:
                    changed = conn.execute(f'UPDATE {table} SET '+', '.join(k+'=?' for k in updates)+
                        ' WHERE filename=? AND media_signature=?', [*updates.values(),key,signature]).rowcount
                    if changed != 1:
                        from download_safety import PartialDownloadPending
                        raise PartialDownloadPending('Video record changed during metadata read')
                values.update(updates)
        size = video.stat().st_size
        persist({'media_size_bytes': size} if values.get('media_size_bytes') != size else {})
        if duration_probe and not values.get('media_duration'):
            duration = duration_probe(video)
            persist({'media_duration': duration} if duration and duration > 0 else {})
        if stream_probe and not all(values.get(k) for k in ('media_codec','media_width','media_height')):
            codec, width, height = stream_probe(video)
            persist({k:v for k,v in zip(('media_codec','media_width','media_height'),(codec,width,height)) if v})
        if not values.get('phash'):
            if fingerprint_probe:
                persist({'phash': fingerprint_probe(video, duration=values.get('media_duration'))})
            elif phash_probe:
                persist({'phash': phash_probe(video)})
        assert_signature(video, signature)
        return values


def assert_signature(video, expected_signature):
    if db.media_file_signature(video) != expected_signature:
        from download_safety import PartialDownloadPending
        raise PartialDownloadPending('Video changed during lookup; retry when complete')


def save_match(filename, video, match, *, expected_signature):
    """A verified lookup checkpoint tied to the observed file contents."""
    movie = filename.startswith(('jav://', 'movie://', 'JAV/'))
    table = 'processed_movies' if movie else 'processed_files'
    key = 'jav://' + filename[4:] if filename.startswith('JAV/') else filename.removeprefix('movie://')
    assert_signature(video, expected_signature)
    from app.core.queue_changes import assert_current
    with db.get_conn() as conn:
        conn.execute('BEGIN IMMEDIATE')
        assert_current(conn)
        changed = conn.execute(f'UPDATE {table} SET pipeline_match_json=? WHERE filename=? AND media_signature=?',
                     (json.dumps(match), key, expected_signature)).rowcount
        if changed != 1:
            from download_safety import PartialDownloadPending
            raise PartialDownloadPending('Video changed during lookup; retry when complete')
