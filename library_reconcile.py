"""Incremental reconciliation of filed videos after external processing."""
from pathlib import Path
from threading import Lock
from time import monotonic

_lock = Lock()
_run_lock = Lock()
_observed = {}
_pending = {}
_errors = {}

def signature(path):
    st = Path(path).stat()
    return (st.st_size, st.st_mtime_ns)

def needs_hash(row, path):
    try:
        size, mtime = signature(path)
    except OSError:
        return False
    return not row.get("phash_2") or (row.get("phash_2_size"), row.get("phash_2_mtime_ns")) != (size, mtime)

def queue_completion(db, original, output, extensions):
    if not original or not output:
        raise ValueError("Both original_path and output_path are required")
    original, output = Path(original).resolve(), Path(output).resolve()
    row = db.library_file_get_by_destination(str(original))
    if not row:
        raise ValueError("Original path is not an indexed library file")
    root = Path(row.get("library_root") or original.parent).resolve()
    if not output.is_relative_to(root) or output.suffix.lower() not in extensions or not output.is_file():
        raise ValueError("Output must be a video within the same library root")
    with _lock:
        _pending[int(row["id"])] = str(output)
    return int(row["id"])

def replacement(row, rows, extensions):
    original = Path(row["destination"])
    if original.is_file():
        return original
    # Only reconcile an unambiguous extension change in the SAME directory.
    peers = [r for r in rows if Path(r["destination"]).parent == original.parent
             and Path(r["destination"]).stem == original.stem]
    if len(peers) != 1:
        return None
    try:
        matches = [p for p in original.parent.iterdir() if p.stem == original.stem
                   and p.suffix.lower() in extensions and p.is_file()]
    except OSError:
        return None
    return matches[0] if len(matches) == 1 else None

def refresh(db, row, path, compute, probe):
    """Publish only results for a file unchanged throughout hashing/probing."""
    path = Path(path)
    before = signature(path)
    h = compute(path)
    fields = probe({}, path)
    if fields is None or signature(path) != before:
        return False
    mt, created, codec, width, height = fields
    with db.get_conn() as conn:
        current = conn.execute("SELECT destination FROM library_files WHERE id=?", (row["id"],)).fetchone()
        if not current or current["destination"] != row["destination"]:
            return False
        occupied = conn.execute("SELECT id FROM library_files WHERE destination=? AND id!=?", (str(path), row["id"])).fetchone()
        if occupied:
            return False
        conn.execute("UPDATE library_files SET destination=?, current_filename=?, filename_stem=?, "
                     "phash_2=?, phash_2_size=?, phash_2_mtime_ns=?, phash_3=NULL, phash_3_scanned_at=NULL, "
                     "media_mtime=?, file_created_iso=?, media_codec=?, media_width=?, media_height=?, "
                     "is_removed=0, removed_at=NULL, updated_at=? WHERE id=?",
                     (str(path), path.name, path.stem, h, *before, mt, created, codec, width, height, db._iso_now(), row["id"]))
        # Preserve links back to the original pipeline records.
        conn.execute("UPDATE processed_files SET destination=?, destination_base=?, destination_ext=? WHERE destination=?",
                     (str(path), str(path.with_suffix("")), path.suffix.lower().lstrip("."), row["destination"]))
        conn.execute("UPDATE processed_movies SET destination=? WHERE destination=?", (str(path), row["destination"]))
        conn.commit()
    return True

def reconcile(db, compute, probe, extensions, emit):
    # Scheduled max_instances=1 plus this lock also protects manual invocation.
    if not _run_lock.acquire(blocking=False):
        return
    try:
        with db.get_conn() as conn:
            rows = [dict(r) for r in conn.execute("SELECT * FROM library_files")]
        ids = {int(r["id"]) for r in rows}
        for cache in (_observed, _pending, _errors):
            for key in set(cache) - ids:
                cache.pop(key, None)
        attempted = 0
        for row in rows:
            rid = int(row["id"])
            explicit = _pending.get(rid)
            path = Path(explicit) if explicit else replacement(row, rows, extensions)
            if path is None:
                _observed.pop(rid, None)
                continue
            try:
                sig = (str(path), *signature(path))
                if not explicit and not needs_hash(row, path) and str(path) == row["destination"]:
                    _observed.pop(rid, None)
                    continue
                old = _observed.get(rid)
                now = monotonic()
                if not old or old[0] != sig:
                    if row.get("phash_2") or row.get("phash_3"):
                        with db.get_conn() as conn:
                            conn.execute("UPDATE library_files SET phash_2=NULL, phash_3=NULL, phash_3_scanned_at=NULL WHERE id=? AND destination=?", (rid, row["destination"]))
                            conn.commit()
                    _observed[rid] = (sig, now)
                    continue
                if now - old[1] < 60 or now < _errors.get(rid, 0):
                    continue
                attempted += 1
                if refresh(db, row, path, compute, probe):
                    _pending.pop(rid, None)
                    _observed.pop(rid, None)
                    _errors.pop(rid, None)
                    emit(f"Library fingerprint updated: {path.name}")
                else:
                    _observed.pop(rid, None)
            except Exception as exc:
                _errors[rid] = monotonic() + 3600
                emit(f"Library reconciliation: {exc}")
            if attempted >= 4:
                break
    finally:
        _run_lock.release()
