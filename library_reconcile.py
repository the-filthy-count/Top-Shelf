"""Incremental reconciliation of filed videos after external processing."""
from pathlib import Path
from threading import Lock
from time import monotonic

_lock = Lock()
_run_lock = Lock()
_observed = {}
_pending = {}
_errors = {}
_decode_failures = {}
_cursor = 0
SCAN_BATCH = 500

def signature(path):
    st = Path(path).stat()
    return (st.st_size, st.st_mtime_ns)

def needs_hash(row, path):
    try:
        size, mtime = signature(path)
    except FileNotFoundError:
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
    except FileNotFoundError:
        return None
    return matches[0] if len(matches) == 1 else None

def refresh(db, row, path, compute, probe, *, known_hash=None, known_signature=None):
    """Publish only results for a file unchanged throughout hashing/probing."""
    path = Path(path)
    before = signature(path)
    key = str(path)
    with _lock:
        failed = _decode_failures.get(key)
    if failed and failed[0] == before and monotonic() < failed[1]:
        raise ValueError("Fingerprint decode previously failed; retry deferred for up to one hour")
    try:
        h = known_hash if known_hash and known_signature == before else compute(path)
    except Exception as exc:
        with db.get_conn() as conn:
            conn.execute("UPDATE library_files SET media_error=? WHERE id=? AND destination=?",
                         (str(exc)[:1000], row["id"], row["destination"]))
        with _lock:
            if len(_decode_failures) >= 1000:
                _decode_failures.pop(next(iter(_decode_failures)))
            _decode_failures[key] = (before, monotonic() + 3600)
        raise
    with _lock:
        _decode_failures.pop(key, None)
    if not h:
        return False
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
                     "is_removed=0, removed_at=NULL, media_error=NULL, updated_at=? WHERE id=?",
                     (str(path), path.name, path.stem, h, *before, mt, created, codec, width, height, db._iso_now(), row["id"]))
        # Preserve links back to the original pipeline records.
        conn.execute("UPDATE processed_files SET destination=?, destination_base=?, destination_ext=? WHERE destination=?",
                     (str(path), str(path.with_suffix("")), path.suffix.lower().lstrip("."), row["destination"]))
        conn.execute("UPDATE processed_movies SET destination=? WHERE destination=?", (str(path), row["destination"]))
        conn.commit()
    return True

def reconcile(db, compute, probe, extensions, emit):
    global _cursor
    # Scheduled max_instances=1 plus this lock also protects manual invocation.
    if not _run_lock.acquire(blocking=False):
        return
    try:
        # Keyset pagination: memory and SQL work no longer scale with the library.
        with _lock:
            priority = list(dict.fromkeys(list(_pending) + list(_observed)))[:SCAN_BATCH]
        with db.get_conn() as conn:
            important = []
            if priority:
                important = [dict(r) for r in conn.execute(
                    "SELECT * FROM library_files WHERE id IN (" + ",".join("?" for _ in priority) + ")", priority)]
            sweep = [dict(r) for r in conn.execute(
                "SELECT * FROM library_files WHERE id>? ORDER BY id LIMIT ?", (_cursor, SCAN_BATCH))]
            if not sweep:
                _cursor = 0
                sweep = [dict(r) for r in conn.execute(
                    "SELECT * FROM library_files ORDER BY id LIMIT ?", (SCAN_BATCH,))]
        present = {int(r["id"]) for r in important}
        for rid in set(priority) - present:
            for cache in (_observed, _pending, _errors):
                cache.pop(rid, None)
        selected = important + [r for r in sweep if int(r["id"]) not in present]
        attempted = 0
        for row in selected:
            if int(row["id"]) not in priority:
                _cursor = int(row["id"])
            rid = int(row["id"])
            explicit = _pending.get(rid)
            try:
                from storage_health import root_operation
                with root_operation(row.get("library_root") or Path(row["destination"]).parent):
                    if explicit:
                        path = Path(explicit)
                    else:
                        with db.get_conn() as conn:
                            peers = [dict(r) for r in conn.execute(
                                "SELECT * FROM library_files WHERE filename_stem=? OR id=?",
                                (Path(row["destination"]).stem, rid))]
                        path = replacement(row, peers, extensions)
                    if path is None:
                        _observed.pop(rid, None)
                        continue
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
                        if rid in _observed or len(_observed) < SCAN_BATCH * 2:
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
                if len(_errors) >= 1000:
                    _errors.pop(next(iter(_errors)))
                _errors[rid] = monotonic() + 3600
                emit(f"Library reconciliation: {exc}")
            if attempted >= 4:
                break
    finally:
        _run_lock.release()
