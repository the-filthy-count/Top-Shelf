"""Shared process-local decoder budget and lightweight workload diagnostics."""
from contextlib import contextmanager
from threading import Lock, Condition
from time import monotonic

LIMIT = 2
_lock = Lock()
_started = monotonic()
_jobs = {}
_admission = Condition()
_waiters = []
_active = 0

@contextmanager
def media_slot(label, priority=False):
    global _active
    token = object()
    priority = priority or label == "Interactive frames"
    with _lock:
        job = _jobs.setdefault(label, {"waiting": 0, "active": {}, "completed": 0, "errors": 0})
        job["waiting"] += 1
    admitted = False
    try:
        with _admission:
            entry = (0 if priority else 1, monotonic(), token)
            _waiters.append(entry)
            while True:
                if _stopping:
                    raise RuntimeError("Media workers are stopping")
                if _active < LIMIT and min(_waiters) == entry:
                    _waiters.remove(entry)
                    _active += 1
                    admitted = True
                    break
                _admission.wait(0.1)
        with _lock:
            job["waiting"] -= 1
            job["active"][token] = monotonic()
        try:
            yield
        except BaseException:
            with _lock:
                job["errors"] += 1
            raise
    finally:
        with _admission:
            if admitted:
                _active -= 1
            else:
                _waiters[:] = [entry for entry in _waiters if entry[2] is not token]
            _admission.notify_all()
        with _lock:
            if admitted:
                job["active"].pop(token, None)
                job["completed"] += 1
            else:
                job["waiting"] -= 1

def snapshot():
    now = monotonic()
    with _lock:
        return {"limit": LIMIT, "scope": "since server start", "jobs": [
            {"name": name, "active": len(job["active"]), "waiting": job["waiting"],
             "completed": job["completed"], "errors": job["errors"],
             "elapsed_seconds": round(max((now - t for t in job["active"].values()), default=0), 1),
             "frames_per_minute": round(job["completed"] * 60 / max(now - _started, 1), 1)}
            for name, job in _jobs.items()]}

# Track direct children so shutdown cannot leave media jobs running behind it.
_processes = set()
_stopping = False

def run_process(args, *, timeout, capture_output=True, check=False, text=False):
    import subprocess
    import tempfile
    import os
    # Spool bounded output instead of communicate() retaining arbitrary packet
    # listings in RAM. Monitor files while the child is running.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        with _lock:
            if _stopping:
                raise RuntimeError('Media workers are stopping')
            process = subprocess.Popen(args, stdout=out, stderr=err)
            _processes.add(process)
        deadline = monotonic() + timeout
        try:
            try:
                while True:
                    if max(os.fstat(out.fileno()).st_size, os.fstat(err.fileno()).st_size) > 16 * 1024 * 1024:
                        raise ValueError('Media command output exceeded 16 MiB')
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(args, timeout)
                    try:
                        process.wait(timeout=min(0.1, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        pass
                out.seek(0); err.seek(0)
                stdout, stderr = out.read(16 * 1024 * 1024 + 1), err.read(16 * 1024 * 1024 + 1)
                if max(len(stdout), len(stderr)) > 16 * 1024 * 1024:
                    raise ValueError('Media command output exceeded 16 MiB')
            except BaseException:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                process.wait()
                raise
            if text:
                stdout, stderr = stdout.decode(errors='replace'), stderr.decode(errors='replace')
            result = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
            if check:
                result.check_returncode()
            return result
        finally:
            with _lock:
                _processes.discard(process)

def shutdown():
    global _stopping
    with _lock:
        _stopping = True
        processes = list(_processes)
    for process in processes:
        try:
            process.kill()
        except ProcessLookupError:
            pass
