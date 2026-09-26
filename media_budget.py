"""Shared process-local decoder budget and lightweight workload diagnostics."""
from contextlib import contextmanager
from threading import BoundedSemaphore, Lock
from time import monotonic

LIMIT = 2
_slots = BoundedSemaphore(LIMIT)
_lock = Lock()
_started = monotonic()
_jobs = {}

@contextmanager
def media_slot(label):
    with _lock:
        job = _jobs.setdefault(label, {"waiting": 0, "active": {}, "completed": 0, "errors": 0})
        job["waiting"] += 1
    token = object()
    with _slots:
        with _lock:
            job["waiting"] -= 1
            job["active"][token] = monotonic()
        failed = False
        try:
            yield
        except BaseException:
            failed = True
            raise
        finally:
            with _lock:
                job["active"].pop(token)
                job["completed"] += 1
                job["errors"] += int(failed)

def snapshot():
    now = monotonic()
    with _lock:
        return {"limit": LIMIT, "scope": "since server start", "jobs": [
            {"name": name, "active": len(job["active"]), "waiting": job["waiting"],
             "completed": job["completed"], "errors": job["errors"],
             "elapsed_seconds": round(max((now - t for t in job["active"].values()), default=0), 1),
             "frames_per_minute": round(job["completed"] * 60 / max(now - _started, 1), 1)}
            for name, job in _jobs.items()]}
