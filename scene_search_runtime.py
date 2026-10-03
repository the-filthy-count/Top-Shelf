"""Bounded source fan-out; slow providers cannot hold the HTTP request forever."""
from concurrent.futures import ThreadPoolExecutor, wait
from threading import BoundedSemaphore, Lock
from time import monotonic
from contextvars import ContextVar

_POOL = ThreadPoolExecutor(max_workers=10, thread_name_prefix="scene-search")
_SLOTS = BoundedSemaphore(20)
_CACHE = {}
_CACHE_LOCK = Lock()
_SOURCE_DEADLINE = ContextVar("scene_source_deadline", default=None)

class SourceSearchError(RuntimeError):
    pass

def source_time_left():
    deadline = _SOURCE_DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise SourceSearchError("request deadline exceeded")
    return remaining

def _run_job(job, deadline):
    token = _SOURCE_DEADLINE.set(deadline)
    try:
        return job()
    finally:
        _SOURCE_DEADLINE.reset(token)


class Results(list):
    def __init__(self, rows=(), warnings=()):
        super().__init__(rows)
        self.warnings = list(warnings)

def run_sources(jobs, timeout=12, cache_key=None):
    futures = {}
    warnings = []
    for name, job in jobs:
        key = (cache_key, name) if cache_key is not None else None
        with _CACHE_LOCK:
            now = monotonic()
            for old_key, (created, old_future) in list(_CACHE.items()):
                if old_future.done() and (now - created > 60 or old_future.cancelled() or old_future.exception() is not None):
                    del _CACHE[old_key]
            cached = _CACHE.get(key) if key is not None else None
            if cached:
                futures[cached[1]] = name
                continue
            if not _SLOTS.acquire(blocking=False):
                warnings.append(f"{name}: search workers busy; retry shortly.")
                continue
            try:
                future = _POOL.submit(_run_job, job, monotonic() + max(.001, timeout - 1))
            except Exception:
                _SLOTS.release()
                raise
            future.add_done_callback(lambda _: _SLOTS.release())
            if key is not None:
                if len(_CACHE) >= 128:
                    for old_key, (_, old_future) in list(_CACHE.items()):
                        if old_future.done():
                            del _CACHE[old_key]
                            break
                _CACHE[key] = (now, future)
            futures[future] = name
    done, pending = wait(futures, timeout=timeout) if futures else (set(), set())
    rows = []
    for future, name in futures.items():
        if future in done:
            try:
                rows.extend(future.result())
            except SourceSearchError as exc:
                warnings.append(f"{name}: {exc}.")
            except Exception:
                warnings.append(f"{name}: search failed; retry this source.")
        else:
            if cache_key is None:
                future.cancel()
            warnings.append(f"{name}: timed out; retry this source.")
    return Results(rows, warnings)
