"""Bounded source fan-out; slow providers cannot hold the HTTP request forever."""
from concurrent.futures import ThreadPoolExecutor, wait
from threading import BoundedSemaphore, Lock
from time import monotonic

_POOL = ThreadPoolExecutor(max_workers=10, thread_name_prefix="scene-search")
_SLOTS = BoundedSemaphore(20)
_CACHE = {}
_CACHE_LOCK = Lock()

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
                future = _POOL.submit(job)
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
            except Exception:
                warnings.append(f"{name}: search failed; retry this source.")
        else:
            if cache_key is None:
                future.cancel()
            warnings.append(f"{name}: timed out; retry this source.")
    return Results(rows, warnings)
