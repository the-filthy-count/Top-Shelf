"""Bounded background admission and thread-safe activity delivery."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, BoundedSemaphore
import queue

class BoundedExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers, pending=32, **kwargs):
        super().__init__(max_workers=max_workers, **kwargs)
        self._admission = BoundedSemaphore(max_workers + pending)
        self._stats_lock = Lock()
        self._queued = self._running = self._rejected = 0
        self.capacity = max_workers + pending

    def submit(self, fn, /, *args, **kwargs):
        if not self._admission.acquire(blocking=False):
            with self._stats_lock:
                self._rejected += 1
            raise RuntimeError('Background capacity reached; defer work')
        with self._stats_lock:
            self._queued += 1
        try:
            def execute():
                with self._stats_lock:
                    self._queued -= 1
                    self._running += 1
                try:
                    return fn(*args, **kwargs)
                finally:
                    with self._stats_lock:
                        self._running -= 1
                    import sys
                    database = sys.modules.get("database")
                    if database is not None:
                        database.close_thread_connection()
            future = super().submit(execute)
        except BaseException:
            with self._stats_lock:
                self._queued -= 1
            self._admission.release()
            raise
        def done(future):
            if future.cancelled():
                with self._stats_lock:
                    self._queued -= 1
            self._admission.release()
        future.add_done_callback(done)
        return future

    def snapshot(self):
        with self._stats_lock:
            return {"active": self._running, "queued": self._queued, "deferred": self._rejected, "capacity": self.capacity}

class ActivityBuffer:
    def __init__(self, limit=2000):
        self.lock = Lock()
        self.pending = deque(maxlen=limit)
        self.subscribers = set()
        self.dropped = 0

    def append(self, line):
        with self.lock:
            if len(self.pending) == self.pending.maxlen:
                self.dropped += 1
            self.pending.append(line)
            for subscriber in self.subscribers:
                try:
                    subscriber.put_nowait(line)
                except queue.Full:
                    try:
                        subscriber.get_nowait()
                    except queue.Empty:
                        pass
                    subscriber.put_nowait(line)

    def subscribe(self):
        subscriber = queue.Queue(maxsize=256)
        with self.lock:
            self.subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber):
        with self.lock:
            self.subscribers.discard(subscriber)

    def drain(self):
        with self.lock:
            lines = list(self.pending)
            self.pending.clear()
            if self.dropped:
                lines.insert(0, f'Activity buffer overflow: {self.dropped} older messages omitted')
                self.dropped = 0
            return lines


def bounded_results(executor, function, items, window):
    """Keep at most window futures alive, irrespective of library size."""
    from concurrent.futures import wait, FIRST_COMPLETED
    iterator = iter(items)
    pending = {}
    def fill():
        while len(pending) < window:
            try:
                item = next(iterator)
            except StopIteration:
                return
            pending[executor.submit(function, item)] = item
    fill()
    try:
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                item = pending.pop(future)
                yield item, future.result()
            fill()
    finally:
        for future in pending:
            future.cancel()

_source_handles = {}
_source_guard = Lock()

def claim_sources(paths):
    """Advisory ownership shared across containers using the same source bind."""
    import fcntl
    from pathlib import Path
    with _source_guard:
        for raw in sorted({str(Path(p).expanduser().resolve()) for p in paths if str(p).strip()}):
            path = Path(raw)
            if not path.is_dir() or raw in _source_handles:
                continue
            handle = (path / '.top-shelf-processing.lock').open('a')
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BaseException:
                handle.close()
                raise RuntimeError(f'Another Top-Shelf instance owns source directory: {path}')
            _source_handles[raw] = handle


_singleflight_guard = Lock()
_singleflight_active = set()

def singleflight(function):
    """Coalesce concurrent refresh triggers, including scheduler/startup overlap."""
    from functools import wraps
    @wraps(function)
    def run(*args, **kwargs):
        with _singleflight_guard:
            if function.__name__ in _singleflight_active:
                return None
            _singleflight_active.add(function.__name__)
        try:
            return function(*args, **kwargs)
        finally:
            with _singleflight_guard:
                _singleflight_active.discard(function.__name__)
    return run
