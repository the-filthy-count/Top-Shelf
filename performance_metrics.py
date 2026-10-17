"""Bounded, process-local timing diagnostics. No filenames or external telemetry."""
from collections import deque
from contextlib import contextmanager
from threading import Lock
from time import perf_counter

_lock = Lock()
_metrics = {}

def record(name, seconds=0, failed=False):
    with _lock:
        if name not in _metrics:
            if len(_metrics) >= 128:
                return
            _metrics[name] = {"count": 0, "errors": 0, "total_ms": 0, "samples": deque(maxlen=256)}
        entry = _metrics[name]
        ms = max(0, seconds * 1000)
        entry["count"] += 1
        entry["errors"] += int(failed)
        entry["total_ms"] += ms
        entry["samples"].append(ms)

@contextmanager
def measure(name):
    started = perf_counter()
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        record(name, perf_counter() - started, failed)

def snapshot():
    with _lock:
        result = {}
        for name, entry in _metrics.items():
            samples = sorted(entry["samples"])
            result[name] = {"count": entry["count"], "errors": entry["errors"],
                "mean_ms": round(entry["total_ms"] / entry["count"], 2),
                "recent_p95_ms": round(samples[min(len(samples)-1, int(len(samples)*.95))], 2),
                "last_ms": round(entry["samples"][-1], 2)}
        return result
