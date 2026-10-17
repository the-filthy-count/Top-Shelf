"""Fail closed on unavailable roots; never create probe threads for stalled I/O."""
from contextlib import contextmanager
from threading import Lock
from time import monotonic
from pathlib import Path
import os

_guard = Lock()
_states = {}

class StorageUnavailable(OSError):
    pass

@contextmanager
def root_operation(root):
    # Lexical normalization must not itself touch the potentially stalled disk.
    key = os.path.abspath(os.path.expanduser(str(root)))
    with _guard:
        state = _states.setdefault(key, dict(active=False, failures=0, retry=0, started=0, error=''))
        if state['active'] or monotonic() < state['retry']:
            raise StorageUnavailable('Storage work paused for ' + key)
        state.update(active=True, started=monotonic())
    try:
        Path(key).stat()
        yield
    except OSError as exc:
        with _guard:
            state['failures'] = min(10, state['failures'] + 1)
            state['retry'] = monotonic() + min(1800, 30 * 2 ** (state['failures'] - 1))
            state['error'] = str(exc)[:240]
        raise
    else:
        with _guard:
            state.update(failures=0, retry=0, error='')
    finally:
        with _guard:
            state['active'] = False

def snapshot():
    with _guard:
        now = monotonic()
        return {root: dict(active=s['active'], failures=s['failures'], error=s['error'],
                           active_seconds=round(now-s['started'], 1) if s['active'] else 0,
                           retry_seconds=max(0, round(s['retry']-now)))
                for root, s in _states.items()}
