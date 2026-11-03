"""Retain quality-gate losers and roll back failed same-path upgrades."""
import threading
from pathlib import Path

from app.core.video_recovery import archive, restore
from app.utils.fs import safe_move

_local = threading.local()


def _pending():
    if not hasattr(_local, 'pending'):
        _local.pending = {}
    return _local.pending


def replace(source, destination):
    """The incumbent is recoverable before publication of its replacement."""
    key = str(Path(destination).absolute())
    if key in _pending():
        raise ValueError('An unfinished replacement already owns this destination')
    operation = archive(destination)['recovery_id']
    _pending()[key] = operation
    try:
        safe_move(source, destination, replace_existing=False)
    except Exception:
        rollback(destination)
        raise


def rollback(destination):
    operation = _pending().pop(str(Path(destination).absolute()), None)
    if operation:
        restore(operation)


def finish(destination):
    # Successful upgrades retain the incumbent in the shared recovery panel.
    _pending().pop(str(Path(destination).absolute()), None)
