"""Process-wide fingerprint endpoint cooldown shared by sync and async lookups."""
import math
import threading
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

_lock = threading.Lock()
_until = {}


def _key(endpoint):
    return urlsplit(endpoint).netloc.lower()


def check(endpoint):
    with _lock:
        remaining = _until.get(_key(endpoint), 0) - time.monotonic()
    if remaining > 0:
        raise RuntimeError(f"Fingerprint service rate-limited; retry in {math.ceil(remaining)} seconds")


def rate_limited(endpoint, retry_after=None):
    delay = 300.0
    try:
        delay = float(retry_after)
        if not math.isfinite(delay) or delay <= 0:
            delay = 300.0
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(str(retry_after))
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            delay = max(1.0, date.timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            pass
    with _lock:
        key = _key(endpoint)
        _until[key] = max(_until.get(key, 0), time.monotonic() + delay)
    raise RuntimeError(f"HTTP 429: fingerprint service rate-limited; retry in {math.ceil(delay)} seconds")
