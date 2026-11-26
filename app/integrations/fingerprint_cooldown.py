"""Process-wide fingerprint endpoint cooldown shared by sync and async lookups."""
from contextvars import ContextVar
from contextlib import contextmanager
import asyncio
import logging
import math
import threading
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

_lock = threading.Lock()
_until = {}
_logger = logging.getLogger(__name__)


class RateLimited(RuntimeError):
    def __init__(self, message, retry_after=300):
        super().__init__(message)
        self.retry_after = max(1, float(retry_after))

_defer = ContextVar('defer_provider_wait', default=False)
_waits = ContextVar('provider_waits_for_job', default=None)

def deferred_retry():
    return max((remaining(endpoint) for endpoint in (_waits.get() or [])), default=0)

def _record_wait(endpoint):
    waits = _waits.get()
    if waits is not None:
        waits.append(endpoint)

@contextmanager
def defer_waits():
    token = _defer.set(True)
    wait_token = _waits.set([])
    try:
        yield
    finally:
        _defer.reset(token)
        _waits.reset(wait_token)


def remaining(endpoint):
    with _lock:
        return max(0, _until.get(_key(endpoint), 0) - time.monotonic())


def active_waits():
    """Public host names and remaining seconds only; never expose credentials."""
    with _lock:
        now = time.monotonic()
        return [{"provider": urlsplit("//" + host).hostname or "Search service",
                 "seconds": math.ceil(until - now)}
                for host, until in _until.items() if until > now]


def send(endpoint, request):
    """Keep this lookup in place until the service accepts the request."""
    while True:
        delay = remaining(endpoint)
        if delay > 0 and _defer.get():
            _record_wait(endpoint)
            raise RateLimited('Provider cooling down', delay)
        if delay > 0:
            time.sleep(min(delay, 30))
            continue
        response = request()
        if response.status_code != 429:
            return response
        retry_after = response.headers.get("Retry-After")
        response.close()
        try:
            rate_limited(endpoint, retry_after)
        except RateLimited as exc:
            if _defer.get():
                _record_wait(endpoint)
                raise
            _logger.warning("%s: %s; pausing this lookup", _key(endpoint), exc)


async def send_async(endpoint, request):
    """Cancellable wait that leaves the event loop free for the interface."""
    while True:
        delay = remaining(endpoint)
        if delay > 0 and _defer.get():
            _record_wait(endpoint)
            raise RateLimited('Provider cooling down', delay)
        if delay > 0:
            await asyncio.sleep(min(delay, 30))
            continue
        response = await request()
        if response.status_code != 429:
            return response
        retry_after = response.headers.get("Retry-After")
        await response.aclose()
        try:
            rate_limited(endpoint, retry_after)
        except RateLimited as exc:
            if _defer.get():
                _record_wait(endpoint)
                raise
            _logger.warning("%s: %s; pausing this lookup", _key(endpoint), exc)



def _key(endpoint):
    return urlsplit(endpoint).netloc.lower()


def check(endpoint):
    with _lock:
        remaining = _until.get(_key(endpoint), 0) - time.monotonic()
    if remaining > 0:
        raise RateLimited(f"Fingerprint service rate-limited; retry in {math.ceil(remaining)} seconds")


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
    raise RateLimited(f"HTTP 429: fingerprint service rate-limited; retry in {math.ceil(delay)} seconds", remaining(endpoint))
