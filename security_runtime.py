"""Small, bounded guards for browser authentication."""
from collections import OrderedDict
from threading import Lock
from time import monotonic
from urllib.parse import urlsplit


def same_origin_write(request):
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return True
    if request.headers.get("sec-fetch-site") == "cross-site":
        return False
    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        # Non-browser API clients do not send browser provenance headers.
        return request.headers.get("sec-fetch-site") not in {"same-site"}
    try:
        source = urlsplit(origin)
        target = urlsplit(str(request.url))
        return (source.scheme in {"http", "https"}
                and (source.scheme, source.hostname, source.port or (443 if source.scheme == "https" else 80))
                == (target.scheme, target.hostname, target.port or (443 if target.scheme == "https" else 80)))
    except ValueError:
        return False


class LoginLimiter:
    """Bounded per-peer attempt budget, including simultaneous attempts."""
    def __init__(self):
        self.entries = OrderedDict()
        self.lock = Lock()

    def allow(self, peer):
        now = monotonic()
        with self.lock:
            start, count = self.entries.get(peer, (now, 0))
            if now - start >= 300:
                start, count = now, 0
            self.entries[peer] = (start, count + 1)
            self.entries.move_to_end(peer)
            while len(self.entries) > 4096:
                self.entries.popitem(last=False)
            return count < 10


login_limiter = LoginLimiter()
