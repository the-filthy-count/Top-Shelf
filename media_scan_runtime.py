"""Coalesce scan triggers and check remote work before asking for another scan."""
from threading import Lock
from time import monotonic
import requests
import logging

_log = logging.getLogger(__name__)

_lock = Lock()
_last = {}
_active = set()


def busy(server, settings):
    url = str(settings.get(server + '_url') or '').rstrip('/')
    key = settings.get('plex_token' if server == 'plex' else server + '_api_key', '')
    if server == 'stash':
        response = requests.post(url + '/graphql', json={'query': '{ jobQueue { status } }'},
                                 headers={'ApiKey': key}, timeout=10)
        try:
            response.raise_for_status()
            payload = response.json()
        finally:
            response.close()
        data = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or payload.get('errors') or not isinstance(data, dict) or 'jobQueue' not in data:
            raise ValueError('Cannot verify Stash job queue')
        queue = data['jobQueue']
        # Stash's resolver returns a nil slice (JSON null) for an empty queue.
        if queue is None:
            return False
        if not isinstance(queue, list) or any(not isinstance(job, dict) or not isinstance(job.get('status'), str) for job in queue):
            raise ValueError('Cannot verify Stash job queue')
        return any(job['status'] not in ('FINISHED', 'CANCELLED', 'FAILED') for job in queue)
    if server in ('jellyfin', 'emby'):
        response = requests.get(url + '/ScheduledTasks', headers={'X-Emby-Token': key}, timeout=10)
        try:
            response.raise_for_status()
            tasks = response.json()
        finally:
            response.close()
        if not isinstance(tasks, list):
            raise ValueError('Cannot verify media server tasks')
        return any(task.get('State') in ('Running', 'Cancelling') for task in tasks)
    # Plex's scan status is not consistent across supported versions. Apply the
    # cooldown without pretending it is a confirmed completion check.
    return False


def dispatch(server, settings, send, defer):
    with _lock:
        if server in _active:
            defer(60)
            return
        remaining = max(0, 1800 - (monotonic() - _last.get(server, -1800)))
        if remaining:
            defer(remaining)
            return
        _active.add(server)
    try:
        if busy(server, settings):
            defer(60)
            return
        send()
        with _lock:
            _last[server] = monotonic()
    except Exception as exc:
        defer(300)
        _log.warning("%s scan deferred for 300s after %s; pending scan retained", server, type(exc).__name__)
    finally:
        with _lock:
            _active.discard(server)
