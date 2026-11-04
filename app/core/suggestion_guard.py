"""One active suggestion search per queue filename across all entry points."""
from threading import Lock

_lock = Lock()
_active = set()


def claim(filename):
    with _lock:
        if filename in _active:
            return False
        _active.add(filename)
        return True


def release(filename):
    with _lock:
        _active.discard(filename)


def bind_saved_identities(scene, performers, saved, studio_link=None):
    """Reject known ID conflicts and attach verified identities to text hits."""
    source = scene.get('_source') or ''
    actual = {str((p.get('performer') or p).get('id') or '')
              for p in scene.get('performers') or [] if isinstance(p, dict)}
    proof = dict((scene.get('_search_performer_ids') or {}).get('ids') or {})
    for name in performers:
        ids = saved.get(name, {}).get(source)
        if ids is None:
            continue
        ids = {str(value) for value in ids}
        if len(ids) != 1 or not ids.issubset(actual):
            return None
        proof[name] = next(iter(ids))
    result = dict(scene, _search_performer_ids={'source': source, 'ids': proof})
    studio_id = (studio_link or {}).get('source_ids', {}).get(source)
    if studio_id:
        actual_studio = scene.get('studio') or {}
        matches = isinstance(actual_studio, dict) and str(actual_studio.get('id') or '') == str(studio_id)
        if not matches and not performers:
            return None
        if matches:
            result['_search_studio_id'] = dict(source=source, id=str(studio_id), name=studio_link['name'])
    return result
