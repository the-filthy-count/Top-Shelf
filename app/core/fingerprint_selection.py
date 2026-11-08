"""Choose among a database's fingerprint hits without depending on response order."""
import re


def distance(scene, phash):
    value = str(phash or '').strip()
    if not re.fullmatch(r'[0-9a-fA-F]{16}', value):
        return None
    fingerprints = scene.get('fingerprints') or scene.get('hashes') or []
    if isinstance(fingerprints, dict):
        fingerprints = [{'algorithm': key, 'hash': val} for key, val in fingerprints.items()]
    distances = []
    for fp in fingerprints:
        if not isinstance(fp, dict):
            continue
        algorithm = str(fp.get('algorithm') or fp.get('type') or '').upper()
        candidate = str(fp.get('hash') or fp.get('value') or '').strip()
        if algorithm == 'PHASH' and re.fullmatch(r'[0-9a-fA-F]{16}', candidate):
            distances.append((int(value, 16) ^ int(candidate, 16)).bit_count())
    return min(distances) if distances else None


def select_scene(matches, phash):
    # Collapse repeated records for one ID, retaining its best fingerprint evidence.
    unique = {}
    for scene in matches or []:
        if not isinstance(scene, dict) or not scene.get('id'):
            continue
        sid = str(scene['id'])
        dist = distance(scene, phash)
        previous = unique.get(sid)
        if previous is None or (dist is not None and (previous[0] is None or dist < previous[0])):
            unique[sid] = (dist, scene)
    if len(unique) == 1:
        return next(iter(unique.values()))[1]
    # Unavailable fingerprints cannot establish which competing scene is closest.
    if not unique or any(d is None for d, _ in unique.values()):
        return None
    best = min(d for d, _ in unique.values())
    winners = [scene for d, scene in unique.values() if d == best]
    return winners[0] if len(winners) == 1 else None


def save_review(db, filename, matches, source, phash):
    """Preserve ambiguous hits in the existing suggestions review queue."""
    rows = {}
    for scene in matches:
        if not isinstance(scene, dict) or not scene.get('id'):
            continue
        dist = distance(scene, phash)
        payload = dict(scene, _source=source, _fingerprint_review=True)
        payload['_reasons'] = [{'label': 'Fingerprint ambiguity — review required', 'kind': 'warning'}]
        images = scene.get('images') or []
        rows[str(scene['id'])] = {
            'source_db': source, 'source_id': str(scene['id']),
            'score': (64-dist)/64 if dist is not None else 0,
            'title': scene.get('title') or '', 'date': scene.get('release_date') or '',
            'studio': (scene.get('studio') or {}).get('name') or '',
            'performers': ', '.join((p.get('performer') or p).get('name') or '' for p in scene.get('performers') or []),
            'thumb_url': images[0].get('url', '') if images else '', 'payload': payload,
        }
    db.update_file(filename, status='unmatched')
    db.queue_suggestions_add(filename, list(rows.values()))
