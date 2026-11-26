"""Choose among a database's fingerprint hits without depending on response order."""


def distance(scene, phash):
    from app.core.video_matching.decisions import distance as measure
    return measure(scene,phash)


def select_scene(matches, phash):
    from app.core.video_matching.decisions import fingerprint
    return fingerprint(matches,phash).candidate


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
