"""TPDB joint-cast lookup via its paginated REST performer filmography."""
from urllib.parse import quote


def joint_scenes(performer_ids, api_key, get, max_pages=20):
    wanted = set(performer_ids)
    if len(wanted) < 2:
        return [], True
    url = 'https://api.theporndb.net/performers/' + quote(performer_ids[0], safe='') + '/scenes'
    out, seen = [], set()
    for page in range(1, max_pages + 1):
        response = get(url, headers={'Accept': 'application/json', 'Authorization': 'Bearer ' + api_key},
                       params={'page': page, 'per_page': 100}, timeout=30)
        response.raise_for_status()
        body = response.json()
        rows = body.get('data')
        if not isinstance(rows, list):
            raise ValueError('TPDB returned no scene list')
        for row in rows:
            cast = []
            for performer in row.get('performers') or []:
                # Site-specific cast IDs point to their canonical parent.
                parent = performer.get('parent') or {}
                canonical = parent if parent.get('id') else performer
                cast.append({'performer': {'id': canonical.get('id'),
                                          'name': canonical.get('name') or performer.get('name'),
                                          'gender': (canonical.get('extras') or {}).get('gender')}})
            if not wanted.issubset({p['performer']['id'] for p in cast}):
                continue
            sid = row.get('id')
            if not sid or sid in seen:
                continue
            seen.add(sid)
            image = row.get('image')
            out.append({'id': sid, 'title': row.get('title'), 'details': row.get('description'),
                        'release_date': row.get('date'), 'duration': row.get('duration'),
                        'studio': row.get('site') or {}, 'performers': cast,
                        'tags': row.get('tags') or [],
                        'images': [{'url': image}] if isinstance(image, str) and image else [],
                        '_source': 'TPDB'})
        last_page = (body.get('meta') or {}).get('last_page')
        if last_page is not None:
            if page >= int(last_page):
                return out, True
        elif not (body.get('links') or {}).get('next'):
            return out, True
    return out, False  # Never treat a bounded, incomplete scan as a definitive miss.
