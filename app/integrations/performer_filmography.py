"""Persistent, paginated performer credits. Source failures retain the last good scan."""
import hashlib
import json
import re
import time
import threading
from urllib.parse import quote
import requests
import database as db
from runtime_jobs import KeyedExecutor

SOURCES = {'tpdb': ('TPDB', 'https://api.theporndb.net'),
           'stashdb': ('StashDB', 'https://stashdb.org'),
           'fansdb': ('FansDB', 'https://fansdb.cc'),
           'javstash': ('JAVStash', 'https://javstash.org')}
QUERY = '''query Filmography($input: SceneQueryInput!) { queryScenes(input: $input) {
 scenes { id title release_date studio { name } performers { performer { id } } }
} }'''
_active = set()
_lock = threading.Lock()
_jobs = KeyedExecutor(max_workers=2, pending=16, thread_name_prefix='filmography')


def identity(row):
    try:
        groups = json.loads(row.get('group_ids_json') or '{}')
    except (TypeError, ValueError):
        groups = {}
    result = {}
    for source in SOURCES:
        ids = {str(row.get('match_' + source + '_id') or '').strip()}
        for item in groups.get(source, []) if isinstance(groups, dict) else []:
            ids.add(str(item.get('id') or '') if isinstance(item, dict) else str(item))
        result[source] = sorted(i for i in ids if i)
    return result


def cache_key(ids):
    return 'filmography:v1:' + hashlib.sha256(json.dumps(ids, sort_keys=True).encode()).hexdigest()


def fetch_source(source, performer_id, api_key, session=requests):
    label, host = SOURCES[source]
    rows, seen = [], set()
    for page in range(1, 1001):
        if source == 'tpdb':
            response = session.get(host + '/performers/' + quote(performer_id, safe='') + '/scenes',
                headers={'Authorization': 'Bearer ' + api_key, 'Accept': 'application/json'},
                params={'page': page, 'per_page': 100}, timeout=30)
        else:
            response = session.post(host + '/graphql', headers={'ApiKey': api_key},
                json={'query': QUERY, 'variables': {'input': {'page': page, 'per_page': 50,
                'sort': 'DATE', 'direction': 'DESC',
                'performers': {'value': [performer_id], 'modifier': 'INCLUDES'}}}}, timeout=30)
        response.raise_for_status()
        body = response.json()
        if body.get('errors'):
            raise ValueError('Source returned GraphQL errors')
        batch = body.get('data') if source == 'tpdb' else (body.get('data') or {}).get('queryScenes', {}).get('scenes')
        if not isinstance(batch, list):
            raise ValueError('Source returned no scene list')
        fresh = 0
        for item in batch:
            sid = str(item.get('id') or '')
            if not sid or sid in seen:
                continue
            seen.add(sid); fresh += 1
            date = str(item.get('date' if source == 'tpdb' else 'release_date') or '')
            studio = item.get('site' if source == 'tpdb' else 'studio') or {}
            rows.append({'id': sid, 'source': label, 'title': item.get('title') or '',
                         'date': date, 'year': date[:4], 'studio': studio.get('name') or '',
                         'url': ('https://theporndb.net' if source == 'tpdb' else host) + '/scenes/' + quote(sid, safe='')})
        if source == 'tpdb':
            last = (body.get('meta') or {}).get('last_page')
            finished = page >= int(last) if last is not None else not (body.get('links') or {}).get('next')
        else:
            finished = len(batch) < 50
        if finished:
            return rows
        if not fresh:
            raise ValueError('Pagination repeated a page; scan incomplete')
    raise ValueError('Pagination safety limit reached; scan incomplete')


def dedupe(rows):
    def norm(value):
        return ' '.join(re.findall(r'\w+', str(value).casefold()))
    merged, keys = [], {}
    for row in rows:
        # Cross-source merge requires exact title, date AND studio; unknowns never merge.
        exact = ('id', row['source'], row['id'])
        title, studio, date = norm(row['title']), norm(row['studio']), row['date']
        cross = ('scene', title, date, studio) if title and studio and len(date) == 10 else exact
        index = keys.get(exact, keys.get(cross))
        if index is not None and exact not in keys and any(link['source'] == row['source'] for link in merged[index]['sources']):
            index = None
        if index is None:
            index = len(merged)
            merged.append(dict(row, sources=[]))
        keys[exact] = keys[cross] = index
        link = {'source': row['source'], 'id': row['id'], 'url': row['url']}
        if link not in merged[index]['sources']:
            merged[index]['sources'].append(link)
    return sorted(merged, key=lambda row: (row['date'], row['title'].casefold()), reverse=True)


def refresh(ids):
    settings = db.get_settings()
    key = cache_key(ids)
    statuses, all_rows = [], []
    try:
        for source, performer_ids in ids.items():
            api_key = str(settings.get('api_key_' + source) or '').strip()
            if not performer_ids or not api_key:
                statuses.append({'source': SOURCES[source][0], 'state': 'Not linked' if not performer_ids else 'API key missing'})
                continue
            for performer_id in performer_ids:
                source_key = key + ':' + source + ':' + performer_id
                saved = db.app_json_cache_get(source_key) or {}
                try:
                    scenes = fetch_source(source, performer_id, api_key)
                    saved = {'scenes': scenes, 'updated_at': time.time()}
                    db.app_json_cache_put(source_key, saved)
                    state = 'Complete'
                except Exception:
                    state = 'Refresh failed; showing saved results' if saved else 'Refresh failed'
                all_rows.extend(saved.get('scenes') or [])
                statuses.append({'source': SOURCES[source][0], 'state': state, 'count': len(saved.get('scenes') or [])})
    finally:
        db.app_json_cache_put(key, {'films': dedupe(all_rows), 'sources': statuses, 'loading': False, 'updated_at': time.time()})


def read(ids):
    key = cache_key(ids)
    result = db.app_json_cache_get(key) or {'films': [], 'sources': []}
    with _lock:
        result['loading'] = key in _active
    return result


def start(ids):
    key = cache_key(ids)
    with _lock:
        if key in _active:
            return True
        _active.add(key)
    def run():
        try:
            refresh(ids)
        finally:
            with _lock:
                _active.discard(key)
    accepted = _jobs.submit_once(key, run)
    if not accepted:
        with _lock:
            _active.discard(key)
    return accepted
