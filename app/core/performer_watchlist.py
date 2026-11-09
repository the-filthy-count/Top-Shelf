"""Unidentified performers: persisted leads, explicit identity confirmation."""
import json
import logging
import threading
import time
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, quote, unquote
import requests
import database as db

SOURCES = {'TPDB': ('api_key_tpdb', 'https://api.theporndb.net/performers'),
           'StashDB': ('api_key_stashdb', 'https://stashdb.org/graphql'),
           'FansDB': ('api_key_fansdb', 'https://fansdb.cc/graphql'),
           'JAVStash': ('api_key_javstash', 'https://javstash.org/graphql')}
EXTERNAL_SOURCES = ('Babepedia', 'Coomer')
_creators_lock = threading.Lock()
_creators_cache = (0, [])
_wake = threading.Event()
_thread = None

def install(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS performer_watchlist (
        id INTEGER PRIMARY KEY, name TEXT NOT NULL, aliases TEXT NOT NULL DEFAULT '[]',
        notes TEXT NOT NULL DEFAULT '', source_url TEXT NOT NULL DEFAULT '',
        candidates TEXT NOT NULL DEFAULT '[]', checks TEXT NOT NULL DEFAULT '{}',
        confirmed TEXT, checked_at REAL, next_check REAL NOT NULL DEFAULT 0,
        checking INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1)''')
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS watchlist_name ON performer_watchlist(lower(name))')

def validate(payload):
    name = str(payload.get('name') or '').strip()
    aliases = payload.get('aliases') or []
    if not name or len(name) > 150: raise ValueError('Enter a name or alias (up to 150 characters)')
    if not isinstance(aliases, list) or any(not isinstance(a, str) for a in aliases) or len(aliases) > 10: raise ValueError('Use up to 10 additional aliases')
    aliases = list(dict.fromkeys(str(a).strip() for a in aliases if str(a).strip()))
    if any(len(a) > 150 for a in aliases): raise ValueError('Aliases must be under 150 characters')
    notes = str(payload.get('notes') or '').strip()
    url = str(payload.get('source_url') or '').strip()
    if len(notes) > 2000 or len(url) > 2000: raise ValueError('Notes or URL are too long')
    if url and (urlparse(url).scheme not in ('https', 'http') or not urlparse(url).netloc):
        raise ValueError('Source link must be an HTTP or HTTPS URL')
    return name, json.dumps(aliases), notes, url

def name_key(value):
    return ''.join(c for c in unicodedata.normalize('NFKC', str(value or '')).casefold() if c.isalnum())

def matches_names(hit, names):
    wanted = {name_key(name) for name in names} - {''}
    aliases = hit.get('aliases') or []
    if isinstance(aliases, str): aliases = aliases.split(',')
    recorded = [hit.get('name') or ''] + [a.get('name', '') if isinstance(a, dict) else a for a in aliases]
    return any(name_key(name) in wanted for name in recorded)

def search_terms(names):
    terms = []
    for name in names:
        spaced = re.sub(r'[\s_]+', ' ', name).strip()
        for term in (spaced, spaced.replace(' ', '')):
            if term and term.casefold() not in {t.casefold() for t in terms}: terms.append(term)
    return terms

def rows():
    with db.get_conn() as c:
        result = [dict(r) for r in c.execute('SELECT * FROM performer_watchlist ORDER BY lower(name),id')]
    for r in result:
        for k in ('aliases','candidates','checks','confirmed'): r[k] = json.loads(r[k]) if r[k] else None
        r['candidates'] = [h for h in r['candidates'] if matches_names(h, [r['name']] + (r['aliases'] or []))]
    return result

def search_source(source, names, settings):
    key, endpoint = SOURCES[source]
    if not settings.get(key): return [], 'Not configured'
    hits = {}
    try:
        for name in search_terms(names):
            if source == 'TPDB':
                response = requests.get(endpoint, params={'q': name}, headers={'Authorization': 'Bearer '+settings[key]}, timeout=15)
            else:
                query = 'query($name: String!) { searchPerformer(term: $name, limit: 25) { id name aliases images { url } } }'
                response = requests.post(endpoint, json={'query': query, 'variables': {'name': name}}, headers={'ApiKey': settings[key]}, timeout=15)
            response.raise_for_status()
            body = response.json()
            if body.get('errors'): raise ValueError('Database returned an error')
            data = body.get('data') if source == 'TPDB' else (body.get('data') or {}).get('searchPerformer')
            if not isinstance(data, list): raise ValueError('Unexpected database response')
            for item in data:
                if not matches_names(item, names): continue
                identity = str(item.get('id') or '')
                if not identity: continue
                images = item.get('posters') or item.get('images') or []
                image = images[0] if images else ''
                if isinstance(image, dict): image = image.get('url') or ''
                if urlparse(str(image)).scheme not in ('http','https'): image = ''
                hits[identity] = {'source':source,'id':identity,'name':str(item.get('name') or name),
                                  'image':image,'aliases':item.get('aliases') or []}
        return list(hits.values()), 'Checked'
    except Exception:
        # Never turn a failed request into a definitive "not found".
        return list(hits.values()), 'Check failed — retry later'

def search_external(source, names):
    """Supplementary profile leads, never library identity confirmations."""
    hits = {}
    try:
        if source == 'Coomer':
            global _creators_cache
            # The creator catalogue is shared across all watchlist entries.
            with _creators_lock:
                if time.time() - _creators_cache[0] > 3600:
                    response = requests.get('https://coomer.st/api/v1/creators.txt',
                                            headers={'Accept': 'text/css'}, timeout=30)
                    response.raise_for_status()
                    creators = response.json()
                    if not isinstance(creators, list): raise ValueError('Unexpected creator catalogue')
                    _creators_cache = (time.time(), creators)
                creators = _creators_cache[1]
            for creator in creators:
                service, identity = creator.get('service'), str(creator.get('id') or '')
                if service not in ('onlyfans', 'fansly') or not identity: continue
                hit = {'name': creator.get('name') or identity, 'aliases': [identity]}
                if not matches_names(hit, names): continue
                url = f'https://coomer.st/{service}/user/{quote(identity, safe="")}'
                hits[url] = dict(hit, source=source, id=url, url=url, image='', external_only=True)
        else:
            from lxml import html
            for term in search_terms(names):
                response = requests.get('https://www.babepedia.com/search.php', params={'q': term},
                                        headers={'User-Agent': 'Top-Shelf/1.0'}, timeout=20)
                response.raise_for_status()
                tree = html.fromstring(response.content)
                if not tree.xpath('//a[starts-with(@href, "/babe/")]') and re.search(
                        r'just a moment|captcha|access denied', response.text, re.I):
                    raise ValueError('Search blocked')
                for anchor in tree.xpath('//a[starts-with(@href, "/babe/")]'):
                    href = anchor.get('href').split('?')[0].rstrip('/')
                    slug = href.rsplit('/', 1)[-1]
                    name = unquote(slug).replace('_', ' ')
                    captions = anchor.xpath('.//figcaption/text() | .//span[contains(@class,"name")]/text()')
                    hit = {'name': next((t.strip() for t in captions if t.strip()), name), 'aliases': [name]}
                    if not matches_names(hit, names): continue
                    url = 'https://www.babepedia.com' + href + '/'
                    hits[url] = dict(hit, source=source, id=url, url=url, image='', external_only=True)
        return list(hits.values()), 'Checked'
    except Exception:
        return list(hits.values()), 'Check failed — retry later'


def check_entry(row):
    names = list(dict.fromkeys([row['name']] + json.loads(row['aliases'])))
    settings = db.get_settings()
    with ThreadPoolExecutor(max_workers=4) as pool:
        sources = list(SOURCES) + list(EXTERNAL_SOURCES)
        results = list(pool.map(lambda source: search_source(source, names, settings) if source in SOURCES else search_external(source, names), sources))
    candidates = [hit for hits, _ in results for hit in hits]
    checks = dict(zip(sources, (status for _, status in results)))
    with db.get_conn() as c:
        c.execute('''UPDATE performer_watchlist SET candidates=?,checks=?,checked_at=?,
            next_check=?,checking=0 WHERE id=? AND revision=?''',
            (json.dumps(candidates),json.dumps(checks),time.time(),time.time()+86400,row['id'],row['revision']))
        c.commit()

def start():
    global _thread
    if _thread and _thread.is_alive(): return
    with db.get_conn() as c:
        c.execute('UPDATE performer_watchlist SET checking=0'); c.commit()
    def run():
        while True:
            try:
                with db.get_conn() as c:
                    c.execute('BEGIN IMMEDIATE')
                    row = c.execute('SELECT * FROM performer_watchlist WHERE confirmed IS NULL AND checking=0 AND next_check<=? ORDER BY next_check,id LIMIT 1', (time.time(),)).fetchone()
                    if row:
                        row = dict(row)
                        c.execute('UPDATE performer_watchlist SET checking=1 WHERE id=?',(row['id'],)); c.commit()
                if row:
                    try: check_entry(row)
                    except Exception:
                        logging.exception('Watchlist check failed')
                        with db.get_conn() as c:
                            c.execute('UPDATE performer_watchlist SET checking=0,next_check=? WHERE id=? AND revision=?',(time.time()+3600,row['id'],row['revision'])); c.commit()
                    continue
            except Exception: logging.exception('Watchlist worker failed')
            _wake.wait(60); _wake.clear()
    _thread = threading.Thread(target=run,name='performer-watchlist',daemon=True); _thread.start()
