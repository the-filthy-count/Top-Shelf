"""Public filmography lookups used only by the review/suggestion search."""
import hashlib
import re
import time
from datetime import datetime
from threading import Lock
from urllib.parse import urljoin, urlsplit

import requests
from lxml import html

import database as db
from scene_search_runtime import SourceSearchError, source_time_left

HOSTS = {'AFDB': 'www.adultfilmdatabase.com', 'Data18': 'www.data18.com',
         'EuroPornstar': 'www.europornstar.com'}
_cooldown = {}
_lock = Lock()
MAX_BYTES = 4_000_000


def _text(node):
    return ' '.join(node.text_content().split())


def _slug(name):
    return '-'.join(re.findall(r'[a-z0-9]+', name.casefold()))


def _path(anchor, source):
    url = urlsplit(urljoin('https://' + HOSTS[source], anchor.get('href', '')))
    if url.hostname == HOSTS[source] and not url.fragment and url.scheme in ('https', 'http'):
        return url.path
    return ''


def _fetch(source, path, form=None):
    host = HOSTS[source]
    with _lock:
        if _cooldown.get(source, 0) > time.monotonic():
            raise SourceSearchError('temporarily unavailable')
    url = 'https://' + host + path
    for _ in range(4):
        with requests.request('POST' if form else 'GET', url, data=form, stream=True,
                              allow_redirects=False, timeout=min(source_time_left() or 8, 8)) as response:
            if response.status_code in (403, 429):
                with _lock:
                    _cooldown[source] = time.monotonic() + 900
                raise SourceSearchError('source denied automated access')
            if response.is_redirect:
                url = urljoin(url, response.headers['Location'])
                if urlsplit(url).netloc != host or urlsplit(url).scheme != 'https':
                    raise SourceSearchError('unexpected redirect')
                form = None
                continue
            if response.status_code == 404:
                return None
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_content(65536):
                body.extend(chunk)
                if len(body) > MAX_BYTES:
                    raise SourceSearchError('page too large')
            return html.fromstring(bytes(body))
    raise SourceSearchError('too many redirects')


def _row(source, key, title, path, cast, studio='', date=''):
    return {'id': key, '_source': source, '_review_url': 'https://' + HOSTS[source] + path,
            'title': title, 'performers': [{'performer': {'name': name}} for name in cast],
            'studio': {'name': studio}, 'release_date': date, 'images': []}


def data18_profile(tree):
    heads = tree.xpath('//h1/a[@href]')
    if not heads or not re.fullmatch(r'/name/[a-z0-9-]+', _path(heads[0], 'Data18')):
        raise SourceSearchError('a Data18 performer profile is required')
    name = _text(heads[0])
    rows = {}
    for anchor in tree.xpath('//a[@href]'):
        match = re.fullmatch(r'/scenes/(\d+)', _path(anchor, 'Data18'))
        title = _text(anchor)
        if not match or not title or title == 'Pic Scene':
            continue
        card = anchor.getparent().getparent()
        ids = {_path(a, 'Data18') for a in card.xpath('.//a[@href]') if re.fullmatch(r'/scenes/\d+', _path(a,'Data18'))}
        if len(ids) != 1:
            continue
        cast, studio, date = [name], '', ''
        for link in card.xpath('.//a[@href]'):
            path = _path(link, 'Data18')
            label = _text(link)
            if label and re.fullmatch(r'/name/[a-z0-9-]+', path) and label not in cast:
                cast.append(label)
            if label and re.fullmatch(r'/studios/[a-z0-9-]+', path):
                studio = label
        stamp = re.search(r'\b([A-Z][a-z]{2,8}) (\d{1,2}), (\d{4})\b', _text(card))
        if stamp:
            try:
                date = datetime.strptime(stamp[1][:3]+' '+stamp[2]+' '+stamp[3], '%b %d %Y').date().isoformat()
            except ValueError:
                pass
        rows[match[1]] = _row('Data18', match[1], title, match[0], cast, studio, date)
    return name, list(rows.values())


def import_data18(raw):
    if len(raw) > MAX_BYTES:
        raise ValueError('HTML file too large')
    name, rows = data18_profile(html.fromstring(raw))
    if not rows:
        raise ValueError('No scene credits found on this profile')
    key = 'review:Data18:import:' + _slug(name)
    previous = db.app_json_cache_get(key) or []
    merged = {r['id']:r for r in [*previous,*rows]}
    db.app_json_cache_put(key,list(merged.values()))
    from scene_search_runtime import invalidate_source
    invalidate_source('Data18')
    return {'performer':name,'scenes':len(rows)}


def _filmography(source, name):
    slug = _slug(name)
    if source == 'Data18':
        tree = _fetch(source, '/name/' + slug)
        if tree is None:
            return []
        credited, rows = data18_profile(tree)
        return rows if _slug(credited) == slug else []
    if source == 'AFDB':
        tree = _fetch(source, '/lookup.cfm', {'find':name,'searchType':'Actor','exact':'0'})
        if tree is None:
            return []
        profiles = {_path(a,source) for a in tree.xpath('//a[@href]') if
                    re.fullmatch('/actor/'+re.escape(slug)+r'-\d+/', _path(a,source))}
        if len(profiles) != 1:
            return []
        tree = _fetch(source,profiles.pop())
        if tree is None or not any(_slug(_text(h)) == slug for h in tree.xpath('//h1')):
            return []
        rows = {}
        for link in tree.xpath('//a[@actorthumb]'):
            path = _path(link,source)
            hit = re.fullmatch(r'/video/(\d+)/[^/]+/',path)
            if hit and _text(link):
                tr = link.xpath('ancestor::tr[1]')
                studios = tr[0].xpath('.//a[starts-with(@href,"/studio/")]') if tr else []
                year = re.search(r'\b(?:19|20)\d{2}\b', _text(tr[0]).replace(_text(link),'',1)) if tr else None
                rows[hit[1]] = _row(source,hit[1],_text(link),path,[name],
                                    _text(studios[0]) if studios else '', year[0] if year else '')
        return list(rows.values())
    path = '/' + '-'.join(word.capitalize() for word in slug.split('-')) + '/'
    tree = _fetch(source,path)
    if tree is None or not any(_slug(_text(t)).startswith(slug+'-porn-galleries-') for t in tree.xpath('//title')):
        return []
    rows = []
    for link in tree.xpath('//a[@class="model" and @href]'):
        title = _text(link)
        if title:
            key = hashlib.sha256(link.get('href').encode()).hexdigest()[:20]
            rows.append(_row(source,key,'Gallery: '+title,path,[name]))
    return rows


def _afdb_title(title):
    tree = _fetch('AFDB', '/lookup.cfm', {'find':title, 'searchType':'All', 'exact':'0'})
    if tree is None:
        return []
    paths = {}
    for link in tree.xpath('//a[@href]'):
        path = _path(link, 'AFDB')
        match = re.fullmatch(r'/video/(\d+)/[^/]+/', path)
        if match and set(_slug(title).split('-')).intersection(path.rstrip('/').split('/')[-1].split('-')):
            paths.setdefault(match[1], path)
    rows = []
    for key, path in list(paths.items())[:3]:
        page = _fetch('AFDB', path)
        if page is None:
            continue
        heading = page.xpath('//h1')
        studios = page.xpath('//a[starts-with(@href,"/studio/")]')
        cast = list(dict.fromkeys(_text(a) for a in page.xpath('//a[starts-with(@href,"/actor/")]') if _text(a)))
        if heading and (studios or cast):
            year = re.search(r'\((\d{4})\)', _text(studios[0].getparent())) if studios else None
            rows.append(_row('AFDB', key, _text(heading[0]), path, cast,
                             _text(studios[0]) if studios else '', year[1] if year else ''))
    return rows


def search(source, performer, title):
    name = (performer or '').split(',')[0].strip()
    if not _slug(name):
        if source == "AFDB" and title:
            return [(row,source) for row in _afdb_title(title)]
        return []
    key = 'review:'+source+':'+_slug(name)
    imported = db.app_json_cache_get('review:Data18:import:'+_slug(name)) if source == 'Data18' else None
    cached = db.app_json_cache_get(key)
    if imported:
        rows = imported
    elif cached and time.time()-cached['time'] < 86400:
        rows = cached['rows']
    else:
        rows = _filmography(source,name)
        db.app_json_cache_put(key,{'time':time.time(),'rows':rows})
    tokens = set(_slug(title or '').split('-')) - {''}
    def overlap(row):
        return len(tokens.intersection(_slug(row['title']).split('-')))
    ranked = sorted((r for r in rows if not tokens or overlap(r)),key=overlap,reverse=True)
    return [(r,source) for r in ranked[:30]]
