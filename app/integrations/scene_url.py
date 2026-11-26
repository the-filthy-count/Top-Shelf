"""Read scene metadata directly from supported sites or public page metadata."""
import ipaddress
import json
import re
import socket
import time
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlsplit, urljoin

import requests


def validate_url(url):
    if not isinstance(url, str) or len(url) > 4096:
        raise ValueError('Enter a valid webpage URL')
    p = urlsplit(url)
    if p.scheme not in ('http', 'https') or not p.hostname or p.username or p.password:
        raise ValueError('Enter a public http(s) webpage URL')
    addresses = socket.getaddrinfo(p.hostname, p.port or (443 if p.scheme == 'https' else 80), type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
        raise ValueError('Use a public webpage URL')
    return url


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.meta, self.json, self.title = {}, [], ''
        self.in_title = self.in_json = False
        self.script = ''

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'meta':
            self.meta[(a.get('property') or a.get('name') or '').lower()] = a.get('content', '')
        if tag == 'title':
            self.in_title = True
        if tag == 'script' and a.get('type', '').lower() == 'application/ld+json':
            self.in_json, self.script = True, ''

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        if self.in_json:
            self.script += data

    def handle_endtag(self, tag):
        if tag == 'title':
            self.in_title = False
        if tag == 'script' and self.in_json:
            self.in_json = False
            try:
                self.json.append(json.loads(self.script))
            except ValueError:
                pass


def names(value):
    if not isinstance(value, list):
        value = [value]
    return [name for name in dict.fromkeys(str(x.get('name', '') if isinstance(x, dict) else x).strip()
                             for x in value if x) if name]


def parse_page(html, url):
    page = Page()
    page.feed(html)
    nodes = []
    def walk(value):
        if isinstance(value, list):
            for x in value:
                walk(x)
        elif isinstance(value, dict):
            types = value.get('@type', [])
            if isinstance(types, str):
                types = [types]
            if any(t in ('VideoObject', 'Movie', 'Clip') for t in types):
                nodes.append(value)
            for key in ('@graph', 'mainEntity', 'video'):
                walk(value.get(key))
    for data in page.json:
        walk(data)
    node = next((n for n in nodes if n.get('url') == url), nodes[0] if nodes else {})
    meta = page.meta
    image = node.get('thumbnailUrl') or meta.get('og:image') or meta.get('twitter:image') or ''
    if isinstance(image, list):
        image = image[0] if image else ''
    return {
        'title': unescape(str(node.get('name') or meta.get('og:title') or page.title)).strip(),
        'description': unescape(str(node.get('description') or meta.get('og:description') or meta.get('description') or '')).strip(),
        'date': str(node.get('datePublished') or node.get('uploadDate') or meta.get('article:published_time') or '')[:10],
        'studio': next(iter(names(node.get('productionCompany') or meta.get('og:site_name'))), ''),
        'performers': names(node.get('actor') or node.get('actors') or []),
        'tags': names(node.get('keywords').split(',') if isinstance(node.get('keywords'), str) else node.get('keywords') or []),
        'image': urljoin(url, image) if isinstance(image, str) and image else '',
        'url': url,
    }


def fetch_page(url):
    deadline = time.monotonic() + 35
    with requests.Session() as session:
        session.trust_env = False
        for _ in range(6):
            validate_url(url)
            with session.get(url, allow_redirects=False, stream=True, timeout=(5, 10),
                             headers={'User-Agent': 'Mozilla/5.0 (compatible; Top-Shelf)'}) as response:
                if response.is_redirect:
                    target = urljoin(url, response.headers.get('Location', ''))
                    target_path = urlsplit(target).path.rstrip('/')
                    if target_path != urlsplit(url).path.rstrip('/') and target_path in ('', '/sfw', '/login', '/signin', '/join'):
                        raise ValueError('The site redirected to its homepage or login page. No scene details were imported.')
                    url = target
                    continue
                response.raise_for_status()
                if 'html' not in response.headers.get('Content-Type', '').lower():
                    raise ValueError('The URL did not return a webpage')
                body = bytearray()
                for part in response.iter_content(65536):
                    body.extend(part)
                    if len(body) > 4 * 1024 * 1024 or time.monotonic() > deadline:
                        raise ValueError('Webpage is too large or took too long to load')
                return parse_page(body.decode(response.encoding or 'utf-8', errors='replace'), url)
    raise ValueError('Too many webpage redirects')


def scrape(url, settings):
    url = validate_url(url.strip())
    from app.integrations import vixen
    if vixen.identify(url):
        return vixen.scrape(url)
    data = fetch_page(url)
    if not data['title'] or re.search(r'just a moment|access denied|attention required', data['title'], re.I):
        raise ValueError('No scene details found. The page may require login or a site-specific scraper.')
    data['provider'] = 'Webpage metadata'
    return data
