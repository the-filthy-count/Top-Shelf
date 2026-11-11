"""FreeOnes name searches with explicit upstream errors."""
from urllib.parse import urljoin, urlsplit
import requests
from lxml import html

def parse_results(content):
    tree = html.fromstring(content)
    results = []
    seen = set()
    for card in tree.xpath('//*[@data-test="teaser-subject"]'):
        links = card.xpath('.//a[@href]/@href')
        names = card.xpath('.//*[@data-test="subject-name"]')
        if not links or not names:
            continue
        url = urljoin('https://www.freeones.com', links[0])
        parts = urlsplit(url)
        if parts.hostname not in ('freeones.com','www.freeones.com'):
            continue
        path = parts.path.rstrip('/')
        if path.endswith('/feed'): path = path[:-5]
        if not path.endswith('/bio'): path += '/bio'
        url = 'https://www.freeones.com' + path
        name = names[0].text_content().strip()
        if not name or url in seen: continue
        seen.add(url)
        images = card.xpath('.//img/@data-src') or card.xpath('.//img/@src')
        thumb = urljoin(url, images[0]) if images and not images[0].startswith('data:') else None
        results.append({'name':name,'url':url,'thumb_url':thumb})
    return results[:8]

def search(name):
    if not str(name or '').strip(): return []
    try:
        response = requests.get('https://www.freeones.com/babes',
            params={'q':name.strip(),'v':'teasers','s':'relevance','l':12}, timeout=(6,20))
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError('FreeOnes search unavailable: ' + str(exc)) from exc
    if any(marker in response.text.lower() for marker in ('just a moment...', 'cf-chl-', 'verify you are human')):
        raise RuntimeError('FreeOnes is requesting browser verification. Open FreeOnes and paste the profile link instead.')
    return parse_results(response.content)
