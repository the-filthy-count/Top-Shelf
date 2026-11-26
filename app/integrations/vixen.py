"""Direct Vixen Media Group metadata adapter (no Stash runtime).

API fields documented by CommunityScrapers' vixenNetwork adapter:
https://github.com/stashapp/CommunityScrapers/tree/master/scrapers/vixenNetwork
"""
import re
from urllib.parse import urlsplit

from curl_cffi import requests

SITES = {'vixen': 'Vixen', 'blacked': 'Blacked', 'blackedraw': 'Blacked Raw',
         'tushy': 'Tushy', 'tushyraw': 'Tushy Raw', 'deeper': 'Deeper',
         'slayed': 'Slayed', 'milfy': 'Milfy', 'wifey': 'Wifey'}
QUERY = '''query SceneMetadata($slug: String, $site: Site) {
  findOneVideo(input: {slug: $slug, site: $site}) {
    title slug description releaseDate videoId runLength
    models { name } categories { name } images { poster { src width } }
  }
}'''


def identify(url):
    parsed = urlsplit(url)
    host = (parsed.hostname or '').lower().removeprefix('www.')
    site = host.removesuffix('.com')
    path = re.fullmatch(r'/videos/([a-zA-Z0-9_-]+)/?', parsed.path)
    if host != site + '.com' or site not in SITES or not path:
        return None
    return site, path[1]


def normalize(video, site, slug):
    if not isinstance(video, dict) or not video.get('title'):
        raise ValueError('No scene found at that Vixen network URL')
    if video.get('slug') != slug:
        raise ValueError('The site returned a different scene; nothing was imported')
    images = (video.get('images') or {}).get('poster') or []
    images = [i for i in images if isinstance(i, dict) and str(i.get('src', '')).startswith('https://')]
    image = max(images, key=lambda i: int(i.get('width') or 0), default={}).get('src', '')
    duration = None
    runtime = re.fullmatch(r'(\d+):(\d{2}):(\d{2})', video.get('runLength') or '')
    if runtime:
        h, m, s = map(int, runtime.groups())
        duration = h * 3600 + m * 60 + s
    def names(key):
        return list(dict.fromkeys(x['name'].strip() for x in video.get(key) or []
                                 if isinstance(x, dict) and isinstance(x.get('name'), str) and x['name'].strip()))
    return {'title': video['title'], 'description': video.get('description') or '',
            'date': (video.get('releaseDate') or '')[:10], 'studio': SITES[site],
            'performers': names('models'), 'tags': names('categories'), 'image': image,
            'duration': duration, 'code': str(video.get('videoId') or ''),
            'url': f'https://www.{site}.com/videos/{slug}', 'provider': SITES[site] + ' direct'}


def scrape(url):
    identity = identify(url)
    if identity is None:
        raise ValueError('Use a Vixen network /videos/ URL')
    site, slug = identity
    # Only fixed official hosts; never forward local Stash credentials or
    # follow a redirect to an unrelated page and mistake it for scene data.
    try:
        response = requests.post(f'https://www.{site}.com/graphql',
            json={'query': QUERY, 'variables': {'slug': slug, 'site': site.upper()}},
            headers={'Referer': f'https://www.{site}.com/videos/{slug}', 'Accept': 'application/json'},
            impersonate='chrome', allow_redirects=False, timeout=25)
        if 300 <= response.status_code < 400:
            raise ValueError(f'{SITES[site]} redirected its metadata API to a restricted or regional landing page. No scene details were imported.')
        if response.status_code in (401, 403):
            raise ValueError(f'{SITES[site]} blocked the metadata request from this server.')
        response.raise_for_status()
        if len(response.content) > 4 * 1024 * 1024:
            raise ValueError('Scene metadata response is too large')
        payload = response.json()
    except requests.RequestsError as exc:
        raise ValueError(f'{SITES[site]} metadata request failed') from exc
    except ValueError:
        raise
    if payload.get('errors'):
        raise ValueError(f'{SITES[site]} could not return scene metadata')
    return normalize((payload.get('data') or {}).get('findOneVideo'), site, slug)
