"""Scoped Magpie download API bridge; credentials never sent to the browser."""
import re
import math
import json
import time
from threading import Lock
from threading import BoundedSemaphore
from urllib.parse import urlsplit
from typing import Literal

import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field
import database as db

class PrivateValidationRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()
        async def private_handler(request):
            try:
                return await handler(request)
            except RequestValidationError:
                # Validation errors normally echo input, including an API token.
                return JSONResponse({'detail': 'Invalid Magpie request fields'}, status_code=422)
        return private_handler


router = APIRouter(prefix='/api/magpie', route_class=PrivateValidationRoute)
_slots = BoundedSemaphore(3)
CATEGORIES = {'scenes':'Scenes', 'movies':'Movies', 'jav':'JAV', 'stars':'Stars', 'studios':'Studios', 'vices':'Vices', 'other':'Other'}
_category_lock = Lock()
_cache_lock = Lock()
_cache = (None, 0, [], None)

def job_categories():
    try:
        data = json.loads(db.get_settings().get('magpie_job_categories', '{}'))
        return data if isinstance(data, dict) else {}
    except (TypeError, ValueError):
        return {}


def job_category_key(ident):
    return str(db.get_settings().get('magpie_url', '')).rstrip('/') + ':' + str(ident)


def invalidate():
    global _cache
    _cache = (None, 0, [], None)

def mappings():
    try:
        value = json.loads(db.get_settings().get('magpie_categories', '{}'))
        return {k:v for k,v in value.items() if k in CATEGORIES and isinstance(v,str)}
    except (ValueError, AttributeError):
        return {}

STATUSES = {'queued', 'downloading', 'completed', 'failed', 'cancelled', 'interrupted'}


def validate_base(value):
    value = value.strip().rstrip('/')
    try:
        p = urlsplit(value)
        if (p.scheme not in {'http', 'https'} or not p.hostname or p.username or p.password
                or p.query or p.fragment or p.port == 0 or any(ord(c) <= 32 for c in value)):
            raise ValueError()
        _ = p.port
    except ValueError:
        raise HTTPException(400, 'Enter a valid HTTP(S) Magpie URL without credentials, query or fragment')
    return value


def connection():
    settings = db.get_settings()
    url, token = settings.get('magpie_url', ''), settings.get('magpie_token', '')
    if not url or not token:
        raise HTTPException(409, 'Configure the Magpie URL and API token first')
    return validate_base(url), token


def request(method, path, *, body=None, params=None, credentials=None):
    base, token = credentials or connection()
    if not _slots.acquire(blocking=False):
        raise HTTPException(503, 'Magpie is busy. Try again shortly.')
    try:
        # No redirect following: never forward a credential to another origin.
        with requests.Session() as session:
            session.trust_env = False
            response = session.request(method, base + '/api/' + path,
                headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'},
                json=body, params=params, timeout=(3, 12), allow_redirects=False)
            if not 200 <= response.status_code < 300:
                messages = {401: 'Magpie token is invalid, expired or revoked. Create a new API token in Magpie.',
                    403: 'Magpie refused this API token.', 404: 'Magpie endpoint or download not found. Check its URL and version.',
                    409: 'Magpie rejected this operation: the URL may already be queued or the job state has changed.',
                    400: 'Magpie rejected the URL or destination. Check that the destination is writable and the URL is supported.',
                    422: 'Magpie rejected the submitted URL or destination.', 429: 'Magpie queue or request limit reached.'}
                raise HTTPException(502 if response.status_code not in {400,409,422,429} else response.status_code,
                                    messages.get(response.status_code, 'Magpie is unavailable or returned an unexpected response.'))
            try:
                return response.json()
            except ValueError:
                raise HTTPException(502, 'Magpie returned an invalid response. Check the configured URL.')
    except requests.RequestException:
        raise HTTPException(502, 'Cannot reach Magpie. Check its URL, certificate and network connection.')
    finally:
        _slots.release()


class Config(BaseModel):
    url: str = Field(max_length=2048)
    token: str = Field(default='', max_length=256)


@router.get('/config')
def get_config():
    settings = db.get_settings()
    return {'url': settings.get('magpie_url', ''), 'token_set': bool(settings.get('magpie_token')), 'categories': mappings(), 'category_labels': CATEGORIES}


def config_credentials(body):
    url = validate_base(body.url)
    current = db.get_settings()
    token = body.token.strip()
    if token and (not token.startswith('magpie_') or any(c.isspace() for c in token)):
        raise HTTPException(400, 'Enter a Magpie API token, not the administrator password')
    if not token and url != current.get('magpie_url', ''):
        raise HTTPException(400, 'Enter the API token when changing the Magpie URL')
    if not token and not current.get('magpie_token'):
        raise HTTPException(400, 'Enter a Magpie API token')
    return url, token or current.get('magpie_token', '')


@router.post('/test')
def test_connection(body: Config):
    result = request('GET', 'destinations', credentials=config_credentials(body))
    if not isinstance(result, list):
        raise HTTPException(502, 'Invalid Magpie destinations response')
    return result


@router.post('/config')
def save_config(body: Config):
    url, token = config_credentials(body)
    current = db.get_settings()
    values = {'magpie_url': url}
    if token:
        values['magpie_token'] = token
    if url != current.get('magpie_url', ''):
        values['magpie_categories'] = '{}'
    db.save_settings(values)
    invalidate()
    return {'ok': True}


@router.get('/destinations')
def destinations():
    result = request('GET', 'destinations')
    if not isinstance(result, list):
        raise HTTPException(502, 'Invalid Magpie destinations response')
    return result


@router.get('/downloads')
def downloads(status: str = '', offset: int = Query(0, ge=0)):
    if status and status not in STATUSES:
        raise HTTPException(400, 'Invalid download status')
    result = request('GET', 'downloads', params={'status':status, 'limit':50, 'offset':offset})
    if not isinstance(result, list):
        raise HTTPException(502, 'Invalid Magpie downloads response')
    return result


class Download(BaseModel):
    url: str = Field(min_length=8, max_length=8192)
    destination_id: str = Field(default='', max_length=128)
    category: str = Field(default='', max_length=32)
    custom_fields: dict[str, str] = Field(default_factory=dict, max_length=20)
    filename_position: Literal['start', 'end'] = 'start'


@router.post('/downloads')
def enqueue(body: Download):
    try:
        p = urlsplit(body.url)
    except ValueError:
        raise HTTPException(400, 'Enter a valid HTTP(S) download URL')
    if p.scheme not in {'http', 'https'} or not p.hostname or p.username or p.password:
        raise HTTPException(400, 'Enter an HTTP(S) download URL without credentials')
    destination_id = body.destination_id
    if body.category:
        if body.category not in CATEGORIES:
            raise HTTPException(400, 'Unknown Magpie category')
        destination_id = mappings().get(body.category, '')
    if not destination_id:
        raise HTTPException(400, 'Map this category to a Magpie destination in Settings first')
    if any(not key.strip() or len(key)>64 or len(value)>256 for key,value in body.custom_fields.items()):
        raise HTTPException(400, 'Filename tags must have a name and contain at most 256 characters')
    payload = {'url':body.url, 'destination_id':destination_id}
    if body.custom_fields:
        payload.update(custom_fields=body.custom_fields, filename_position='start')
    result = request('POST', 'downloads', body=payload)
    if body.category and isinstance(result, dict):
        ident = result.get('id') or result.get('download_id')
        if ident:
            with _category_lock:
                saved = job_categories()
                saved[job_category_key(ident)] = body.category
                db.save_settings({'magpie_job_categories': json.dumps(saved)})
    invalidate()
    return result


@router.post('/downloads/{ident}/{action}')
def action(ident: str, action: str):
    if not re.fullmatch(r'[a-f0-9]{32}', ident) or action not in {'cancel','retry'}:
        raise HTTPException(400, 'Invalid Magpie download action')
    result = request('POST', f'downloads/{ident}/{action}')
    invalidate()
    return result


@router.delete('/downloads/{ident}')
def remove_history(ident: str):
    if not re.fullmatch(r'[a-f0-9]{32}', ident):
        raise HTTPException(400, 'Invalid Magpie download ID')
    job = request('GET', f'downloads/{ident}')
    if not isinstance(job, dict) or job.get('status') != 'completed':
        raise HTTPException(409, 'Only completed Magpie downloads can be removed from history')
    result = request('DELETE', f'downloads/{ident}')
    invalidate()
    return result


class CategoryMappings(BaseModel):
    categories: dict[str, str]


@router.post('/categories')
def save_categories(body: CategoryMappings):
    if set(body.categories) - set(CATEGORIES):
        raise HTTPException(400, 'Unknown Magpie category')
    known = {d['id'] for d in destinations() if d.get('writable')}
    if any(value and value not in known for value in body.categories.values()):
        raise HTTPException(400, 'Select a writable Magpie destination for each mapped category')
    db.save_settings({'magpie_categories': json.dumps(body.categories)})
    invalidate()
    return {'ok': True}


def job_snapshot():
    """Bounded polling shared by page, SSE and counters. Retain last results on failure."""
    global _cache
    settings = db.get_settings()
    key = (settings.get('magpie_url'), settings.get('magpie_token'))
    if not all(key):
        return [], None
    cached = _cache
    ttl = 5 if any(job.get('status') == 'downloading' for job in cached[2]) else 20
    if cached[0] == key and time.monotonic() - cached[1] < ttl:
        return cached[2], cached[3]
    if not _cache_lock.acquire(False):
        return (cached[2], cached[3]) if cached[0] == key else ([], 'Magpie is refreshing.')
    try:
        jobs = {}
        # Magpie caps its active queue at 1000. Fetch active jobs separately so
        # a long recent history can never hide an active download.
        for status in ('queued', 'downloading', ''):
            for offset in range(0, 1000 if status else 200, 500):
                rows = request('GET', 'downloads', params={'status':status, 'limit':500 if status else 200, 'offset':offset})
                if not isinstance(rows,list):
                    raise HTTPException(502, 'Invalid Magpie downloads response')
                for row in rows:
                    if isinstance(row,dict) and re.fullmatch(r'[a-f0-9]{32}',str(row.get('id',''))):
                        jobs[row['id']] = row
                if len(rows) < 500:
                    break
        # Magpie runs at most five concurrent transfers. Read their live details
        # through the same authenticated bridge, never exposing its token.
        warning = None
        for job in [row for row in jobs.values() if row.get('status') == 'downloading'][:5]:
            try:
                detail = request('GET', 'downloads/' + job['id'])
                if not isinstance(detail, dict) or detail.get('id') != job['id']:
                    raise HTTPException(502, 'Invalid Magpie job detail response')
                jobs[job['id']] = {**job, **detail}
            except HTTPException:
                warning = 'Magpie: live progress unavailable. Showing last listed progress.'
        result = list(jobs.values())
        _cache = (key, time.monotonic(), result, warning)
        return result, warning
    except HTTPException as exc:
        previous = cached[2] if cached[0] == key else []
        warning = 'Magpie: ' + str(exc.detail) + (' Showing last known jobs.' if previous else '')
        _cache = (key, time.monotonic(), previous, warning)
        return previous, warning
    finally:
        _cache_lock.release()


def job_progress(job):
    """Keep overall and current-file progress distinct; totals may be unknown."""
    def number(key):
        try:
            value = float(job.get(key))
            return value if math.isfinite(value) and value >= 0 else None
        except (ValueError, TypeError):
            return None
    if job.get('status') == 'completed':
        return 100, 'Album'
    handled = (number('files') or 0) + (number('skipped') or 0)
    total = number('total_files')
    if total and total >= handled:
        return 100 * handled / total, 'Album'
    downloaded, size = number('file_bytes'), number('file_total')
    if size and downloaded is not None:
        return min(100, 100 * downloaded / size), 'Current file'
    return None, 'Finding media' if not handled else 'Total unknown'


def explicit_progress(job, field, fallback):
    # Missing means an older API. Explicit null means the total is unknown.
    if field not in job:
        return fallback
    value = job[field]
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
        return max(0, min(100, value)) if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def merge_downloads(payload, category=None):
    result = dict(payload)
    items = list(payload.get('items') or [])
    jobs, error = job_snapshot()
    mapped = mappings()
    saved_categories = job_categories()
    only = category.removeprefix('@magpie:') if category and category.startswith('@magpie:') else None
    if only:
        items = []
    speed = 0
    for job in jobs:
        if only and (not mapped.get(only) or job.get('destination_id') != mapped[only]):
            continue
        if category not in (None,'','*') and not only:
            settings = db.get_settings()
            keys = {'scenes':'prowlarr_category','movies':'prowlarr_category_movies','jav':'prowlarr_category_jav','vices':'prowlarr_category_vices'}
            allowed = {mapped.get(k) for k,v in keys.items() if settings.get(v) == category and mapped.get(k)}
            if job.get('destination_id') not in allowed:
                continue
        status = job.get('status','')
        active = status in {'queued','downloading'}
        progress, progress_label = job_progress(job)
        # Do not infer either total from completion or from the other ring.
        file_progress, _ = job_progress({**job, 'status': '', 'total_files': None})
        if not job.get('file_total'):
            file_progress = None
        album_progress, album_label = job_progress({**job, 'status': '', 'file_total': None})
        if album_label != 'Album':
            album_progress = None
        file_progress = explicit_progress(job, 'file_progress_percent', file_progress)
        album_progress = explicit_progress(job, 'album_progress_percent', album_progress)
        if status == 'completed':
            progress, progress_label = 100, 'Album'
        elif album_progress is not None:
            progress, progress_label = album_progress, 'Album'
        elif file_progress is not None:
            progress, progress_label = file_progress, 'Current file'
        else:
            progress, progress_label = None, 'Total unknown'
        if progress_label == 'Album':
            try:
                total_files = float(job.get('total_files'))
                count = int(total_files) if math.isfinite(total_files) and total_files >= 0 and total_files.is_integer() else None
            except (TypeError, ValueError, OverflowError):
                count = None
            progress_label = f"Album · {count} {'file' if count == 1 else 'files'}" if count is not None else 'Album · file count unknown'
        try:
            raw_total = float(job.get('total_files'))
            file_total = int(raw_total) if math.isfinite(raw_total) and raw_total >= 1 and raw_total.is_integer() else None
        except (TypeError, ValueError, OverflowError):
            file_total = None
        completed_files = max(0, int(job.get('files') or 0))
        if file_total is not None:
            file_count_label = f"{file_total} {'file' if file_total == 1 else 'files'}"
        elif status == 'downloading' and job.get('current_filename'):
            file_count_label = f'{completed_files} complete · 1 downloading' if completed_files else '1 file downloading'
        elif status in {'queued', 'downloading'}:
            file_count_label = f'{completed_files} files complete' if completed_files else 'Finding files'
        else:
            file_count_label = f"{completed_files} {'file' if completed_files == 1 else 'files'}"
        selected = saved_categories.get(job_category_key(job['id']))
        labels = [CATEGORIES[k] for k,v in mapped.items() if v and v == job.get('destination_id')]
        # Legacy/external jobs cannot be assigned a category from a shared destination.
        category_label = CATEGORIES.get(selected) or (labels[0] if len(labels) == 1 else job.get('destination_name', ''))
        items.append({'id':job['id'], 'source':'magpie', 'client':'magpie',
            'source_url':job.get('url') or '',
            'name':job.get('current_filename') or job.get('url') or 'Magpie download',
            'display_name':job.get('current_filename') or job.get('url') or 'Magpie download',
            'category':category_label,
            'queue':'active' if active else 'history', 'status':status,
            'progress_pct':progress, 'progress_label':progress_label,
            'file_progress_pct':file_progress, 'album_progress_pct':album_progress,
            'size_mb':float(job.get('bytes') or 0)/1048576,
            'failed':status in {'failed','interrupted'}, 'failure_reason':job.get('error') or '',
            'magpie_action':'cancel' if active else ('retry' if status in {'failed','cancelled','interrupted'} else ''),
            'magpie_files':job.get('files',0), 'magpie_file_count_label':file_count_label, 'destination_name':job.get('destination_name',''),
            'completed':status == 'completed'})
        if status == 'downloading':
            speed += max(0,float(job.get('speed') or 0))
    magpie_items = [item for item in items if item.get('source') == 'magpie']
    db.enrich_download_items_with_filing(magpie_items)
    db.enrich_download_items_with_source_scene(magpie_items)
    result['items'] = items
    result['bandwidth_down_bps'] = (0 if only else payload.get('bandwidth_down_bps',0)) + speed
    result['errors'] = list(payload.get('errors') or []) + ([error] if error else [])
    result['categories'] = list(payload.get('categories') or []) + [
        {'label':'Magpie · '+CATEGORIES[k], 'value':'@magpie:'+k} for k,v in mapped.items() if v]
    result['configured'] = dict(payload.get('configured') or {}, magpie=bool(db.get_settings().get('magpie_url') and db.get_settings().get('magpie_token')))
    return result
