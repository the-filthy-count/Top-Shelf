"""
Top-Shelf - FastAPI backend

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8891 --reload
"""

import asyncio
import base64
import io
import json
import logging
import secrets
import math
import mimetypes
import os
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import AsyncGenerator
from urllib.parse import quote_plus, unquote

import bcrypt
import imagehash
import requests
import threading
import time
from apscheduler.schedulers.background import BackgroundScheduler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Body, FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image

import database as db

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "1.0.0"

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".wmv", ".mov", ".m4v", ".flv", ".webm"}

UPPERCASE_KEYWORDS = {
    'tv','vip','mylf','la','als','bts','vk','ftv','omg','yngr','nvg','bbg',
    'bgg','bff','ddf','xxx','pov','dilf','bbc','dp','bj','bwc','pmv','atm',
    'cei','dap','pawg','cim','milf','milfs','bbw','enf','cbt','dt','bdsm',
    'jav','joi','cof','ffm','mmf','owo','bbbj','povd','brcc','vr','4k','hd',
    'atk','aj',
}

SCREENSHOT_SIZE = 160
COLUMNS = 5
ROWS = 5

STASHDB_ENDPOINT = "https://stashdb.org/graphql"
TPDB_ENDPOINT    = "https://theporndb.net/graphql"
FANSDB_ENDPOINT  = "https://fansdb.cc/graphql"


def get_api_keys() -> dict:
    s = db.get_settings()
    return {
        "stashdb": s.get("api_key_stashdb", ""),
        "tpdb":    s.get("api_key_tpdb", ""),
        "fansdb":  s.get("api_key_fansdb", ""),
    }

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

processing_state = {"running": False, "current_file": None, "log": []}
log_queue: asyncio.Queue = None
scheduler = BackgroundScheduler(daemon=True)
observer          = None  # scene folder watchdog observer
download_observer = None  # scene + movie download folder watchdog observer (may schedule multiple paths)
_pending_files: dict     = {}  # filename -> scheduled time (scene watcher)
_pending_movie_files: dict = {}  # filename -> scheduled time (movies folder watcher)
_movie_poll_seen: set = set()
movie_observer = None  # movies download folder watchdog
_pending_downloads: dict = {}  # path -> scheduled time (scene download watch)
_pending_movie_downloads: dict = {}  # path -> scheduled time (movie download watch)
_pending_lock    = threading.Lock()
_download_lock   = threading.Lock()
_content_cache_lock = threading.Lock()

# Favourites library scan / refresh-all progress (in-memory, for UI polling)
_favourites_index_lock = threading.Lock()
_favourites_index_progress: dict = {
    "running": False,
    "phase": "",  # "scan_missing" | "scan_full" | "refresh_all"
    "total": 0,
    "done": 0,
    "current_name": "",
}


def _favourites_progress_start(phase: str, total: int) -> None:
    with _favourites_index_lock:
        _favourites_index_progress.update(
            {
                "running": True,
                "phase": phase,
                "total": max(0, int(total)),
                "done": 0,
                "current_name": "",
            }
        )


def _favourites_progress_update(current_name: str, done: int) -> None:
    with _favourites_index_lock:
        _favourites_index_progress["current_name"] = current_name or ""
        _favourites_index_progress["done"] = max(0, int(done))


def _favourites_progress_finish() -> None:
    with _favourites_index_lock:
        _favourites_index_progress["running"] = False
        _favourites_index_progress["phase"] = ""
        _favourites_index_progress["total"] = 0
        _favourites_index_progress["done"] = 0
        _favourites_index_progress["current_name"] = ""


class NewVideoHandler(FileSystemEventHandler):
    """Watches source folder and queues new video files after a hold period."""

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in VIDEO_EXTENSIONS:
            return
        s = db.get_settings()
        if s.get("folder_watch_enabled", "true").lower() != "true":
            return
        hold = int(s.get("folder_watch_hold_secs", "60"))
        with _pending_lock:
            if path.name not in _pending_files:
                emit(f"WATCHER detected: {path.name} (hold {hold}s)")
            _pending_files[path.name] = time.time() + hold

    def on_moved(self, event):
        # Handle files moved/renamed into the folder
        self.on_created(type("E", (), {"is_directory": False, "src_path": event.dest_path})())

    def on_modified(self, event):
        # Some systems fire modified instead of created for file moves
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in VIDEO_EXTENSIONS:
            return
        s = db.get_settings()
        if s.get("folder_watch_enabled", "true").lower() != "true":
            return
        hold = int(s.get("folder_watch_hold_secs", "60"))
        with _pending_lock:
            if path.name not in _pending_files:
                emit(f"WATCHER detected (modified): {path.name} (hold {hold}s)")
                _pending_files[path.name] = time.time() + hold


class NewMovieVideoHandler(FileSystemEventHandler):
    """Watch movies_source_dir; queue video files after hold (same settings as scene watch)."""

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in VIDEO_EXTENSIONS:
            return
        s = db.get_settings()
        if s.get("folder_watch_enabled", "true").lower() != "true":
            return
        hold = int(s.get("folder_watch_hold_secs", "60"))
        with _pending_lock:
            if path.name not in _pending_movie_files:
                emit(f"MOVIE WATCHER: {path.name} (hold {hold}s)")
            _pending_movie_files[path.name] = time.time() + hold

    def on_moved(self, event):
        self.on_created(type("E", (), {"is_directory": False, "src_path": event.dest_path})())

    def on_modified(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in VIDEO_EXTENSIONS:
            return
        s = db.get_settings()
        if s.get("folder_watch_enabled", "true").lower() != "true":
            return
        hold = int(s.get("folder_watch_hold_secs", "60"))
        with _pending_lock:
            if path.name not in _pending_movie_files:
                emit(f"MOVIE WATCHER (modified): {path.name} (hold {hold}s)")
                _pending_movie_files[path.name] = time.time() + hold


_poll_seen: set = set()  # files the poll has already queued (avoids re-queuing on every cycle)


def _check_pending_files():
    """Called by scheduler every 30s - fires pipeline for files past their hold time.
    Also polls the source directory for any NEW files that inotify missed."""
    if processing_state["running"]:
        return

    s = db.get_settings()
    if s.get("folder_watch_enabled", "true").lower() != "true":
        return

    source_dir = Path(s.get("source_dir", ""))
    hold = int(s.get("folder_watch_hold_secs", "60"))
    now = time.time()

    # Polling fallback: scan source dir for NEW video files only
    # Skip files we've already seen/queued, and files with any processing history
    if source_dir.exists():
        history = {r["filename"] for r in db.get_history(limit=10000)}
        with _pending_lock:
            for f in source_dir.iterdir():
                if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
                    if (f.name not in _pending_files
                            and f.name not in _poll_seen
                            and f.name not in history):
                        _pending_files[f.name] = now + hold
                        _poll_seen.add(f.name)
                        emit(f"WATCHER poll detected: {f.name} (hold {hold}s)")

    # Clean up _poll_seen for files that no longer exist
    if source_dir.exists():
        current_files = {f.name for f in source_dir.iterdir() if f.is_file()}
        _poll_seen.difference_update(_poll_seen - current_files)

    # Fire pipeline for files past hold time
    ready = []
    with _pending_lock:
        for fname, fire_at in list(_pending_files.items()):
            if now >= fire_at:
                ready.append(fname)
                del _pending_files[fname]
    if ready:
        to_run = [f for f in ready if (source_dir / f).exists()]
        if to_run:
            run_pipeline(to_run)


def _check_pending_movies():
    """Poll movies_source_dir and fire movie pipeline when hold elapsed (matches scene watcher)."""
    if processing_state["running"]:
        return
    s = db.get_settings()
    if s.get("folder_watch_enabled", "true").lower() != "true":
        return
    movie_dir = Path((s.get("movies_source_dir") or "").strip())
    if not movie_dir.exists():
        return
    hold = int(s.get("folder_watch_hold_secs", "60"))
    now = time.time()
    terminal = db.get_movie_terminal_filenames()
    if movie_dir.exists():
        with _pending_lock:
            for f in movie_dir.iterdir():
                if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
                    if (
                        f.name not in _pending_movie_files
                        and f.name not in _movie_poll_seen
                        and f.name not in terminal
                    ):
                        _pending_movie_files[f.name] = now + hold
                        _movie_poll_seen.add(f.name)
                        emit(f"MOVIE WATCHER poll: {f.name} (hold {hold}s)")
    if movie_dir.exists():
        current = {f.name for f in movie_dir.iterdir() if f.is_file()}
        _movie_poll_seen.difference_update(_movie_poll_seen - current)
    ready = []
    with _pending_lock:
        for fname, fire_at in list(_pending_movie_files.items()):
            if now >= fire_at:
                ready.append(fname)
                del _pending_movie_files[fname]
    if ready:
        to_run = [f for f in ready if (movie_dir / f).exists()]
        if to_run:
            run_movie_pipeline(to_run)


def _start_watcher():
    global observer
    s = db.get_settings()
    if s.get("folder_watch_enabled", "true").lower() != "true":
        return
    source_dir = Path(s.get("source_dir", ""))
    if not source_dir.exists():
        return
    if observer:
        observer.stop()
    observer = Observer()
    observer.schedule(NewVideoHandler(), str(source_dir), recursive=False)
    observer.start()


def _start_movie_watcher():
    global movie_observer
    s = db.get_settings()
    if s.get("folder_watch_enabled", "true").lower() != "true":
        if movie_observer:
            movie_observer.stop()
            movie_observer = None
        return
    md = Path((s.get("movies_source_dir") or "").strip())
    if not md.exists():
        if movie_observer:
            movie_observer.stop()
            movie_observer = None
        return
    if movie_observer:
        movie_observer.stop()
    movie_observer = Observer()
    movie_observer.schedule(NewMovieVideoHandler(), str(md), recursive=False)
    movie_observer.start()


def _restart_watcher():
    """Restart watcher with current settings - called after settings save."""
    global observer
    if observer:
        observer.stop()
        observer = None
    _start_watcher()
    _start_movie_watcher()

app = FastAPI(title="Top-Shelf")

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

COOKIE_NAME   = "ts_session"
LOGIN_PATH    = "/login"
PUBLIC_PATHS  = {"/login", "/api/auth/login", "/api/auth/logout", "/api/health"}


def _is_authenticated(request: Request) -> bool:
    if not db.get_password_hash():
        return True  # No password set - open access
    token = request.cookies.get(COOKIE_NAME, "")
    return db.validate_session(token)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    # Always allow public paths and static assets
    if path in PUBLIC_PATHS or path.startswith("/static"):
        return await call_next(request)
    if not _is_authenticated(request):
        if path.startswith("/api/"):
            from fastapi.responses import JSONResponse
            return JSONResponse({"error": "Unauthorised"}, status_code=401)
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"{LOGIN_PATH}?next={path}", status_code=302)
    return await call_next(request)


app.mount("/static", StaticFiles(directory="static"), name="static")


def _apply_retry_schedule():
    """Read settings and (re)schedule the retry job."""
    try:
        scheduler.remove_job("retry")
    except Exception:
        pass
    s = db.get_settings()
    if s.get("retry_enabled", "true").lower() != "true":
        return
    hour = int(s.get("retry_hour", "1"))
    freq = int(s.get("retry_frequency_h", "24"))
    if freq == 24:
        trigger = CronTrigger(hour=hour, minute=0)
    else:
        trigger = CronTrigger(hour=f"*/{freq}", minute=0)
    scheduler.add_job(run_retry_pipeline, trigger, id="retry", replace_existing=True)


def run_retry_pipeline():
    """Re-run the full pipeline on all non-filed files that still exist in the source dir."""
    if processing_state["running"]:
        return
    s = db.get_settings()
    source_dir = Path(s.get("source_dir", ""))
    candidates = db.get_retry_files()
    # Only retry files that still exist on disk
    to_retry = [f for f in candidates if (source_dir / f).exists()]
    if not to_retry:
        return
    run_pipeline(to_retry)


def _apply_tpdb_sync_schedule():
    """Read settings and (re)schedule the TPDB favourites sync job."""
    s = db.get_settings()
    if s.get("tpdb_sync_enabled", "false").lower() != "true":
        try:
            scheduler.remove_job("tpdb_sync")
        except Exception:
            pass
        return
    hour = int(s.get("tpdb_sync_hour", "2"))
    freq = int(s.get("tpdb_sync_frequency_h", "24"))
    if freq == 24:
        trigger = CronTrigger(hour=hour, minute=0)
    else:
        trigger = CronTrigger(hour=f"*/{freq}", minute=0)
    scheduler.add_job(sync_tpdb_favourites, trigger, id="tpdb_sync", replace_existing=True)


def _apply_favourites_schedule():
    """Nightly scan of library performer/studio folders into Favourites index."""
    try:
        scheduler.remove_job("favourites_scan")
    except Exception:
        pass
    s = db.get_settings()
    if s.get("favourites_scan_enabled", "false").lower() != "true":
        return
    hour = int(s.get("favourites_scan_hour", "3")) % 24
    trigger = CronTrigger(hour=hour, minute=12)
    scheduler.add_job(
        run_favourites_scheduled_scan,
        trigger,
        id="favourites_scan",
        replace_existing=True,
    )


@app.on_event("startup")
async def startup():
    global log_queue
    log_queue = asyncio.Queue()
    db.init_db()
    _migrate_sidecar_phashes()
    _apply_retry_schedule()
    _apply_tpdb_sync_schedule()
    _apply_favourites_schedule()
    scheduler.add_job(_check_pending_files, "interval", seconds=30, id="pending_check")
    scheduler.add_job(_check_pending_movies, "interval", seconds=30, id="movie_pending_check")
    scheduler.add_job(db.purge_expired_sessions, "interval", hours=1, id="session_purge")
    scheduler.add_job(_check_pending_downloads, "interval", seconds=30, id="download_check")
    scheduler.add_job(_auto_import_completed_poll, "interval", seconds=45, id="download_auto_import", replace_existing=True)
    scheduler.add_job(_refresh_hourly_content_cache, "interval", hours=1, id="content_cache_refresh", replace_existing=True)
    scheduler.start()
    _start_watcher()
    _start_movie_watcher()
    _start_download_watcher()
    # Warm feed cache in background so HTTP comes up immediately (Docker / first load)
    asyncio.get_running_loop().run_in_executor(None, _refresh_hourly_content_cache)


@app.on_event("shutdown")
async def shutdown():
    global observer, download_observer, movie_observer
    if observer:
        observer.stop()
    if movie_observer:
        movie_observer.stop()
    if download_observer:
        download_observer.stop()
    scheduler.shutdown(wait=False)


def _migrate_sidecar_phashes():
    """One-time migration: read any existing .phash sidecar files into the DB and delete them."""
    settings = db.get_settings()
    source_dir = Path(settings.get("source_dir", ""))
    if not source_dir.exists():
        return
    for phash_file in source_dir.glob("*.phash"):
        filename = phash_file.stem + phash_file.with_suffix("").suffix
        # stem alone won't work for .mkv.phash - reconstruct the video filename
        video_name = phash_file.name[:-6]  # strip .phash
        try:
            phash = phash_file.read_text().strip()
            if phash:
                db.upsert_file(video_name)
                db.update_file(video_name, status="pending", phash=phash)
            phash_file.unlink()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

STASHDB_QUERY = """
query FindScenesByFullFingerprints($fingerprints: [FingerprintQueryInput!]!) {
  findScenesByFullFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
  }
}
"""

TPDB_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [[FingerprintQueryInput]]!) {
  findScenesBySceneFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
  }
}
"""


SEARCH_SCENES_QUERY = """
query SearchScenes($term: String!, $limit: Int) {
  searchScene(term: $term, limit: $limit) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
  }
}
"""

QUERY_SCENES_QUERY = """
query QueryScenes($input: SceneQueryInput!) {
  queryScenes(input: $input) {
    count
    scenes {
      id title release_date
      studio { id name }
      performers { performer { id name gender } }
      images { url width height }
    }
  }
}
"""

SEARCH_PERFORMER_QUERY = """
query SearchPerformer($term: String!, $limit: Int) {
  searchPerformer(term: $term, limit: $limit) {
    id
    name
  }
}
"""

SEARCH_STUDIO_QUERY = """
query SearchStudio($term: String!, $limit: Int) {
  searchStudio(term: $term, limit: $limit) {
    id
    name
    images { url width height }
  }
}
"""


def _stashbox_post(endpoint: str, api_key: str, query: str, variables: dict) -> dict:
    resp = requests.post(
        endpoint,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json", "ApiKey": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(str(data["errors"]))
    return data.get("data", {})


def _build_search_term(title: str = None, performer: str = None,
                       studio: str = None) -> str:
    """Combine structured fields into a single search term."""
    parts = []
    if title:
        parts.append(title)
    if performer:
        parts.append(performer)
    if studio:
        parts.append(studio)
    return " ".join(parts)


def search_scenes_on_db(endpoint: str, api_key: str, term: str, limit: int = 25) -> list:
    data = _stashbox_post(endpoint, api_key, SEARCH_SCENES_QUERY, {"term": term, "limit": limit})
    return data.get("searchScene") or []


def query_scenes_by_date(endpoint: str, api_key: str, term: str,
                         date_from: str = None, date_to: str = None,
                         per_page: int = 25) -> list:
    """Use queryScenes with date range filter plus free-text title match."""
    inp = {"per_page": per_page, "page": 1, "direction": "DESC", "sort": "DATE"}
    if term:
        inp["title"] = term
    if date_from or date_to:
        date_range = {}
        if date_from:
            date_range["from"] = date_from
        if date_to:
            date_range["to"] = date_to
        inp["date"] = date_range
    data = _stashbox_post(endpoint, api_key, QUERY_SCENES_QUERY, {"input": inp})
    result = data.get("queryScenes") or {}
    return result.get("scenes") or []


def search_all_databases(term: str = None, title: str = None, performer: str = None,
                         studio: str = None, date_from: str = None,
                         date_to: str = None) -> list:
    """
    Search all three databases, merge and deduplicate results.
    Strategy: build a combined search term from all text fields and use
    searchScene (full-text). If date range given, also run queryScenes
    with date filter and merge results.
    """
    keys = get_api_keys()
    sources = [
        ("StashDB",   STASHDB_ENDPOINT, keys["stashdb"]),
        ("TPDB",     TPDB_ENDPOINT,    keys["tpdb"]),
        ("FansDB",    FANSDB_ENDPOINT,  keys["fansdb"]),
    ]

    # Build a combined search term from all text inputs
    combined = _build_search_term(
        title=title or term,
        performer=performer,
        studio=studio,
    )

    all_results = []
    seen_ids = set()

    def _search_source(source_name, endpoint, api_key):
        """Search a single source, returns list of (scene, source_name) tuples."""
        results = []
        try:
            if combined:
                for s in (search_scenes_on_db(endpoint, api_key, combined) or []):
                    results.append((s, source_name))
            if date_from or date_to:
                for s in (query_scenes_by_date(endpoint, api_key, combined,
                                               date_from=date_from, date_to=date_to) or []):
                    results.append((s, source_name))
        except Exception:
            pass
        return results

    active_sources = [(n, e, k) for n, e, k in sources if k]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=len(active_sources) or 1) as pool:
        futures = {pool.submit(_search_source, n, e, k): n for n, e, k in active_sources}
        for future in as_completed(futures):
            for scene, source_name in future.result():
                sid = scene.get("id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    scene["_source"] = source_name
                    all_results.append(scene)

    return all_results

# ---------------------------------------------------------------------------
# Phash
# ---------------------------------------------------------------------------

def get_video_duration(video_path: Path) -> float:
    cmd = ["ffprobe", "-hide_banner", "-loglevel", "error",
           "-of", "compact=p=0:nk=1", "-show_entries", "packet=pts_time"]
    try:
        res = subprocess.run([*cmd, "-read_intervals", "9999999%+#1000", str(video_path)],
                             check=True, capture_output=True, text=True)
        return float(res.stdout.strip().split("\n")[-1])
    except (subprocess.CalledProcessError, ValueError):
        res = subprocess.run([*cmd, str(video_path)], check=True, capture_output=True, text=True)
        return float(res.stdout.strip().split("\n")[-1])


def get_sprite_screenshot(video_path: Path, t: float) -> Image.Image:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error",
           "-ss", str(t), "-i", str(video_path),
           "-frames:v", "1", "-vf", f"scale={SCREENSHOT_SIZE}:{-2}",
           "-c:v", "bmp", "-f", "image2", "-"]
    res = subprocess.run(cmd, check=True, capture_output=True)
    return Image.open(BytesIO(res.stdout))


def build_sprite(video_path: Path) -> Image.Image:
    duration = get_video_duration(video_path)
    offset = 0.05 * duration
    step = (0.9 * duration) / (COLUMNS * ROWS)
    images = [get_sprite_screenshot(video_path, offset + i * step) for i in range(COLUMNS * ROWS)]
    w, h = images[0].size
    montage = Image.new("RGB", (w * COLUMNS, h * ROWS))
    for i, img in enumerate(images):
        montage.paste(img, (w * (i % COLUMNS), h * math.floor(i / ROWS)))
    return montage


def compute_phash(video_path: Path) -> str:
    return str(imagehash.phash(build_sprite(video_path)))

# ---------------------------------------------------------------------------
# Stash-box queries
# ---------------------------------------------------------------------------

def query_stashbox(phash_hex, endpoint, api_key, query, fingerprint_var):
    if fingerprint_var == "full":
        variables = {"fingerprints": [{"hash": phash_hex, "algorithm": "PHASH"}]}
    else:
        variables = {"fingerprints": [[{"hash": phash_hex, "algorithm": "PHASH"}]]}
    resp = requests.post(endpoint,
                         json={"query": query, "variables": variables},
                         headers={"Content-Type": "application/json", "ApiKey": api_key},
                         timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Stash-box error: {data['errors']}")
    result = data.get("data", {})
    if "findScenesByFullFingerprints" in result:
        return result["findScenesByFullFingerprints"] or []
    if "findScenesBySceneFingerprints" in result:
        nested = result["findScenesBySceneFingerprints"] or []
        return [s for group in nested for s in (group or [])]
    return []


def query_with_fallback(phash_hex):
    keys = get_api_keys()
    try:
        m = query_stashbox(phash_hex, STASHDB_ENDPOINT, keys["stashdb"], STASHDB_QUERY, "full")
        if m: return m, "StashDB"
    except Exception as e:
        emit(f"  WARNING: StashDB failed ({e}), trying TPDB...")
    try:
        m = query_stashbox(phash_hex, TPDB_ENDPOINT, keys["tpdb"], TPDB_QUERY, "scene")
        if m: return m, "TPDB"
    except Exception as e:
        emit(f"  WARNING: TPDB failed ({e}), trying FansDB...")
    try:
        m = query_stashbox(phash_hex, FANSDB_ENDPOINT, keys["fansdb"], STASHDB_QUERY, "full")
        if m: return m, "FansDB"
    except Exception as e:
        raise RuntimeError(f"FansDB also failed: {e}")
    return [], "none"

# ---------------------------------------------------------------------------
# Download folder processor
# ---------------------------------------------------------------------------

JUNK_EXTENSIONS = {
    ".nfo", ".nzb", ".srr", ".sfv", ".jpg", ".jpeg", ".png", ".gif",
    ".txt", ".url", ".exe", ".bat", ".sub", ".srt", ".ass", ".ssa",
    ".idx", ".md5", ".xml", ".html", ".htm", ".lnk", ".torrent",
}


def _looks_like_gibberish(stem: str) -> bool:
    """
    True if the filename stem has no separators at all.
    Readable names use dots, spaces, dashes or underscores as word breaks.
    Pure gibberish is a run of characters with no breaks whatsoever.
    """
    return not any(c in stem for c in ".-_ ")


def _process_download_entry(entry_path: Path, dest_dir: Path) -> None:
    """
    Process a single file or folder from the download watch directory.
    - Deletes junk and sample files
    - Renames video to folder name if filename looks like gibberish
    - Moves video(s) to dest_dir
    - Deletes the source folder
    """
    emit(f"DOWNLOAD {entry_path.name}")

    if entry_path.is_file():
        # Single file dropped directly
        if entry_path.suffix.lower() not in VIDEO_EXTENSIONS:
            emit(f"  Skipped: not a video ({entry_path.suffix})")
            return
        if "sample" in entry_path.name.lower():
            emit(f"  Skipped: sample file")
            entry_path.unlink()
            return
        dest = dest_dir / entry_path.name
        safe_move(entry_path, dest)
        emit(f"  Moved: {entry_path.name}")
        return

    if not entry_path.is_dir():
        return

    folder_name = entry_path.name

    # Gather all files recursively
    all_files = list(entry_path.rglob("*"))

    # Find non-sample videos
    videos = [
        f for f in all_files
        if f.is_file()
        and f.suffix.lower() in VIDEO_EXTENSIONS
        and "sample" not in f.name.lower()
    ]

    # Find and delete samples
    samples = [
        f for f in all_files
        if f.is_file()
        and f.suffix.lower() in VIDEO_EXTENSIONS
        and "sample" in f.name.lower()
    ]
    for s in samples:
        s.unlink()
        emit(f"  Deleted sample: {s.name}")

    if not videos:
        emit(f"  No videos found - deleting folder")
        shutil.rmtree(entry_path, ignore_errors=True)
        return

    for video in videos:
        stem = video.stem
        ext  = video.suffix

        # Use folder name if only one video and filename looks like gibberish
        if len(videos) == 1 and _looks_like_gibberish(stem):
            new_name = f"{folder_name}{ext}"
            emit(f"  Renamed: {video.name} → {new_name}")
        else:
            new_name = video.name

        dest = dest_dir / new_name
        # Avoid collision
        if dest.exists():
            dest = dest_dir / f"{Path(new_name).stem}_1{ext}"

        safe_move(video, dest)
        emit(f"  Moved: {new_name} → {dest_dir.name}/")

    # Delete the source folder and everything remaining
    shutil.rmtree(entry_path, ignore_errors=True)
    emit(f"  Folder deleted: {folder_name}")
    emit("---")


def _check_pending_downloads() -> None:
    """Called by scheduler every 30s - fires download processor for entries past hold time."""
    now = time.time()
    ready = []
    ready_m = []
    with _download_lock:
        for path_str, fire_at in list(_pending_downloads.items()):
            if now >= fire_at:
                ready.append(path_str)
                del _pending_downloads[path_str]
        for path_str, fire_at in list(_pending_movie_downloads.items()):
            if now >= fire_at:
                ready_m.append(path_str)
                del _pending_movie_downloads[path_str]
    if ready:
        s = db.get_settings()
        for path_str in ready:
            entry = Path(path_str)
            if not entry.exists():
                continue
            try:
                ep = entry.resolve()
                dest_dir, derr = _dl_resolve_import_dest_dir(
                    s, "", str(ep.parent), str(ep),
                )
                if derr or not dest_dir:
                    emit(f"  Download process skipped ({entry.name}): {derr}")
                    continue
                _process_download_entry(entry, dest_dir)
            except Exception as e:
                emit(f"  Download process error: {e}")
    if ready_m:
        s = db.get_settings()
        fd = (s.get("features_dir") or "").strip()
        if not fd:
            for path_str in ready_m:
                emit(f"  Movie download process skipped ({Path(path_str).name}): Movies Library Directory not set in Settings")
        else:
            dest_root = Path(fd).expanduser()
            if not dest_root.is_dir():
                for path_str in ready_m:
                    emit(f"  Movie download process skipped ({Path(path_str).name}): Movies Library Directory not found: {fd}")
            else:
                for path_str in ready_m:
                    entry = Path(path_str)
                    if not entry.exists():
                        continue
                    try:
                        _process_download_entry(entry, dest_root)
                    except Exception as e:
                        emit(f"  Movie download process error: {e}")


class DownloadWatchHandler(FileSystemEventHandler):
    """Watches the download folder and queues new entries after a hold period."""

    def _queue(self, path_str: str) -> None:
        s = db.get_settings()
        if s.get("download_watch_enabled", "false").lower() != "true":
            return
        dl_raw = (s.get("download_watch_dir") or "").strip()
        if not dl_raw:
            return
        hold = int(s.get("download_watch_hold_secs", "300"))
        with _download_lock:
            # Only queue the top-level entry (folder or file)
            entry = Path(path_str)
            dl_dir = Path(dl_raw)
            if entry.parent == dl_dir or entry == dl_dir:
                top = path_str
            else:
                # Find the immediate child of dl_dir
                try:
                    rel = entry.relative_to(dl_dir)
                    top = str(dl_dir / rel.parts[0])
                except ValueError:
                    top = path_str
            _pending_downloads[top] = time.time() + hold

    def on_created(self, event):
        self._queue(event.src_path)

    def on_moved(self, event):
        self._queue(event.dest_path)


class MovieDownloadWatchHandler(FileSystemEventHandler):
    """Watches the movie download folder; queues top-level entries after hold (same hold as scene watch)."""

    def _queue(self, path_str: str) -> None:
        s = db.get_settings()
        if s.get("download_watch_enabled", "false").lower() != "true":
            return
        mdl_raw = (s.get("movie_download_watch_dir") or "").strip()
        if not mdl_raw:
            return
        hold = int(s.get("download_watch_hold_secs", "300"))
        with _download_lock:
            entry = Path(path_str)
            dl_dir = Path(mdl_raw)
            if entry.parent == dl_dir or entry == dl_dir:
                top = path_str
            else:
                try:
                    rel = entry.relative_to(dl_dir)
                    top = str(dl_dir / rel.parts[0])
                except ValueError:
                    top = path_str
            _pending_movie_downloads[top] = time.time() + hold

    def on_created(self, event):
        self._queue(event.src_path)

    def on_moved(self, event):
        self._queue(event.dest_path)


def _start_download_watcher() -> None:
    global download_observer
    s = db.get_settings()
    if s.get("download_watch_enabled", "false").lower() != "true":
        if download_observer:
            download_observer.stop()
            download_observer = None
        return
    if download_observer:
        download_observer.stop()
        download_observer = None
    dl_raw = (s.get("download_watch_dir") or "").strip()
    mdl_raw = (s.get("movie_download_watch_dir") or "").strip()
    obs = Observer()
    scheduled = False
    if dl_raw:
        dl_dir = Path(dl_raw)
        if dl_dir.exists():
            obs.schedule(DownloadWatchHandler(), str(dl_dir), recursive=True)
            scheduled = True
    if mdl_raw:
        mdl_dir = Path(mdl_raw)
        if mdl_dir.exists():
            obs.schedule(MovieDownloadWatchHandler(), str(mdl_dir), recursive=True)
            scheduled = True
    if not scheduled:
        return
    download_observer = obs
    download_observer.start()


def _restart_download_watcher() -> None:
    global download_observer
    if download_observer:
        download_observer.stop()
        download_observer = None
    _start_download_watcher()


# ---------------------------------------------------------------------------
# Directory matching (settings-driven)
# ---------------------------------------------------------------------------

def normalise(text: str) -> str:
    """Lowercase compare key for folder names vs API names (hyphens ↔ spaces)."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("-", " ").replace("_", " ")
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Media server integration
# ---------------------------------------------------------------------------

# Debounce tracker - {server: scheduled_time}
_scan_pending: dict = {}
_scan_lock = threading.Lock()


def _trigger_scan_after_debounce(server: str, delay_secs: int, fn):
    """Schedule a scan, cancelling any pending one for the same server."""
    with _scan_lock:
        _scan_pending[server] = time.time() + delay_secs

    def _wait_and_fire():
        time.sleep(delay_secs)
        with _scan_lock:
            fire_at = _scan_pending.get(server, 0)
        if time.time() >= fire_at:
            try:
                fn()
            except Exception as e:
                print(f"[{server}] scan trigger failed: {e}")

    threading.Thread(target=_wait_and_fire, daemon=True).start()


def trigger_media_scans(destination: str = None):
    """Fire scan triggers for all configured media servers with debounce."""
    s = db.get_settings()
    if s.get("media_scan_enabled", "true").lower() != "true":
        return
    debounce = int(s.get("media_scan_debounce_mins", "5")) * 60

    # Stash
    stash_url = s.get("stash_url", "").rstrip("/")
    stash_key = s.get("stash_api_key", "")
    if stash_url and stash_key and s.get("stash_enabled", "true") == "true":
        def _stash_scan():
            emit("  Scan: triggering Stash library scan...")
            query = 'mutation { metadataScan(input: {scanGeneratePhashes: false}) }'
            resp = requests.post(
                f"{stash_url}/graphql",
                json={"query": query},
                headers={"ApiKey": stash_key, "Content-Type": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            emit("  Scan: Stash scan triggered")
        _trigger_scan_after_debounce("stash", debounce, _stash_scan)

    # Jellyfin
    jellyfin_url = s.get("jellyfin_url", "").rstrip("/")
    jellyfin_key = s.get("jellyfin_api_key", "")
    if jellyfin_url and jellyfin_key and s.get("jellyfin_enabled", "true") == "true":
        def _jellyfin_scan():
            emit("  Scan: triggering Jellyfin library scan...")
            resp = requests.post(
                f"{jellyfin_url}/Library/Refresh",
                headers={"X-Emby-Token": jellyfin_key},
                timeout=15,
            )
            resp.raise_for_status()
            emit("  Scan: Jellyfin scan triggered")
        _trigger_scan_after_debounce("jellyfin", debounce, _jellyfin_scan)

    # Plex
    plex_url = s.get("plex_url", "").rstrip("/")
    plex_token = s.get("plex_token", "")
    if plex_url and plex_token and s.get("plex_enabled", "true") == "true":
        def _plex_scan():
            emit("  Scan: triggering Plex library scan...")
            resp = requests.get(
                f"{plex_url}/library/sections/all/refresh",
                params={"X-Plex-Token": plex_token},
                timeout=15,
            )
            resp.raise_for_status()
            emit("  Scan: Plex scan triggered")
        _trigger_scan_after_debounce("plex", debounce, _plex_scan)

    # Emby
    emby_url = s.get("emby_url", "").rstrip("/")
    emby_key = s.get("emby_api_key", "")
    if emby_url and emby_key and s.get("emby_enabled", "true") == "true":
        def _emby_scan():
            emit("  Scan: triggering Emby library scan...")
            resp = requests.post(
                f"{emby_url}/Library/Refresh",
                headers={"X-Emby-Token": emby_key},
                timeout=15,
            )
            resp.raise_for_status()
            emit("  Scan: Emby scan triggered")
        _trigger_scan_after_debounce("emby", debounce, _emby_scan)


def push_to_stash(scene: dict, destination: str, source: str, phash: str = None) -> bool:
    """
    Create or update a scene in local Stash with metadata from our match.
    Uses sceneCreate mutation - Stash will link it when it scans the file.
    """
    s = db.get_settings()
    if s.get("stash_enabled", "true") != "true":
        return False
    stash_url = s.get("stash_url", "").rstrip("/")
    stash_key = s.get("stash_api_key", "")
    if not stash_url or not stash_key:
        return False

    try:
        title       = scene.get("title") or ""
        date        = scene.get("release_date") or scene.get("date") or ""
        studio_name = (scene.get("studio") or {}).get("name") or ""
        performers  = scene.get("performers") or []
        perf_names  = [p["performer"]["name"] for p in performers if p.get("performer")]
        details     = scene.get("details") or scene.get("_plot") or ""

        headers = {"ApiKey": stash_key, "Content-Type": "application/json"}

        # Step 1: find the file ID for our destination path
        file_id = None
        try:
            find_query = """
            query { findFiles(filter: {q: "%s", per_page: 5}) {
                files { ... on VideoFile { id path } }
            } }
            """ % Path(destination).name.replace('"', '')
            fr = requests.post(f"{stash_url}/graphql",
                json={"query": find_query},
                headers=headers, timeout=10)
            fr.raise_for_status()
            files = (fr.json().get("data") or {}).get("findFiles", {}).get("files", [])
            for f in files:
                if f.get("path") == destination:
                    file_id = f["id"]
                    break
        except Exception:
            pass

        # Step 2: create the scene
        mutation = """
        mutation SceneCreate($input: SceneCreateInput!) {
            sceneCreate(input: $input) { id title }
        }
        """
        input_data = {
            "title":     title,
            "organized": True,
        }
        if date:
            input_data["date"] = date
        if details:
            input_data["details"] = details
        if file_id:
            input_data["file_ids"] = [file_id]
        # Link back to StashDB scene if match came from there
        stash_scene_id = scene.get("id")
        if stash_scene_id:
            input_data["stash_ids"] = [{"stash_id": stash_scene_id, "endpoint": "https://stashdb.org"}]
        variables = {"input": input_data}

        resp = requests.post(
            f"{stash_url}/graphql",
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            msgs = [e.get("message", str(e)) for e in data["errors"]]
            if any("not authorized" in m.lower() for m in msgs):
                emit("  Stash: push failed (not authorised — check your Stash API key)")
            else:
                emit(f"  Stash: push failed ({'; '.join(msgs)})")
            return False
        new_id = (data.get("data") or {}).get("sceneCreate", {}).get("id")
        emit(f"  Stash: scene created (id {new_id}){' with file link' if file_id else ' - run scan to link file'}")
        return True
    except Exception as e:
        emit(f"  Stash: push error ({e})")
        return False


# ---------------------------------------------------------------------------
# StashDB manual scene submission
# ---------------------------------------------------------------------------

STASHDB_STUDIO_SEARCH = """
query SearchStudios($term: String!) {
  searchStudio(term: $term) {
    id
    name
    images { url width height }
  }
}
"""

STASHDB_PERFORMER_SEARCH = """
query SearchPerformers($term: String!) {
  searchPerformer(term: $term) { id name }
}
"""

STASHDB_SCENE_CREATE = """
mutation SceneCreate($input: SceneCreateInput!) {
  sceneCreate(input: $input) { id title }
}
"""

STASHDB_IMAGE_CREATE = """
mutation ImageCreate($input: ImageCreateInput!) {
  imageCreate(input: $input) { id url }
}
"""

def stashdb_upload_image(image_url: str, api_key: str) -> str | None:
    """Upload an image to StashDB by URL and return the image ID.
    Note: requires elevated StashDB permissions - silently skipped if not authorised."""
    if not image_url:
        return None
    try:
        data = _stashbox_post(STASHDB_ENDPOINT, api_key, STASHDB_IMAGE_CREATE,
                              {"input": {"url": image_url}})
        errors = data.get("errors") or []
        if any("not authorized" in str(e) for e in errors):
            return None  # silently skip - standard API keys can't upload images
        return (data.get("imageCreate") or {}).get("id")
    except Exception:
        return None


def stashdb_search_studio(name: str, api_key: str) -> list[dict]:
    """Search StashDB for a studio by name. Returns list of {id, name, images?}."""
    try:
        data = _stashbox_post(STASHDB_ENDPOINT, api_key, STASHDB_STUDIO_SEARCH, {"term": name})
        return data.get("searchStudio") or []
    except Exception:
        return []


def stashdb_search_performer(name: str, api_key: str) -> list[dict]:
    """Search StashDB for a performer by name. Returns list of {id, name}."""
    try:
        data = _stashbox_post(STASHDB_ENDPOINT, api_key, STASHDB_PERFORMER_SEARCH, {"term": name})
        return data.get("searchPerformer") or []
    except Exception:
        return []


def stashdb_submit_scene(title: str, date: str, studio_id: str,
                          performer_ids: list, phash: str,
                          details: str, image_url: str, api_key: str,
                          duration: int = 0) -> dict:
    """Submit a new scene to StashDB."""
    fingerprints = []
    if phash:
        fingerprints.append({"algorithm": "PHASH", "hash": phash, "duration": duration})

    # Upload image first to get image_id if URL provided
    image_ids = []
    if image_url:
        img_id = stashdb_upload_image(image_url, api_key)
        if img_id:
            image_ids = [img_id]

    inp = {
        "title":        title,
        "date":         date,
        "details":      details or "",
        "fingerprints": fingerprints,  # NON_NULL - always required
    }
    if studio_id:
        inp["studio_id"] = studio_id
    if performer_ids:
        # StashDB expects [{performer_id, as}] not a flat list of IDs
        inp["performers"] = [{"performer_id": pid} for pid in performer_ids]
    if image_ids:
        inp["image_ids"] = image_ids

    try:
        data = _stashbox_post(STASHDB_ENDPOINT, api_key, STASHDB_SCENE_CREATE, {"input": inp})
        errors = data.get("errors") or []
        if errors:
            msgs = [e.get("message", str(e)) for e in errors]
            if any("not authorized" in m.lower() for m in msgs):
                return {"success": False, "error": "Not authorised — your StashDB account needs EDIT permissions to submit scenes"}
            return {"success": False, "error": "; ".join(msgs)}
        scene = data.get("sceneCreate") or {}
        return {"success": True, "id": scene.get("id"), "title": scene.get("title")}
    except Exception as e:
        return {"success": False, "error": str(e)}


def capture_frame_as_base64(video_path: Path, percent: float) -> str | None:
    """Capture a single frame from a video at percent (0-100) of duration."""
    try:
        duration = get_video_duration(video_path)
        if not duration:
            return None
        t = duration * (percent / 100.0)
        cmd = [
            "ffmpeg", "-ss", str(t), "-i", str(video_path),
            "-frames:v", "1", "-f", "image2", "-vcodec", "mjpeg", "pipe:1",
            "-loglevel", "error"
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0 or not result.stdout:
            return None
        import base64
        return "data:image/jpeg;base64," + base64.b64encode(result.stdout).decode()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# StashDB phash submission
# ---------------------------------------------------------------------------

SUBMIT_FINGERPRINT_MUTATION = """
mutation SubmitFingerprint($input: FingerprintSubmission!) {
  submitFingerprint(input: $input)
}
"""

def submit_phash_to_stashdb(scene_id: str, phash: str, duration: float = None) -> bool:
    """Submit a phash fingerprint to StashDB for a given scene ID."""
    s = db.get_settings()
    if s.get("submit_phash_enabled", "true").lower() != "true":
        return False
    api_key = s.get("api_key_stashdb", "")
    if not api_key or not scene_id or not phash:
        return False
    try:
        fingerprint = {
            "hash":      phash,
            "algorithm": "PHASH",
        }
        # Duration is required by StashDB - get it from the filed file if possible
        if duration and duration > 0:
            fingerprint["duration"] = int(duration)
        else:
            # Try to get duration from DB path if available
            fingerprint["duration"] = 0
        payload = {
            "query": SUBMIT_FINGERPRINT_MUTATION,
            "variables": {
                "input": {
                    "scene_id":    scene_id,
                    "fingerprint": fingerprint,
                }
            }
        }
        resp = requests.post(
            STASHDB_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json", "ApiKey": api_key},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            msgs = [e.get("message", str(e)) for e in data["errors"]]
            if any("not authorized" in m.lower() for m in msgs):
                emit("  Phash submit: not authorised (READ-only StashDB account)")
            else:
                emit(f"  Phash submit: failed ({'; '.join(msgs)})")
            return False
        emit("  Phash submit: sent to StashDB")
        return True
    except Exception as e:
        emit(f"  Phash submit: error ({e})")
        return False


# ---------------------------------------------------------------------------
# Filename parser
# ---------------------------------------------------------------------------

def parse_filename(filename: str, settings: dict) -> dict:
    """
    Attempt to extract site, date, performers, and title from a filename.
    Returns a dict with keys: site, date, performers, title (all may be empty).
    Tries patterns in order based on settings["filename_patterns"].
    """
    import json as _json

    stem = Path(filename).stem

    # Strip gubbins words from stem first
    strip_words = [w.strip().lower() for w in settings.get("filename_strip_words", "").split(",") if w.strip()]
    for word in strip_words:
        stem = re.sub(re.escape(word), "", stem, flags=re.IGNORECASE).strip(".-_ ")

    # Load site abbreviations and expand them
    try:
        abbrevs = _json.loads(settings.get("site_abbreviations", "{}"))
    except Exception:
        abbrevs = {}

    try:
        rename_map = _json.loads(settings.get("site_rename_map", "{}"))
    except Exception:
        rename_map = {}

    def expand_abbrev(text: str) -> str:
        # First try rename map (exact match)
        for old_name, new_name in rename_map.items():
            if text.strip().lower() == old_name.strip().lower():
                return new_name
        # Then try abbreviations
        low = text.lower()
        for abbr, full in abbrevs.items():
            if low == abbr.lower():
                return full
        return text

    def clean(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace(".", " ").replace("_", " ")).strip()

    result = {"site": "", "date": "", "performers": "", "title": ""}

    patterns = settings.get("filename_patterns", "pipe|dot_date").split("|")

    for pattern in patterns:
        pattern = pattern.strip()

        # Pipe-separated namer format: site|date|performers|title|network|parent
        if pattern == "pipe":
            parts = stem.split("|")
            if len(parts) >= 4:
                result["site"]       = expand_abbrev(parts[0].strip())
                result["date"]       = parts[1].strip()
                result["performers"] = parts[2].strip()
                result["title"]      = parts[3].strip()
                return result

        # site.YYYY.MM.DD.title (dot separated, full year)
        elif pattern == "dot_date":
            m = re.match(r'^(.+?)\.(20\d{2}|19\d{2})\.(\d{2})\.(\d{2})\.(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(clean(m.group(1)))
                result["date"]  = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
                result["title"] = clean(m.group(5))
                return result

        # site.YY.MM.DD.title (dot separated, short year)
        elif pattern == "dot_date_short":
            m = re.match(r'^(.+?)\.(\d{2})\.(\d{2})\.(\d{2})\.(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(clean(m.group(1)))
                result["date"]  = f"20{m.group(2)}-{m.group(3)}-{m.group(4)}"
                result["title"] = clean(m.group(5))
                return result

        # [Site] Title - Performer (YYYY)
        elif pattern == "bracket_site":
            m = re.match(r'^\[(.+?)\]\s*(.+?)(?:\s*-\s*(.+?))?\s*(?:\((\d{4})\))?$', stem)
            if m:
                result["site"]       = expand_abbrev(m.group(1).strip())
                result["title"]      = (m.group(2) or "").strip()
                result["performers"] = (m.group(3) or "").strip()
                result["date"]       = m.group(4) or ""
                return result

        # site - YYYY-MM-DD - title
        elif pattern == "dash_date":
            m = re.match(r'^(.+?)\s*[-–]\s*(20\d{2}|19\d{2}-\d{2}-\d{2})\s*[-–]\s*(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(m.group(1).strip())
                result["date"]  = m.group(2).strip()
                result["title"] = m.group(3).strip()
                return result

    # Fallback: return cleaned stem as title
    result["title"] = clean(stem)
    return result


def apply_rename_map(name: str, settings: dict) -> str:
    """Apply site rename corrections to a studio name."""
    import json as _json
    try:
        rename_map = _json.loads(settings.get("site_rename_map", "{}"))
    except Exception:
        rename_map = {}
    for old_name, new_name in rename_map.items():
        if name.strip().lower() == old_name.strip().lower():
            return new_name
    return name


def find_studio_dir(studio_name: str, settings: dict) -> Path | None:
    studio_name = apply_rename_map(studio_name, settings)
    series_dir = Path(settings.get("series_dir", ""))
    if not series_dir.exists():
        return None
    norm = normalise(studio_name)
    for folder in (d.name for d in series_dir.iterdir() if d.is_dir()):
        if normalise(folder) == norm:
            return series_dir / folder
    return None


def find_performer_dir(performers: list, performer_dirs: list) -> tuple | None:
    # Strictly female performers first
    female_names = [
        p["performer"]["name"] for p in performers
        if (p["performer"].get("gender") or "").upper() in ("FEMALE", "TRANSGENDER_FEMALE")
    ]
    # Performers with no gender info as fallback
    unknown_names = [
        p["performer"]["name"] for p in performers
        if not (p["performer"].get("gender") or "").strip()
    ]
    # Try female first, then unknown, then all as last resort
    candidates = female_names or unknown_names or [p["performer"]["name"] for p in performers]

    for entry in sorted(performer_dirs, key=lambda x: x["rank"]):
        base = Path(entry["path"])
        if not base.exists():
            continue
        norm_folders = {normalise(d.name): d.name for d in base.iterdir() if d.is_dir()}
        for name in candidates:
            if normalise(name) in norm_folders:
                return base / norm_folders[normalise(name)], name
    return None


def _find_performer_dir_with_aliases(performers: list, performer_dirs: list) -> tuple | None:
    """Fallback: fetch aliases from all sources and try matching those against folders."""
    # Build a set of all normalised folder names across all dirs
    all_folders = {}  # normalised -> (base_path, original_name)
    for entry in sorted(performer_dirs, key=lambda x: x["rank"]):
        base = Path(entry["path"])
        if not base.exists():
            continue
        for d in base.iterdir():
            if d.is_dir():
                key = normalise(d.name)
                if key not in all_folders:
                    all_folders[key] = (base / d.name, d.name)

    if not all_folders:
        return None

    # Gender-priority ordering (same as find_performer_dir)
    female_names = [
        p["performer"]["name"] for p in performers
        if (p["performer"].get("gender") or "").upper() in ("FEMALE", "TRANSGENDER_FEMALE")
    ]
    unknown_names = [
        p["performer"]["name"] for p in performers
        if not (p["performer"].get("gender") or "").strip()
    ]
    candidates = female_names or unknown_names or [p["performer"]["name"] for p in performers]

    for name in candidates:
        aliases = _fetch_performer_aliases(name)
        emit(f"  ALIAS lookup '{name}' → found {len(aliases)} aliases: {aliases[:5]}")
        for alias in aliases:
            norm_alias = normalise(alias)
            if norm_alias in all_folders:
                folder_path, folder_name = all_folders[norm_alias]
                emit(f"  ALIAS match: '{name}' → alias '{alias}' → folder '{folder_name}'")
                return folder_path, folder_name
    emit(f"  ALIAS no match found (checked {len(all_folders)} folders)")
    return None


def _performer_objects_in_match_order(performers: list) -> list:
    """Same gender priority as find_performer_dir; returns performer dicts from scene."""
    female = []
    for p in performers:
        pf = p.get("performer") or {}
        if (pf.get("gender") or "").upper() in ("FEMALE", "TRANSGENDER_FEMALE"):
            female.append(pf)
    if female:
        return female
    unk = []
    for p in performers:
        pf = p.get("performer") or {}
        if not (pf.get("gender") or "").strip():
            unk.append(pf)
    if unk:
        return unk
    return [p.get("performer") or {} for p in performers]


def _performer_crosswalk_ids(pf: dict, source: str) -> tuple[str, str, str]:
    """Map scene performer id to favourite_entities columns for the active match source."""
    pid = str((pf or {}).get("id") or "").strip()
    if not pid:
        return "", "", ""
    s = (source or "").upper()
    if "MANUAL" in s:
        return pid, pid, pid
    if "STASH" in s:
        return "", pid, ""
    if "FANS" in s:
        return "", "", pid
    return pid, "", ""


def _library_index_queue_matching_enabled(settings: dict) -> bool:
    """Prefer saved folder links from the library entity index when filing the queue."""
    v = settings.get("library_index_queue_matching_enabled")
    if v is None:
        v = settings.get("favourites_crosswalk_queue_enabled", "true")
    return str(v).lower() == "true"


def _find_performer_dir_via_library_index(
    performers: list,
    source: str,
) -> tuple | None:
    """Use library index (TPDB/Stash/Fans ids per folder) before name/alias matching."""
    for pf in _performer_objects_in_match_order(performers):
        tid, sid, fid = _performer_crosswalk_ids(pf, source)
        row = db.favourite_find_performer_folder_by_crosswalk(tid, sid, fid)
        if not row:
            continue
        path = Path(row["path"])
        if not path.is_dir():
            continue
        name = (pf.get("name") or row.get("folder_name") or "").strip() or row.get("folder_name") or "Unknown"
        emit(f"  Library index: performer id → folder '{row['folder_name']}'")
        return path, name
    return None


def _studio_crosswalk_ids(studio_obj: dict, source: str) -> tuple[str, str, str]:
    sid = str((studio_obj or {}).get("id") or "").strip()
    if not sid:
        return "", "", ""
    s = (source or "").upper()
    if "MANUAL" in s:
        return sid, sid, sid
    if "STASH" in s:
        return "", sid, ""
    if "FANS" in s:
        return "", "", sid
    return sid, "", ""


def _fetch_performer_aliases(name: str, quiet: bool = False) -> list[str]:
    """Search all configured sources for a performer and return their aliases (parallel)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _ae(msg: str) -> None:
        if not quiet:
            emit(msg)

    settings = db.get_settings()
    tasks = []

    def _tpdb_aliases():
        aliases = []
        try:
            resp = requests.get(TPDB_PERFORMER_SEARCH, params={"q": name},
                                headers=_tpdb_headers(), timeout=10)
            if resp.status_code == 200:
                perf_data = resp.json().get("data") or []
                # Include primary names of ALL search results (not just first)
                for p in perf_data[:5]:
                    pname = p.get("name", "")
                    if pname and pname not in aliases:
                        aliases.append(pname)
                # Fetch detailed aliases from the top result
                if perf_data:
                    pid = perf_data[0].get("id") or perf_data[0].get("_id")
                    if pid:
                        detail = fetch_performer_detail("TPDB", str(pid))
                        if detail:
                            for a in (detail.get("aliases") or []):
                                aname = a if isinstance(a, str) else a.get("name", "")
                                if aname and aname not in aliases:
                                    aliases.append(aname)
                            _ae(f"    TPDB aliases for '{name}': {aliases}")
                        else:
                            _ae(f"    TPDB no detail for pid={pid}")
                else:
                    _ae(f"    TPDB no results for '{name}'")
        except Exception as e:
            _ae(f"    TPDB alias error: {e}")
        return aliases

    def _stashdb_aliases():
        aliases = []
        try:
            gql = """
            query ($name: String!) {
              searchPerformer(term: $name, limit: 5) {
                id name aliases
              }
            }
            """
            resp = requests.post(
                "https://stashdb.org/graphql",
                json={"query": gql, "variables": {"name": name}},
                headers={"ApiKey": settings['api_key_stashdb'],
                          "Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                for p in (data.get("searchPerformer") or []):
                    pname = p.get("name", "")
                    if pname and pname not in aliases:
                        aliases.append(pname)
                    for a in (p.get("aliases") or []):
                        if a and a not in aliases:
                            aliases.append(a)
        except Exception:
            pass
        return aliases

    def _fansdb_aliases():
        aliases = []
        try:
            gql = """
            query ($term: String!) {
              searchPerformer(term: $term, limit: 5) {
                id name aliases
              }
            }
            """
            data = _fansdb_gql(gql, {"term": name})
            for p in (data.get("searchPerformer") or []):
                pname = p.get("name", "")
                if pname and pname not in aliases:
                    aliases.append(pname)
                for a in (p.get("aliases") or []):
                    if a and a not in aliases:
                        aliases.append(a)
        except Exception:
            pass
        return aliases

    if settings.get("api_key_tpdb"):
        tasks.append(_tpdb_aliases)
    if settings.get("api_key_stashdb"):
        tasks.append(_stashdb_aliases)
    if settings.get("api_key_fansdb"):
        tasks.append(_fansdb_aliases)

    if not tasks:
        return []

    all_aliases = []
    seen = set()
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [pool.submit(fn) for fn in tasks]
        for future in as_completed(futures):
            for a in future.result():
                if a and a not in seen:
                    seen.add(a)
                    all_aliases.append(a)

    return all_aliases

# ---------------------------------------------------------------------------
# Filename pattern rendering
# ---------------------------------------------------------------------------

def apply_caps(text: str) -> str:
    return " ".join(w.upper() if w.lower() in UPPERCASE_KEYWORDS else w for w in text.split())


def render_pattern(pattern: str, fields: dict) -> str:
    """Substitute {field} tokens in a pattern string."""
    result = pattern
    for key, value in fields.items():
        result = result.replace("{" + key + "}", str(value))
    return result

# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------

def emit(msg: str) -> None:
    processing_state["log"].append(msg)
    if log_queue:
        try:
            log_queue.put_nowait(msg)
        except Exception:
            pass


def safe_move(src: Path, dst: Path) -> None:
    try:
        shutil.move(str(src), str(dst))
    except (OSError, PermissionError) as e:
        if "cross-device" in str(e).lower() or getattr(e, "errno", None) in (18, 1):
            shutil.copyfile(str(src), str(dst))
            os.remove(str(src))
        else:
            raise


def download_image(url: str, dest: Path) -> bool:
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            dest.write_bytes(resp.content)
            return True
    except Exception:
        pass
    return False


def best_image_url(images: list) -> str | None:
    valid = [i for i in (images or []) if i.get("url")]
    if not valid:
        return None
    return max(valid, key=lambda i: (i.get("width") or 0) * (i.get("height") or 0))["url"]


def build_nfo(title, show_title, studio, performers, date) -> str:
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        year, mmdd = str(dt.year), dt.strftime("%m%d")
    except Exception:
        year, mmdd = "0000", "0000"
    root = ET.Element("episodedetails")
    for tag, val in [("title", title), ("showtitle", show_title), ("season", year),
                     ("episode", mmdd), ("aired", date), ("premiered", date),
                     ("year", year), ("studio", studio),
                     ("dateadded", f"{date} 00:00:00"), ("mpaa", "Adult")]:
        ET.SubElement(root, tag).text = val
    ET.SubElement(root, "tag").text = "Porn"
    ET.SubElement(root, "tag").text = "XXX"
    for name in performers:
        actor = ET.SubElement(root, "actor")
        ET.SubElement(actor, "name").text = name.strip()
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue().decode("utf-8")

# ---------------------------------------------------------------------------
# Filing logic (shared between pipeline and manual filing)
# ---------------------------------------------------------------------------

def file_scene_from_match(video: Path, scene: dict, source: str = "") -> dict:
    """
    File a video using a scene dict (stash-box shape).
    Returns a result dict with status and optional destination.
    """
    filename = video.name
    settings = db.get_settings()
    performer_dirs = db.get_directories("performer")

    title      = apply_caps(scene.get("title") or "Untitled")
    date       = scene.get("release_date") or ""
    studio_obj = scene.get("studio") or {}
    studio     = studio_obj.get("name") or ""
    performers = scene.get("performers") or []
    images     = scene.get("images") or []
    perf_names = [p["performer"]["name"] for p in performers]

    emit(f"  Match: [{source}] {studio} - {date} - {title}")
    emit(f"  Performers: {', '.join(perf_names) or 'Unknown'}")

    if not date:
        emit("  ERROR: no release date in match")
        db.update_file(filename, status="error", error="no release date")
        return {"status": "error", "error": "no release date"}

    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        year, month, day = str(dt.year), dt.strftime("%m"), dt.strftime("%d")
    except Exception:
        emit(f"  ERROR: unparseable date '{date}'")
        db.update_file(filename, status="error", error=f"bad date: {date}")
        return {"status": "error"}

    season_token = f"Season {year}"
    ext = video.suffix

    base_fields = {
        "title":      title,
        "studio":     apply_caps(studio) if studio else "Unknown",
        "date":       date,
        "year":       year,
        "month":      month,
        "day":        day,
        "source":     source,
        "performers": ", ".join(perf_names),
    }

    cross_on = _library_index_queue_matching_enabled(settings)

    studio_dir = None
    if cross_on:
        st_tp, st_st, st_fn = _studio_crosswalk_ids(studio_obj, source)
        row_st = db.favourite_find_studio_folder_by_crosswalk(st_tp, st_st, st_fn)
        if row_st:
            cand = Path(row_st["path"])
            if cand.is_dir():
                studio_dir = cand
                emit(f"  Series: library index → {row_st['folder_name']}")
    if studio_dir is None:
        studio_dir = find_studio_dir(studio, settings) if studio else None

    if studio_dir:
        fields     = {**base_fields, "performer": perf_names[0] if perf_names else "Unknown"}
        pattern    = settings.get("pattern_series", db.DEFAULTS["pattern_series"])
        base_name  = render_pattern(pattern, fields)
        show_title = apply_caps(studio)
        nfo_title  = f"{', '.join(perf_names)} - {title}" if perf_names else title
        dest_season = studio_dir / season_token
        route      = f"Series/{studio_dir.name}"
    else:
        result = None
        if cross_on:
            result = _find_performer_dir_via_library_index(performers, source)
        if not result:
            result = find_performer_dir(performers, performer_dirs)
        if not result and settings.get("alias_lookup_enabled", "false") == "true":
            emit("  No direct performer match — checking aliases...")
            result = _find_performer_dir_with_aliases(performers, performer_dirs)
        if not result:
            emit(f"  WARNING: no Series dir for '{studio}' and no performer dir match")
            db.update_file(filename, status="no_dir", match_source=source,
                           match_title=title, match_studio=studio, match_date=date,
                           performers=", ".join(perf_names))
            return {"status": "no_dir"}

        performer_path, performer_name = result
        fields     = {**base_fields, "performer": performer_name}
        pattern    = settings.get("pattern_performer", db.DEFAULTS["pattern_performer"])
        base_name  = render_pattern(pattern, fields)
        show_title = performer_name
        nfo_title  = f"{performer_name} - {title}"
        dest_season = performer_path / season_token
        route      = f"{performer_path.parent.name}/{performer_path.name}"

    emit(f"  Route: {route}")
    emit(f"  Dest:  {base_name}{ext}")

    dest_season.mkdir(parents=True, exist_ok=True)

    nfo = build_nfo(nfo_title, show_title, studio or "Unknown", perf_names, date)
    (dest_season / f"{base_name}.nfo").write_text(nfo, encoding="utf-8")
    emit("  NFO: saved")

    # Prefer generated base64 thumb over URL
    thumb_data_url = scene.get("_thumb_data_url", "")
    if thumb_data_url and thumb_data_url.startswith("data:image"):
        try:
            header, b64data = thumb_data_url.split(",", 1)
            (dest_season / f"{base_name}-thumb.jpg").write_bytes(base64.b64decode(b64data))
            emit("  Thumb: saved (generated frame)")
        except Exception as e:
            emit(f"  Thumb: save failed ({e})")
    else:
        image_url = best_image_url(images)
        if image_url:
            ok = download_image(image_url, dest_season / f"{base_name}-thumb.jpg")
            emit("  Thumb: downloaded" if ok else "  Thumb: download failed")
        else:
            emit("  Thumb: no image available")

    safe_move(video, dest_season / f"{base_name}{ext}")
    emit("  Video: moved")

    destination = str(dest_season / f"{base_name}{ext}")
    db.update_file(filename, status="filed", match_source=source,
                   match_title=title, match_studio=studio, match_date=date,
                   performers=", ".join(perf_names), destination=destination)

    # Submit phash to StashDB if match came from StashDB and we have a phash cached
    if source == "StashDB":
        scene_id = scene.get("id")
        phash = db.get_phash(filename)
        if scene_id and phash:
            submit_phash_to_stashdb(scene_id, phash)

    # Trigger media server scans (Stash will discover the file and scrape metadata)
    trigger_media_scans(destination)

    emit("  DONE")
    return {"status": "filed", "destination": destination}


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def process_single(video: Path) -> dict:
    filename = video.name
    db.upsert_file(filename)
    emit(f"FILE {filename}")

    settings = db.get_settings()
    performer_dirs = db.get_directories("performer")

    # Phash - check database first, compute and store if missing
    cached = db.get_phash(filename)
    if cached:
        phash = cached
        emit(f"  Phash: cached ({phash})")
    else:
        try:
            emit("  Phash: computing...")
            phash = compute_phash(video)
            emit(f"  Phash: done ({phash})")
        except Exception as e:
            emit(f"  Phash: FAILED - {e}")
            db.update_file(filename, status="error", error=str(e))
            return {"status": "error", "error": str(e)}

    db.update_file(filename, status="processing", phash=phash)

    # Lookup
    try:
        emit("  Lookup: querying databases...")
        matches, source = query_with_fallback(phash)
    except Exception as e:
        emit(f"  Lookup: FAILED - {e}")
        db.update_file(filename, status="error", error=str(e))
        return {"status": "error", "error": str(e)}

    if not matches:
        emit("  Lookup: no match found")
        db.update_file(filename, status="unmatched")
        return {"status": "unmatched"}

    return file_scene_from_match(video, matches[0], source=source)


def run_pipeline(filenames: list = None) -> None:
    processing_state["running"] = True
    processing_state["log"] = []
    try:
        settings = db.get_settings()
        source_dir = Path(settings.get("source_dir", ""))
        if filenames:
            video_files = [source_dir / f for f in filenames if (source_dir / f).exists()]
        else:
            video_files = sorted([
                f for f in source_dir.iterdir()
                if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
            ])
        emit(f"PIPELINE START - {len(video_files)} file(s)")
        emit("---")
        for video in video_files:
            processing_state["current_file"] = video.name
            process_single(video)
            emit("---")
        emit("PIPELINE COMPLETE")
    except Exception as e:
        emit(f"PIPELINE ERROR: {e}")
    finally:
        processing_state["running"] = False
        processing_state["current_file"] = None

# ---------------------------------------------------------------------------
# TMDB movie search and filing
# ---------------------------------------------------------------------------

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"


def get_tmdb_headers() -> dict:
    s = db.get_settings()
    return {
        "Authorization": f"Bearer {s.get('api_key_tmdb', '')}",
        "Accept": "application/json",
    }


def search_tmdb(query: str, year: str = None) -> list[dict]:
    params = {"query": query, "include_adult": "true", "language": "en-US", "page": 1}
    if year:
        params["year"] = year
    resp = requests.get(f"{TMDB_BASE}/search/movie", params=params,
                        headers=get_tmdb_headers(), timeout=15)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return [{
        "id":          str(r["id"]),
        "title":       r.get("title") or r.get("original_title") or "Unknown",
        "year":        (r.get("release_date") or "")[:4],
        "overview":    r.get("overview") or "",
        "poster_url":  f"{TMDB_IMG_BASE}{r['poster_path']}" if r.get("poster_path") else None,
        "rating":      r.get("vote_average"),
    } for r in results]


def get_tmdb_movie(tmdb_id: str) -> dict:
    resp = requests.get(f"{TMDB_BASE}/movie/{tmdb_id}",
                        params={"append_to_response": "credits"},
                        headers=get_tmdb_headers(), timeout=15)
    resp.raise_for_status()
    data = resp.json()
    cast = [m["name"] for m in data.get("credits", {}).get("cast", [])[:10]]
    crew = data.get("credits", {}).get("crew", [])
    directors = [m["name"] for m in crew if m.get("job") == "Director"]
    return {
        "id":           str(data["id"]),
        "title":        data.get("title") or data.get("original_title") or "Unknown",
        "year":         (data.get("release_date") or "")[:4],
        "overview":     data.get("overview") or "",
        "poster_url":   f"{TMDB_IMG_BASE}{data['poster_path']}" if data.get("poster_path") else None,
        "backdrop_url": f"https://image.tmdb.org/t/p/original{data['backdrop_path']}" if data.get("backdrop_path") else None,
        "rating":       data.get("vote_average"),
        "cast":         cast,
        "directors":    directors,
        "genres":       [g["name"] for g in data.get("genres", [])],
        "runtime":      data.get("runtime"),
        "tagline":      data.get("tagline") or "",
    }


def build_movie_nfo(movie: dict, filename: str) -> str:
    root = ET.Element("movie")
    ET.SubElement(root, "title").text        = movie["title"]
    ET.SubElement(root, "originaltitle").text = movie["title"]
    ET.SubElement(root, "year").text         = movie.get("year") or ""
    ET.SubElement(root, "plot").text         = movie.get("overview") or ""
    ET.SubElement(root, "outline").text      = movie.get("tagline") or ""
    ET.SubElement(root, "rating").text       = str(movie.get("rating") or "")
    ET.SubElement(root, "runtime").text      = str(movie.get("runtime") or "")
    ET.SubElement(root, "id").text           = movie.get("id") or ""
    ET.SubElement(root, "tmdbid").text       = movie.get("id") or ""
    ET.SubElement(root, "mpaa").text         = "Adult"
    ET.SubElement(root, "tag").text          = "Porn"
    ET.SubElement(root, "tag").text          = "XXX"
    for genre in movie.get("genres") or []:
        ET.SubElement(root, "genre").text = genre
    for director in movie.get("directors") or []:
        ET.SubElement(root, "director").text = director
    for name in movie.get("cast") or []:
        actor = ET.SubElement(root, "actor")
        ET.SubElement(actor, "name").text = name
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue().decode("utf-8")


def file_movie(
    video: Path,
    movie: dict,
    *,
    tpdb_id: str | None = None,
    match_source: str = "tmdb",
) -> dict:
    """File a movie using TMDB metadata (or TPDB-shaped dict from _tpdb_detail_as_movie_filing_dict)."""
    filename = video.name
    db.upsert_movie(filename)
    settings = db.get_settings()
    features_dir = Path(settings.get("features_dir", ""))
    if not (settings.get("features_dir") or "").strip() or not features_dir.exists():
        db.update_movie(
            filename,
            status="no_dir",
            error="Features directory not configured or does not exist",
        )
        emit("  ERROR: features_dir missing")
        return {"status": "no_dir"}

    title = movie["title"]
    year  = movie.get("year") or "0000"
    folder_name = f"{title} ({year})"
    dest_dir = features_dir / folder_name
    ext = video.suffix
    base_name = f"{title} ({year})"

    emit(f"  Movie: {title} ({year})")
    emit(f"  Dest:  {folder_name}/{base_name}{ext}")

    dest_dir.mkdir(parents=True, exist_ok=True)

    # NFO
    nfo = build_movie_nfo(movie, filename)
    (dest_dir / f"{base_name}.nfo").write_text(nfo, encoding="utf-8")
    emit("  NFO: saved")

    # Poster
    if movie.get("poster_url"):
        if download_image(movie["poster_url"], dest_dir / f"{base_name}-poster.jpg"):
            emit("  Poster: downloaded")
        else:
            emit("  Poster: download failed")

    # Backdrop / fanart
    if movie.get("backdrop_url"):
        if download_image(movie["backdrop_url"], dest_dir / f"{base_name}-fanart.jpg"):
            emit("  Fanart: downloaded")
        else:
            emit("  Fanart: download failed")

    # Move video
    safe_move(video, dest_dir / f"{base_name}{ext}")
    emit("  Video: moved")

    destination = str(dest_dir / f"{base_name}{ext}")
    tid = str(movie.get("id") or "").strip() or None
    db.update_movie(
        filename,
        status="filed",
        tmdb_id=tid,
        tpdb_id=(str(tpdb_id).strip() or None) if tpdb_id else None,
        match_source=match_source,
        title=title,
        year=year,
        overview=movie.get("overview"),
        poster_url=movie.get("poster_url"),
        destination=destination,
    )

    # Trigger media server scans
    trigger_media_scans(destination)

    emit("  DONE")
    return {"status": "filed", "destination": destination}


def guess_movie_query_from_filename(filename: str) -> tuple[str, str | None]:
    """Best-effort title + optional release year from a video filename."""
    stem = Path(filename).stem
    s = re.sub(r"\[[^\]]*\]", " ", stem, flags=re.I)
    s = s.replace("_", " ").replace(".", " ")
    year_m = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    year = year_m.group(1) if year_m else None
    title = re.sub(r"\s*\(?\s*(19\d{2}|20\d{2})\s*\)?\s*$", "", s).strip()
    title = re.sub(r"\s+", " ", title)
    return (title or stem, year)


def search_tpdb_movie_first_hit(query: str, year: str | None) -> dict | None:
    """First TPDB /movies hit for parse= query (no feed content filter — for filing)."""
    s = db.get_settings()
    if not (s.get("api_key_tpdb") or "").strip():
        return None
    params: dict = {"parse": query.strip(), "limit": 10}
    if year:
        params["year"] = year
    try:
        resp = requests.get(
            "https://api.theporndb.net/movies",
            params=params,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {s.get('api_key_tpdb', '')}",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        data = (resp.json() or {}).get("data") or []
        return data[0] if data else None
    except Exception:
        return None


def _tpdb_detail_as_movie_filing_dict(m: dict) -> dict:
    """Normalize TPDB movie payload for file_movie / build_movie_nfo."""
    poster = m.get("image") or m.get("poster") or m.get("poster_image")
    if not poster:
        posters_obj = m.get("posters") or {}
        if isinstance(posters_obj, dict):
            poster = posters_obj.get("full") or posters_obj.get("large")
    bg = m.get("back_image") or m.get("backdrop_url")
    performers: list[str] = []
    for p in m.get("performers") or []:
        if isinstance(p, dict):
            perf = p.get("performer") or p
            n = perf.get("name") or perf.get("full_name")
            if n:
                performers.append(str(n))
        elif isinstance(p, str):
            performers.append(p)
    dirs: list[str] = []
    for d in m.get("directors") or []:
        if isinstance(d, dict) and d.get("name"):
            dirs.append(str(d["name"]))
        elif isinstance(d, str):
            dirs.append(d)
    genres: list[str] = []
    for g in m.get("tags") or []:
        if isinstance(g, dict):
            n = g.get("name") or (g.get("tag") or {}).get("name")
            if n:
                genres.append(str(n))
    y = (m.get("date") or "")[:4] or "0000"
    mid = str(m.get("_id") or m.get("id") or "")
    return {
        "id": "",
        "title": m.get("title") or m.get("name") or "Unknown",
        "year": y,
        "overview": m.get("description") or m.get("synopsis") or "",
        "poster_url": poster,
        "backdrop_url": bg if isinstance(bg, str) and bg.startswith("http") else None,
        "rating": m.get("rating"),
        "cast": performers,
        "directors": dirs,
        "genres": genres,
        "runtime": m.get("duration"),
        "tagline": "",
        "_tpdb_id": mid,
    }


def process_movie_file(filename: str, src: Path | None = None) -> None:
    settings = db.get_settings()
    if src is None:
        src = Path((settings.get("movies_source_dir") or "").strip())
    video = src / filename
    if not video.exists():
        return
    existing = db.get_movie_by_filename(filename)
    if existing and existing.get("status") == "filed":
        emit("  Already filed — skipped")
        return
    db.upsert_movie(filename)
    title_guess, year_guess = guess_movie_query_from_filename(filename)
    movie_dict: dict | None = None
    tpdb_id_used: str | None = None
    match_src = "tmdb"

    tmdb_results = None
    try:
        if (settings.get("api_key_tmdb") or "").strip():
            tmdb_results = search_tmdb(title_guess, year_guess)
    except Exception:
        tmdb_results = None
    if tmdb_results:
        try:
            movie_dict = get_tmdb_movie(tmdb_results[0]["id"])
            match_src = "tmdb"
        except Exception:
            movie_dict = None
    if not movie_dict:
        hit = search_tpdb_movie_first_hit(title_guess, year_guess)
        if hit:
            mid = str(hit.get("_id") or hit.get("id") or "")
            detail = _fetch_tpdb_movie_detail(mid) if mid else {}
            merged = {**hit, **detail} if detail else hit
            movie_dict = _tpdb_detail_as_movie_filing_dict(merged)
            tpdb_id_used = movie_dict.pop("_tpdb_id", None) or mid
            match_src = "tpdb"
    if not movie_dict:
        db.update_movie(
            filename,
            status="unmatched",
            error="No TMDB or TPDB match for parsed title",
        )
        emit("  No TMDB/TPDB match")
        return
    result = file_movie(
        video,
        movie_dict,
        tpdb_id=tpdb_id_used if match_src == "tpdb" else None,
        match_source=match_src,
    )
    if isinstance(result, dict) and result.get("status") == "no_dir":
        return


def run_movie_pipeline(filenames: list[str] | None = None) -> None:
    if processing_state["running"]:
        return
    settings = db.get_settings()
    src = Path((settings.get("movies_source_dir") or "").strip())
    if not src.exists():
        emit("MOVIE PIPELINE: movies_source_dir not configured or missing")
        return
    if filenames is None:
        terminal = db.get_movie_terminal_filenames()
        filenames = []
        for f in sorted(src.iterdir()):
            if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
                if f.name not in terminal:
                    filenames.append(f.name)
    if not filenames:
        emit("MOVIE PIPELINE: nothing to process")
        return
    processing_state["running"] = True
    try:
        emit("PIPELINE MOVIES START")
        for fn in filenames:
            processing_state["current_file"] = fn
            emit(f"FILE {fn}")
            try:
                process_movie_file(fn, src)
            except Exception as e:
                db.update_movie(fn, status="error", error=str(e))
                emit(f"  ERROR: {e}")
            emit("---")
        emit("PIPELINE MOVIES COMPLETE")
    finally:
        processing_state["running"] = False
        processing_state["current_file"] = None


# ---------------------------------------------------------------------------
# Performer / Studio metadata scraper
# ---------------------------------------------------------------------------

TPDB_PERFORMER_SEARCH = "https://api.theporndb.net/performers"
TPDB_PERFORMER_DETAIL = "https://api.theporndb.net/performers/{id}"
TPDB_STUDIO_SEARCH    = "https://api.theporndb.net/sites"
TPDB_STUDIO_DETAIL    = "https://api.theporndb.net/sites/{id}"

FANSDB_PERFORMER_SEARCH_GQL = """
query($term: String!) {
  searchPerformer(term: $term, limit: 10) {
    id name images { url }
  }
}
"""

FANSDB_PERFORMER_DETAIL_GQL = """
query($id: ID!) {
  findPerformer(id: $id) {
    name disambiguation aliases birth_date ethnicity country
    images { url }
  }
}
"""


def _tpdb_headers() -> dict:
    s = db.get_settings()
    return {"Accept": "application/json",
            "Authorization": f"Bearer {s.get('api_key_tpdb', '')}"}


def _extract_movie_genders(movie_data: dict) -> list[str]:
    """Extract performer genders from a TPDB movie/detail payload."""
    genders = []
    for p in (movie_data.get("performers") or []):
        if not isinstance(p, dict):
            continue
        perf = p.get("performer") or {}
        candidates = [
            ((p.get("parent") or {}).get("extras", {}) or {}).get("gender"),
            ((p.get("extra") or {}).get("gender")),
            (p.get("gender")),
            ((perf or {}).get("gender")),
            ((perf or {}).get("sex")),
        ]
        for g in candidates:
            if not g:
                continue
            g = str(g).strip().upper()
            if g and g != "UNKNOWN":
                genders.append(g)
                break
    return genders


def _classify_content(genders: list[str]) -> str:
    """Classify content from performer genders (first match wins).

    1. Any trans / non-binary / intersex-style label -> 'trans'
    2. Else: only females, exactly one -> 'solo_female'
    3. Else: more than one female, no males -> 'lesbian'
    4. Else: only male(s) -> 'gay'
    5. Else: at least one male and at least one female -> 'straight'
    6. Else -> 'unknown'
    """
    if not genders:
        return "unknown"

    females = 0
    males = 0
    trans = 0
    for g in genders:
        g = str(g).upper()
        if g in ("FEMALE", "F", "WOMAN", "CIS_FEMALE"):
            females += 1
        elif g in ("MALE", "M", "MAN", "CIS_MALE", "BOY", "GUY", "MALE_PERFORMER"):
            males += 1
        elif "TRANS" in g or g in ("NON_BINARY", "NONBINARY", "NON-BINARY", "INTERSEX", "OTHER"):
            trans += 1

    # 1. Trans / non-binary / intersex-style
    if trans > 0:
        return "trans"
    # 2. Single female, no males
    if females == 1 and males == 0:
        return "solo_female"
    # 3. Multiple females, no males
    if females > 1 and males == 0:
        return "lesbian"
    # 4. One or more males, no females
    if males > 0 and females == 0:
        return "gay"
    # 5. Mixed male and female
    if males > 0 and females > 0:
        return "straight"
    # 6. Unclassifiable (e.g. unrecognized gender strings only)
    return "unknown"


def _get_content_filter_config() -> tuple[dict[str, bool], bool, set[str]]:
    """Return content-filter settings as (cats, filter_active, allowed)."""
    s = db.get_settings()
    cats = {
        "straight":    s.get("cat_straight", "true") == "true",
        "lesbian":     s.get("cat_lesbian", "true") == "true",
        "gay":         s.get("cat_gay", "true") == "true",
        "solo_female": s.get("cat_solo_female", "true") == "true",
        "trans":       s.get("cat_trans", "true") == "true",
    }
    allowed = {k for k, v in cats.items() if v}
    filter_active = len(allowed) != 5
    return cats, filter_active, allowed


def _title_category_hint(title: str, studio: str = "") -> str:
    text = f" {(title or '').lower()} {(studio or '').lower()} "

    gay_terms = [
        " dick ", " dicks ", " bro ", " bros ", " men ", " boys ", " male ",
        " gay ", " twink ", " twinks ", " bareback ", " jock ", " dudes ",
        " me cum ", " loads ", " daddy ", " cocks ", " blowjob boys ",
        " corbin fisher ", " hot twinks ",
    ]
    lesbian_terms = [
        " lesbian ", " lesbians ", " girlsway ", " girlfriends films ",
        " girl girl ", " all girls ", " all-girl ", " stepmommy ",
    ]
    # Avoid bare "solo" — male solo titles often include it and were mislabeled as solo_female.
    solo_female_terms = [" masturbation ", " self play ", " solo girl ", " solochick "]
    female_context = (
        " her ", " she ", " milf ", " wife ", " girls ", " girl ", " babe ", " woman ",
        " women ", " mom ", " mommy ", " daughter ", " stepsis ", " step sis ",
    )
    trans_terms = [
        " trans ", " shemale ", " tgirl ", " tgirl ", " ts ",
    ]

    if any(term in text for term in trans_terms):
        return "trans"
    if any(term in text for term in gay_terms):
        return "gay"
    if any(term in text for term in lesbian_terms):
        return "lesbian"
    if any(term in text for term in solo_female_terms):
        if any(h in text for h in female_context):
            return "solo_female"
    if " solo " in text and any(h in text for h in female_context):
        return "solo_female"
    return "unknown"


def _fetch_tpdb_movie_detail(movie_id: str) -> dict:
    try:
        resp = requests.get(f"https://api.theporndb.net/movies/{movie_id}", headers=_tpdb_headers(), timeout=15)
        if resp.status_code != 200:
            return {}
        return resp.json().get("data") or {}
    except Exception:
        return {}


def _extract_movie_genders_with_fallback(movie_data: dict) -> tuple[list[str], str, str]:
    """Return (genders, source, category) for MOVIES.
    Prefer movie detail (accurate genders). If detail omits genders, try the list
    payload (often has partial gender). Then title/studio heuristic, then fail closed.
    source is one of detail/payload/title_heuristic/unknown."""
    movie_id = str(movie_data.get("_id") or movie_data.get("id") or "")
    if movie_id:
        detail = _fetch_tpdb_movie_detail(movie_id)
        genders = _extract_movie_genders(detail)
        category = _classify_content(genders)
        if genders and category != "unknown":
            return genders, "detail", category

    genders = _extract_movie_genders(movie_data)
    category = _classify_content(genders)
    if genders and category != "unknown":
        return genders, "payload", category

    # Only use title/studio guessing when API gender data is unusable.
    category = _title_category_hint(
        movie_data.get("title") or "",
        ((movie_data.get("site") or {}).get("name") or movie_data.get("studio") or ""),
    )
    if category != "unknown":
        return [], "title_heuristic", category

    return [], "unknown", "unknown"


def _passes_content_filter(genders, allowed: set = None, category: str | None = None) -> bool:
    """Check if content passes the category-based content filter.
    If no usable category data and filtering is active, exclude the content."""
    if isinstance(genders, set):
        genders = list(genders)
    if allowed is None:
        _, _, allowed = _get_content_filter_config()
        if len(allowed) == 5:
            return True

    if category is None:
        category = _classify_content(genders or [])

    if not category or category == "unknown":
        return False
    return category in allowed


def _tag_blacklist_normalized() -> frozenset[str]:
    """Lowercase tag names from settings `tag_blacklist` (comma-separated)."""
    raw = db.get_settings().get("tag_blacklist") or ""
    return frozenset(x.strip().lower() for x in raw.split(",") if x.strip())


def _tag_names_from_tpdb_entity(entity: dict) -> set[str]:
    """Lowercase tag names from a TPDB REST scene/movie payload."""
    names: set[str] = set()
    if not isinstance(entity, dict):
        return names
    raw = entity.get("tags")
    if not isinstance(raw, list):
        return names
    for t in raw:
        if isinstance(t, str) and t.strip():
            names.add(t.strip().lower())
        elif isinstance(t, dict):
            n = t.get("name") or t.get("label")
            if not n and isinstance(t.get("tag"), dict):
                n = t["tag"].get("name")
            if n and str(n).strip():
                names.add(str(n).strip().lower())
    return names


def _passes_tag_filter(entity: dict) -> bool:
    """Exclude if any item tag exactly matches a blacklisted tag (case-insensitive).

    If the blacklist is empty, always pass. If the API payload has no tags, pass
    (gender/category filters still apply)."""
    bl = _tag_blacklist_normalized()
    if not bl:
        return True
    item_tags = _tag_names_from_tpdb_entity(entity)
    if not item_tags:
        return True
    return not (item_tags & bl)


def _content_filters_fingerprint() -> tuple:
    """Invalidate feed/movie caches when category toggles or tag blacklist change."""
    s = db.get_settings()
    cats = tuple(
        s.get(k, "true") == "true"
        for k in ("cat_straight", "cat_lesbian", "cat_gay", "cat_solo_female", "cat_trans")
    )
    tag_bl = (s.get("tag_blacklist") or "").strip()
    return (cats, tag_bl)


def _movie_card_from_tpdb(m: dict) -> dict:
    poster = m.get("image") or m.get("poster") or m.get("poster_image")
    if not poster:
        posters_obj = m.get("posters") or {}
        if isinstance(posters_obj, dict):
            poster = posters_obj.get("full") or posters_obj.get("large")
    slug = m.get("slug") or str(m.get("_id", ""))
    return {
        "id": str(m.get("_id") or m.get("id") or ""),
        "slug": slug,
        "title": m.get("title") or "",
        "date": m.get("date") or "",
        "studio": (m.get("site") or {}).get("name") or "",
        "poster": poster,
        "link": f"https://theporndb.net/movies/{slug}",
    }


def _collect_filtered_tpdb_movies(base_params: dict, page: int, page_size: int = 20) -> dict:
    """Collect filtered TPDB movies, applying filter before local paging."""
    cats, filter_active, allowed = _get_content_filter_config()
    start = max(0, (int(page) - 1) * page_size)
    end = start + page_size
    upstream_page = 1
    accepted = []
    total_before_filter = 0
    excluded_unknown = 0
    excluded_by_category = 0
    excluded_by_tags = 0
    classification_sources = {"payload": 0, "detail": 0, "title_heuristic": 0, "unknown": 0}
    total_pages_hint = 1

    while True:
        params = dict(base_params)
        params["page"] = upstream_page
        params["limit"] = 50
        resp = requests.get("https://api.theporndb.net/movies", params=params, headers=_tpdb_headers(), timeout=15)
        if resp.status_code != 200:
            raise RuntimeError(f"TPDB returned {resp.status_code}")

        payload = resp.json() or {}
        raw_movies = payload.get("data") or []
        meta = payload.get("meta") or {}
        total_pages_hint = meta.get("last_page", total_pages_hint)
        total_before_filter += len(raw_movies)
        if not raw_movies:
            break

        for m in raw_movies:
            if filter_active:
                genders, source, category = _extract_movie_genders_with_fallback(m)
                classification_sources[source] = classification_sources.get(source, 0) + 1
                if category == "unknown":
                    excluded_unknown += 1
                    continue
                if not _passes_content_filter(genders, allowed, category):
                    excluded_by_category += 1
                    continue
            if not _passes_tag_filter(m):
                excluded_by_tags += 1
                continue
            accepted.append(_movie_card_from_tpdb(m))
            if len(accepted) >= end:
                break

        if len(accepted) >= end:
            break
        if upstream_page >= int(meta.get("last_page") or upstream_page):
            break
        upstream_page += 1

    page_results = accepted[start:end]
    total_after_filter = len(accepted)
    computed_total_pages = max(1, ((total_after_filter - 1) // page_size) + 1) if total_after_filter else 1
    return {
        "results": page_results,
        "page": int(page),
        "total_pages": computed_total_pages,
        "total": total_after_filter,
        "_debug": {
            "filter_active": filter_active,
            "cats": cats,
            "total_before_filter": total_before_filter,
            "total_after_filter": total_after_filter,
            "excluded_unknown": excluded_unknown,
            "excluded_by_category": excluded_by_category,
            "excluded_by_tags": excluded_by_tags,
            "classification_sources": classification_sources,
            "upstream_pages_scanned": upstream_page,
            "upstream_total_pages_hint": total_pages_hint,
        },
    }


ALLOWED_GENDER_BUCKETS = frozenset({"female", "male", "trans", "other"})


def normalize_performer_gender_to_bucket(g: str | None) -> str | None:
    """Map API gender strings to female | male | trans | other."""
    if not g or not str(g).strip():
        return None
    s = str(g).strip().upper().replace("-", "_").replace(" ", "_")
    if s in ("FEMALE", "F", "WOMAN", "CIS_FEMALE", "FEMALE_PERFORMER"):
        return "female"
    if s in ("MALE", "M", "MAN", "CIS_MALE", "MALE_PERFORMER", "BOY"):
        return "male"
    if "TRANS" in s or s in ("TRANSGENDER_FEMALE", "TRANSGENDER_MALE", "TRANSGENDER"):
        return "trans"
    if s in ("NON_BINARY", "NONBINARY", "INTERSEX", "OTHER", "NON_BINARY_GENDER"):
        return "other"
    return None


def gender_allowlist_from_json(s: str | None) -> frozenset[str] | None:
    """Empty list / missing = no filter. Non-empty JSON list → allow only those buckets."""
    if not s or not str(s).strip():
        return None
    try:
        arr = json.loads(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(arr, list) or len(arr) == 0:
        return None
    out = {x for x in arr if x in ALLOWED_GENDER_BUCKETS}
    return frozenset(out) if out else None


def gender_allowlist_from_list(lst: list | None) -> frozenset[str] | None:
    if not lst or not isinstance(lst, list) or len(lst) == 0:
        return None
    out = {x for x in lst if x in ALLOWED_GENDER_BUCKETS}
    return frozenset(out) if out else None


def _tpdb_raw_gender_from_search_hit(p: dict) -> str | None:
    v = p.get("gender")
    if v:
        return str(v)
    ex = p.get("extra")
    if isinstance(ex, dict) and ex.get("gender"):
        return str(ex["gender"])
    extras = p.get("extras") or {}
    if isinstance(extras, dict) and extras.get("gender"):
        return str(extras["gender"])
    return None


def _tpdb_detail_gender_bucket(tpdb_id: str) -> str | None:
    """Authoritative gender from TPDB performer detail (search may omit extras)."""
    if not tpdb_id or not str(tpdb_id).strip():
        return None
    try:
        resp = requests.get(
            TPDB_PERFORMER_DETAIL.format(id=str(tpdb_id).strip()),
            headers=_tpdb_headers(),
            timeout=12,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data")
        if not data or not isinstance(data, dict):
            return None
        raw = _tpdb_raw_gender_from_search_hit(data)
        return normalize_performer_gender_to_bucket(raw)
    except Exception:
        return None


def _stashdb_find_performer_gender_bucket(sid: str) -> str | None:
    """Authoritative gender from StashDB findPerformer (search may omit or differ)."""
    settings = db.get_settings()
    key = (settings.get("api_key_stashdb") or "").strip()
    if not key or not sid or not str(sid).strip():
        return None
    try:
        gql = """
        query($id: ID!) {
          findPerformer(id: $id) {
            gender
          }
        }
        """
        resp = requests.post(
            "https://stashdb.org/graphql",
            json={"query": gql, "variables": {"id": str(sid).strip()}},
            headers={"ApiKey": key, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        p = data.get("findPerformer") or {}
        raw_g = p.get("gender")
        raw_g = str(raw_g) if raw_g is not None else None
        return normalize_performer_gender_to_bucket(raw_g)
    except Exception:
        return None


def _fansdb_find_performer_gender_bucket(fid: str) -> str | None:
    if not fid or not str(fid).strip():
        return None
    try:
        data = _fansdb_gql(
            "query($id: ID!) { findPerformer(id: $id) { gender } }",
            {"id": str(fid).strip()},
        )
        p = data.get("findPerformer") or {}
        raw_g = p.get("gender")
        raw_g = str(raw_g) if raw_g is not None else None
        return normalize_performer_gender_to_bucket(raw_g)
    except Exception:
        return None


def _enforce_favourite_performer_slots_by_detail_gender(
    g_allow: frozenset[str],
    mt_id,
    mt_n,
    ms_id,
    ms_n,
    mf_id,
    mf_n,
):
    """Drop any link whose findPerformer / TPDB detail gender is not in the allowlist.
    Skips Stash/Fans verification when no API key (cannot call findPerformer)."""
    settings = db.get_settings()
    if mt_id:
        b = _tpdb_detail_gender_bucket(str(mt_id).strip())
        if b is None or b not in g_allow:
            mt_id = None
            mt_n = ""
    if ms_id and (settings.get("api_key_stashdb") or "").strip():
        b = _stashdb_find_performer_gender_bucket(str(ms_id).strip())
        if b is None or b not in g_allow:
            ms_id = None
            ms_n = ""
    if mf_id and (settings.get("api_key_fansdb") or "").strip():
        b = _fansdb_find_performer_gender_bucket(str(mf_id).strip())
        if b is None or b not in g_allow:
            mf_id = None
            mf_n = ""
    return mt_id, mt_n, ms_id, ms_n, mf_id, mf_n


def _apply_gender_allowlist(results: list[dict], allow: frozenset[str] | None) -> list[dict]:
    """When allowlist is set, keep only rows whose gender_bucket is in the set (unknown excluded)."""
    if not allow or len(allow) == 0:
        return results
    out: list[dict] = []
    for r in results:
        bucket = r.get("gender_bucket")
        if bucket is not None and bucket in allow:
            out.append(r)
    return out


def _fansdb_gql(query: str, variables: dict) -> dict:
    """FansDB is stash-box; use ApiKey like StashDB (Bearer alone is rejected)."""
    key = (db.get_settings().get("api_key_fansdb") or "").strip()
    if not key:
        return {}
    resp = requests.post(
        FANSDB_ENDPOINT,
        json={"query": query, "variables": variables},
        headers={"ApiKey": key, "Content-Type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors") and not body.get("data"):
        raise RuntimeError(str(body["errors"]))
    return body.get("data") or {}


def search_performers(
    name: str,
    *,
    gender_allowlist: frozenset[str] | None = None,
    strict_name_match: bool = False,
) -> list[dict]:
    """Search TPDB + StashDB + FansDB. If strict_name_match, fetch a wider result set then keep
    only rows whose normalised name equals the query (no fuzzy/single-hit guessing)."""
    results = []
    settings = db.get_settings()
    tpdb_take = 40 if strict_name_match else 5
    stash_limit = 40 if strict_name_match else 5
    fans_limit = 40 if strict_name_match else 10

    def _append_tpdb(p: dict) -> None:
        posters = p.get("posters") or []
        img = posters[0] if posters and isinstance(posters[0], str) else (posters[0].get("url") if posters else None)
        raw_g = _tpdb_raw_gender_from_search_hit(p)
        results.append({
            "source": "TPDB",
            "id":     str(p.get("id", "")),
            "slug":   p.get("slug") or p.get("_id") or str(p.get("id", "")),
            "name":   p["name"],
            "image":  img,
            "gender": raw_g,
            "gender_bucket": normalize_performer_gender_to_bucket(raw_g),
        })

    # TPDB
    try:
        resp = requests.get(TPDB_PERFORMER_SEARCH, params={"q": name},
                            headers=_tpdb_headers(), timeout=15)
        if resp.status_code == 200:
            for p in (resp.json().get("data") or [])[:tpdb_take]:
                _append_tpdb(p)
    except Exception:
        pass
    # StashDB
    if settings.get("api_key_stashdb"):
        try:
            gql = f"""
            query($t: String!) {{
              searchPerformer(term: $t, limit: {stash_limit}) {{
                id name gender images {{ url }}
              }}
            }}
            """
            resp = requests.post(
                "https://stashdb.org/graphql",
                json={"query": gql, "variables": {"t": name}},
                headers={"ApiKey": settings["api_key_stashdb"],
                          "Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                body = resp.json()
                data = body.get("data") or {}
                # Do not skip Stash hits when TPDB already returned the same display name —
                # favourites needs one row per source so TPDB / Stash / Fans filters work.
                for p in (data.get("searchPerformer") or []):
                    imgs = p.get("images") or []
                    img = imgs[0].get("url") if imgs and isinstance(imgs[0], dict) else (imgs[0] if imgs else None)
                    raw_g = p.get("gender")
                    raw_g = str(raw_g) if raw_g is not None else None
                    results.append({
                        "source": "StashDB",
                        "id":     str(p["id"]),
                        "slug":   str(p["id"]),
                        "name":   p["name"],
                        "image":  img,
                        "gender": raw_g,
                        "gender_bucket": normalize_performer_gender_to_bucket(raw_g),
                    })
        except Exception:
            pass
    # FansDB (stash-box: same ApiKey auth as scene search; variable name `term` matches API)
    if settings.get("api_key_fansdb"):

        def _fansdb_append_from_data(data: dict) -> None:
            for p in (data.get("searchPerformer") or []):
                imgs = p.get("images") or []
                img = imgs[0].get("url") if imgs and isinstance(imgs[0], dict) else (imgs[0] if imgs else None)
                raw_g = p.get("gender")
                raw_g = str(raw_g) if raw_g is not None else None
                results.append({
                    "source": "FansDB",
                    "id":     str(p["id"]),
                    "slug":   str(p["id"]),
                    "name":   p["name"],
                    "image":  img,
                    "gender": raw_g,
                    "gender_bucket": normalize_performer_gender_to_bucket(raw_g),
                })

        try:
            gql_img = f"""
            query($term: String!) {{
              searchPerformer(term: $term, limit: {fans_limit}) {{
                id name gender images {{ url }}
              }}
            }}
            """
            data = _fansdb_gql(gql_img, {"term": name})
            _fansdb_append_from_data(data)
        except Exception:
            try:
                gql_min = f"""
                query($term: String!) {{
                  searchPerformer(term: $term, limit: {fans_limit}) {{
                    id name gender
                  }}
                }}
                """
                data = _fansdb_gql(gql_min, {"term": name})
                _fansdb_append_from_data(data)
            except Exception:
                pass
    if strict_name_match:
        want = normalise(name)
        results = [r for r in results if normalise(r.get("name") or "") == want]
    return _apply_gender_allowlist(results, gender_allowlist)


def _tpdb_site_image_url(site: dict) -> str | None:
    """Resolve a usable image URL from a TPDB /sites item (logo/poster may be str or nested)."""
    for key in ("logo", "poster", "image"):
        v = site.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            u = v.get("url") or v.get("full") or v.get("large")
            if isinstance(u, str) and u.strip():
                return u.strip()
    for key in ("logos", "posters", "images"):
        arr = site.get(key)
        if isinstance(arr, list) and arr and isinstance(arr[0], dict):
            got = best_image_url(arr)
            if got:
                return got
    return None


def _tpdb_scalar_media_url(val) -> str | None:
    """TPDB field that is either a URL string or {url, full, large}."""
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        u = val.get("url") or val.get("full") or val.get("large")
        if isinstance(u, str) and u.strip():
            return u.strip()
    return None


def search_studios(name: str) -> list[dict]:
    results = []
    settings = db.get_settings()
    try:
        resp = requests.get(TPDB_STUDIO_SEARCH, params={"q": name},
                            headers=_tpdb_headers(), timeout=15)
        if resp.status_code == 200:
            for s in (resp.json().get("data") or [])[:10]:
                results.append({
                    "source": "TPDB",
                    "id":     str(s["id"]),
                    "slug":   s.get("_id") or s.get("slug") or str(s["id"]),
                    "name":   s.get("name") or s.get("title", ""),
                    "image":  _tpdb_site_image_url(s),
                })
    except Exception:
        pass
    if settings.get("api_key_stashdb"):
        try:
            for s in stashdb_search_studio(name, settings["api_key_stashdb"])[:10]:
                results.append({
                    "source": "StashDB",
                    "id":     str(s["id"]),
                    "slug":   str(s["id"]),
                    "name":   s.get("name") or "",
                    "image":  best_image_url(s.get("images") or []),
                })
        except Exception:
            pass
    if settings.get("api_key_fansdb"):
        try:
            gql = """
            query($term: String!) {
              searchStudio(term: $term, limit: 10) {
                id name images { url width height }
              }
            }
            """
            data = _fansdb_gql(gql, {"term": name})
            for s in (data.get("searchStudio") or []):
                imgs = s.get("images") or []
                img = best_image_url(imgs) if imgs else None
                if not img and imgs and isinstance(imgs[0], dict):
                    img = (imgs[0].get("url") or "").strip() or None
                results.append({
                    "source": "FansDB",
                    "id":     str(s["id"]),
                    "slug":   str(s["id"]),
                    "name":   s.get("name") or "",
                    "image":  img,
                })
        except Exception:
            pass
    return results


def _pick_best_name_match(results: list[dict], folder_name: str) -> dict | None:
    """Normalised name equality only (no fuzzy or single-result fallback)."""
    if not results:
        return None
    want = normalise(folder_name)
    for r in results:
        if normalise(r.get("name") or "") == want:
            return r
    return None


def _favourites_collect_folder_rows() -> list[dict]:
    """Subfolders under performer directory roots and under series_dir (studios)."""
    rows: list[dict] = []
    s = db.get_settings()
    for entry in db.get_directories("performer"):
        base = Path(entry["path"])
        label = entry.get("label") or base.name
        if not base.is_dir():
            continue
        for d in sorted(base.iterdir(), key=lambda p: p.name.lower()):
            if d.is_dir() and not d.name.startswith("."):
                gf = entry.get("gender_filters")
                if not isinstance(gf, list):
                    gf = []
                rows.append({
                    "kind": "performer",
                    "folder_name": d.name,
                    "path": str(d.resolve()),
                    "root_label": label,
                    "gender_filters": gf,
                })
    series_dir = (s.get("series_dir") or "").strip()
    if series_dir and Path(series_dir).is_dir():
        base = Path(series_dir)
        for d in sorted(base.iterdir(), key=lambda p: p.name.lower()):
            if d.is_dir() and not d.name.startswith("."):
                rows.append({
                    "kind": "studio",
                    "folder_name": d.name,
                    "path": str(d.resolve()),
                    "root_label": "Series",
                })
    return rows


def _normalize_sort_birth_date(raw: str | None) -> str | None:
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    m = re.match(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?", s)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2) or "01", m.group(3) or "01"
    try:
        mo_i = max(1, min(12, int(mo)))
        d_i = max(1, min(31, int(d)))
        return f"{y}-{mo_i:02d}-{d_i:02d}"
    except ValueError:
        return f"{y}-{mo}-{d}"


def _tpdb_performer_sort_birth_date(tpdb_id: str) -> str | None:
    try:
        resp = requests.get(
            TPDB_PERFORMER_DETAIL.format(id=tpdb_id),
            headers=_tpdb_headers(),
            timeout=12,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        extras = data.get("extras") or {}
        b = extras.get("birthday") or data.get("birth_date")
        return _normalize_sort_birth_date(b)
    except Exception:
        return None


def _fansdb_performer_sort_birth_date(fans_id: str) -> str | None:
    try:
        data = _fansdb_gql(
            "query($id: ID!) { findPerformer(id: $id) { birth_date } }",
            {"id": fans_id},
        )
        p = data.get("findPerformer") or {}
        return _normalize_sort_birth_date(p.get("birth_date"))
    except Exception:
        return None


def _stashdb_performer_sort_birth_date(sid: str) -> str | None:
    settings = db.get_settings()
    if not settings.get("api_key_stashdb"):
        return None
    try:
        gql = """
        query($id: ID!) {
          findPerformer(id: $id) {
            birth_date
          }
        }
        """
        resp = requests.post(
            "https://stashdb.org/graphql",
            json={"query": gql, "variables": {"id": sid}},
            headers={"ApiKey": settings["api_key_stashdb"],
                     "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        p = data.get("findPerformer") or {}
        return _normalize_sort_birth_date(p.get("birth_date"))
    except Exception:
        return None


def _favourite_performer_sort_birth_date(
    mt_id: str | None, ms_id: str | None, mf_id: str | None
) -> str | None:
    if mt_id:
        d = _tpdb_performer_sort_birth_date(mt_id)
        if d:
            return d
    if mf_id:
        d = _fansdb_performer_sort_birth_date(mf_id)
        if d:
            return d
    if ms_id:
        d = _stashdb_performer_sort_birth_date(ms_id)
        if d:
            return d
    return None


def _tpdb_studio_sort_birth_date(site_id: str) -> str | None:
    try:
        resp = requests.get(
            TPDB_STUDIO_DETAIL.format(id=site_id),
            headers=_tpdb_headers(),
            timeout=12,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        extras = data.get("extras") or {}
        y = extras.get("career_start_year") or extras.get("founded")
        if y is None or y == "":
            return None
        if isinstance(y, (int, float)):
            return f"{int(y)}-01-01"
        s = str(y).strip()
        m = re.match(r"^(\d{4})", s)
        if m:
            return f"{m.group(1)}-01-01"
        return _normalize_sort_birth_date(s)
    except Exception:
        return None


def _favourite_studio_sort_birth_date(mt_id: str | None) -> str | None:
    if mt_id:
        return _tpdb_studio_sort_birth_date(mt_id)
    return None


_FAVOURITE_LOCAL_POSTER_NAMES = ("poster.jpg", "folder.jpg")


def _favourite_find_local_poster_file(folder_path: str) -> Path | None:
    """Root-of-folder poster.jpg or folder.jpg (case-insensitive)."""
    raw = (folder_path or "").strip()
    if not raw:
        return None
    try:
        root = Path(raw).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    if not root.is_dir():
        return None
    for name in _FAVOURITE_LOCAL_POSTER_NAMES:
        p = root / name
        if p.is_file():
            return p.resolve()
    found: dict[str, Path] = {}
    try:
        for child in root.iterdir():
            if not child.is_file():
                continue
            key = child.name.lower()
            if key in ("poster.jpg", "folder.jpg"):
                found[key] = child
    except OSError:
        return None
    for name in _FAVOURITE_LOCAL_POSTER_NAMES:
        p = found.get(name.lower())
        if p:
            try:
                return p.resolve()
            except OSError:
                continue
    return None


# Studio show folder: Kodi/Jellyfin-style art. Order = preference for favourites image.
_FAVOURITE_STUDIO_LOCAL_ART_NAMES = (
    "logo.png",
    "clearlogo.png",
    "poster.jpg",
    "folder.jpg",
)


def _favourite_find_local_studio_art(folder_path: str) -> Path | None:
    """First match in show root: logo.png, clearlogo.png, poster.jpg, folder.jpg (case-insensitive)."""
    raw = (folder_path or "").strip()
    if not raw:
        return None
    try:
        root = Path(raw).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    if not root.is_dir():
        return None
    allowed_lower = {n.lower() for n in _FAVOURITE_STUDIO_LOCAL_ART_NAMES}
    for name in _FAVOURITE_STUDIO_LOCAL_ART_NAMES:
        p = root / name
        if p.is_file():
            return p.resolve()
    found: dict[str, Path] = {}
    try:
        for child in root.iterdir():
            if not child.is_file():
                continue
            key = child.name.lower()
            if key in allowed_lower:
                found[key] = child
    except OSError:
        return None
    for name in _FAVOURITE_STUDIO_LOCAL_ART_NAMES:
        p = found.get(name.lower())
        if p:
            try:
                return p.resolve()
            except OSError:
                continue
    return None


def favourites_refresh_entity_row(
    row_id: int,
    *,
    scrape_aliases: bool = True,
    only_missing: bool = False,
) -> dict:
    """Resolve library folder → external IDs for cross-reference across databases.

    Each configured source (TPDB, StashDB, FansDB) is queried independently; when a
    performer/studio name matches (or a single unambiguous search hit), that source’s
    ID and display name are stored so the same folder links all three where possible.

    If only_missing is True, existing non-empty match_* ids are kept; only empty slots
    are filled. Image and aliases are not overwritten when already present unless a new
    match supplies them for an empty slot.

    When matches_locked is set, this function is skipped entirely (returns skipped: True).
    Manual link updates from the overlay search modal use api_favourites_match instead and
    are not blocked by the lock.
    """
    row = db.favourite_get(row_id)
    if not row:
        return {"error": "Not found"}
    if int(row.get("matches_locked") or 0):
        return {"ok": True, "id": row_id, "skipped": True}
    name = row["folder_name"]
    kind = row["kind"]
    if only_missing:
        scrape_aliases = False

    img: str | None = row.get("image_url")
    had_img = bool(str(img or "").strip())
    aliases: list[str] = []

    mt_id = mt_n = ms_id = ms_n = mf_id = mf_n = None

    if kind == "performer":
        g_allow = gender_allowlist_from_json(row.get("gender_filters_json"))
        res_tp = search_performers(
            name, gender_allowlist=g_allow, strict_name_match=True
        )
        res_st = [r for r in res_tp if r.get("source") == "StashDB"]
        res_tpdb = [r for r in res_tp if r.get("source") == "TPDB"]
        res_fan = [r for r in res_tp if r.get("source") == "FansDB"]
        p_tp = _pick_best_name_match(res_tpdb, name)
        p_st = _pick_best_name_match(res_st, name)
        p_fn = _pick_best_name_match(res_fan, name)
        if only_missing:
            mt_id = (row.get("match_tpdb_id") or "").strip() or None
            mt_n = (row.get("match_tpdb_name") or "") or ""
            ms_id = (row.get("match_stashdb_id") or "").strip() or None
            ms_n = (row.get("match_stashdb_name") or "") or ""
            mf_id = (row.get("match_fansdb_id") or "").strip() or None
            mf_n = (row.get("match_fansdb_name") or "") or ""
            # Drop stored links that violate the directory gender allowlist (search_performers
            # already filters; a stale match from before filters or wrong gender must be cleared).
            if g_allow is not None:

                def _id_in_results(res_list: list[dict], sid: str | None) -> bool:
                    if not sid or not str(sid).strip():
                        return False
                    s = str(sid).strip()
                    return any(str(r.get("id") or "") == s for r in res_list)

                if mt_id and not _id_in_results(res_tpdb, mt_id):
                    mt_id = None
                    mt_n = ""
                if ms_id and not _id_in_results(res_st, ms_id):
                    ms_id = None
                    ms_n = ""
                if mf_id and not _id_in_results(res_fan, mf_id):
                    mf_id = None
                    mf_n = ""
            if not mt_id and p_tp:
                mt_id = str(p_tp.get("id") or "")
                mt_n = p_tp.get("name") or ""
                if not had_img and p_tp.get("image"):
                    img = p_tp.get("image")
                    had_img = True
            if not ms_id and p_st:
                ms_id = str(p_st.get("id") or "")
                ms_n = p_st.get("name") or ""
                if not had_img and p_st.get("image"):
                    img = p_st.get("image")
                    had_img = True
            if not mf_id and p_fn:
                mf_id = str(p_fn.get("id") or "")
                mf_n = p_fn.get("name") or ""
                if not had_img and p_fn.get("image"):
                    img = p_fn.get("image")
                    had_img = True
        else:
            if p_tp:
                mt_id, mt_n = str(p_tp.get("id") or ""), p_tp.get("name") or ""
            if p_st:
                ms_id, ms_n = str(p_st.get("id") or ""), p_st.get("name") or ""
            if p_fn:
                mf_id, mf_n = str(p_fn.get("id") or ""), p_fn.get("name") or ""
            img = (
                (p_tp.get("image") if p_tp else None)
                or (p_st.get("image") if p_st else None)
                or (p_fn.get("image") if p_fn else None)
                or img
            )
            if scrape_aliases:
                aliases = _fetch_performer_aliases(name, quiet=True)
        if g_allow is not None:
            mt_id, mt_n, ms_id, ms_n, mf_id, mf_n = _enforce_favourite_performer_slots_by_detail_gender(
                g_allow, mt_id, mt_n, ms_id, ms_n, mf_id, mf_n
            )
    else:
        res_s = search_studios(name)
        res_tpdb = [r for r in res_s if r.get("source") == "TPDB"]
        res_st = [r for r in res_s if r.get("source") == "StashDB"]
        res_fan = [r for r in res_s if r.get("source") == "FansDB"]
        p_tp = _pick_best_name_match(res_tpdb, name)
        p_st = _pick_best_name_match(res_st, name)
        p_fn = _pick_best_name_match(res_fan, name)
        if only_missing:
            mt_id = (row.get("match_tpdb_id") or "").strip() or None
            mt_n = (row.get("match_tpdb_name") or "") or ""
            ms_id = (row.get("match_stashdb_id") or "").strip() or None
            ms_n = (row.get("match_stashdb_name") or "") or ""
            mf_id = (row.get("match_fansdb_id") or "").strip() or None
            mf_n = (row.get("match_fansdb_name") or "") or ""
            if not mt_id and p_tp:
                mt_id = str(p_tp.get("id") or "")
                mt_n = p_tp.get("name") or ""
                if not had_img and p_tp.get("image"):
                    img = p_tp.get("image")
                    had_img = True
            if not ms_id and p_st:
                ms_id = str(p_st.get("id") or "")
                ms_n = p_st.get("name") or ""
                if not had_img and p_st.get("image"):
                    img = p_st.get("image")
                    had_img = True
            if not mf_id and p_fn:
                mf_id = str(p_fn.get("id") or "")
                mf_n = p_fn.get("name") or ""
                if not had_img and p_fn.get("image"):
                    img = p_fn.get("image")
                    had_img = True
        else:
            if p_tp:
                mt_id, mt_n = str(p_tp.get("id") or ""), p_tp.get("name") or ""
            if p_st:
                ms_id, ms_n = str(p_st.get("id") or ""), p_st.get("name") or ""
            if p_fn:
                mf_id, mf_n = str(p_fn.get("id") or ""), p_fn.get("name") or ""
            img = (
                (p_tp.get("image") if p_tp else None)
                or (p_st.get("image") if p_st else None)
                or (p_fn.get("image") if p_fn else None)
                or img
            )

    now = datetime.now(timezone.utc).isoformat()
    if kind == "performer" and scrape_aliases:
        aliases_json = json.dumps(aliases)
    elif kind == "performer":
        aliases_json = row.get("aliases_json")
    else:
        aliases_json = None

    if kind == "performer":
        sort_bd = _favourite_performer_sort_birth_date(mt_id, ms_id, mf_id)
    else:
        sort_bd = _favourite_studio_sort_birth_date(mt_id)

    if kind == "performer" and _favourite_find_local_poster_file(row.get("path") or ""):
        img = f"/api/favourites/folder-poster?row_id={row_id}"
    elif kind == "studio" and _favourite_find_local_studio_art(row.get("path") or ""):
        img = f"/api/favourites/folder-logo?row_id={row_id}"

    db.favourite_overwrite_matches(
        row_id,
        image_url=img,
        aliases_json=aliases_json,
        match_tpdb_id=mt_id,
        match_tpdb_name=mt_n,
        match_stashdb_id=ms_id,
        match_stashdb_name=ms_n,
        match_fansdb_id=mf_id,
        match_fansdb_name=mf_n,
        sort_birth_date=sort_bd,
    )
    with db.get_conn() as c:
        c.execute(
            "UPDATE favourite_entities SET scanned_at = ? WHERE id = ?",
            (now, row_id),
        )
        c.commit()
    return {"ok": True, "id": row_id}


def favourites_scan_index(
    *,
    prune_missing: bool = False,
    folders: list[dict] | None = None,
    progress_init: bool = True,
    only_missing: bool = True,
) -> dict:
    """Scan configured library folders into favourite_entities; optionally drop removed folders.

    only_missing: when True (default), refresh each row without overwriting existing DB matches.
    """
    if folders is None:
        folders = _favourites_collect_folder_rows()
    valid_paths = {f["path"] for f in folders}
    seen = 0
    done = 0
    if progress_init:
        _favourites_progress_start("scan", len(folders))
    try:
        for f in folders:
            nm = f.get("folder_name") or ""
            _favourites_progress_update(nm, done)
            gf_json = None
            if f.get("kind") == "performer":
                lst = f.get("gender_filters")
                if isinstance(lst, list):
                    gf_json = json.dumps(lst)
            rid = db.favourite_upsert_folder(
                f["kind"],
                f["folder_name"],
                f["path"],
                f["root_label"],
                gender_filters_json=gf_json,
            )
            seen += 1
            if rid:
                favourites_refresh_entity_row(
                    rid,
                    scrape_aliases=not only_missing,
                    only_missing=only_missing,
                )
            done += 1
            _favourites_progress_update(nm, done)
        removed = 0
        if prune_missing:
            _favourites_progress_update("prune", done)
            removed = db.favourite_delete_missing_paths(valid_paths)
        _favourites_progress_update("paths", done)
        db.favourite_refresh_all_path_existence()
        emit(f"FAVOURITES index: {seen} folders scanned, {removed} stale rows removed")
        return {"scanned": seen, "removed_stale": removed}
    finally:
        _favourites_progress_finish()


def run_favourites_scheduled_scan() -> None:
    s = db.get_settings()
    if s.get("favourites_scan_enabled", "false").lower() != "true":
        return
    try:
        favourites_scan_index(prune_missing=False, only_missing=True)
    except Exception as e:
        emit(f"FAVOURITES scheduled scan error: {e}")


def fetch_performer_detail(source: str, pid: str) -> dict | None:
    if source == "TPDB":
        try:
            resp = requests.get(TPDB_PERFORMER_DETAIL.format(id=pid),
                                headers=_tpdb_headers(), timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data")
                return data
        except Exception:
            pass
    elif source == "FansDB":
        try:
            data = _fansdb_gql(FANSDB_PERFORMER_DETAIL_GQL, {"id": pid})
            p = data.get("findPerformer")
            if p:
                return {
                    "name":    p.get("name"),
                    "bio":     p.get("disambiguation") or "",
                    "aliases": p.get("aliases", []),
                    "posters": p.get("images", []),
                    "extras":  {
                        "birthday":    p.get("birth_date"),
                        "ethnicity":   p.get("ethnicity"),
                        "nationality": p.get("country"),
                    }
                }
        except Exception:
            pass
    return None


def fetch_studio_detail(source: str, sid: str) -> dict | None:
    if source == "TPDB":
        try:
            resp = requests.get(TPDB_STUDIO_DETAIL.format(id=sid),
                                headers=_tpdb_headers(), timeout=15)
            if resp.status_code == 200:
                return resp.json().get("data")
        except Exception:
            pass
    return None


def _favourite_tpdb_performer_image(mt_id: str | None) -> str | None:
    if not mt_id or not str(mt_id).strip():
        return None
    d = fetch_performer_detail("TPDB", str(mt_id).strip())
    if not d:
        return None
    posters = d.get("posters") or []
    if not posters:
        return None
    p0 = posters[0]
    if isinstance(p0, str):
        return p0.strip() or None
    if isinstance(p0, dict):
        return (p0.get("url") or "").strip() or None
    return None


def _favourite_stashdb_performer_image(ms_id: str | None) -> str | None:
    if not ms_id or not str(ms_id).strip():
        return None
    key = (db.get_settings().get("api_key_stashdb") or "").strip()
    if not key:
        return None
    try:
        gql = """
        query($id: ID!) {
          findPerformer(id: $id) {
            images { url width height }
          }
        }
        """
        resp = requests.post(
            "https://stashdb.org/graphql",
            json={"query": gql, "variables": {"id": str(ms_id).strip()}},
            headers={"ApiKey": key, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        p = data.get("findPerformer") or {}
        return best_image_url(p.get("images") or [])
    except Exception:
        return None


def _favourite_fansdb_performer_image(mf_id: str | None) -> str | None:
    if not mf_id or not str(mf_id).strip():
        return None
    d = fetch_performer_detail("FansDB", str(mf_id).strip())
    if not d:
        return None
    posters = d.get("posters") or []
    if not posters:
        return None
    p0 = posters[0]
    if isinstance(p0, str):
        return p0.strip() or None
    if isinstance(p0, dict):
        return (p0.get("url") or "").strip() or None
    return None


def _favourite_tpdb_studio_image(mt_id: str | None) -> str | None:
    if not mt_id or not str(mt_id).strip():
        return None
    d = fetch_studio_detail("TPDB", str(mt_id).strip())
    if not d:
        return None
    return _tpdb_site_image_url(d)


def _favourite_stashdb_studio_image(ms_id: str | None) -> str | None:
    if not ms_id or not str(ms_id).strip():
        return None
    key = (db.get_settings().get("api_key_stashdb") or "").strip()
    if not key:
        return None
    try:
        gql = """
        query($id: ID!) {
          findStudio(id: $id) {
            images { url width height }
          }
        }
        """
        resp = requests.post(
            "https://stashdb.org/graphql",
            json={"query": gql, "variables": {"id": str(ms_id).strip()}},
            headers={"ApiKey": key, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data") or {}
        s = data.get("findStudio") or {}
        return best_image_url(s.get("images") or [])
    except Exception:
        return None


def _favourite_fansdb_studio_image(mf_id: str | None) -> str | None:
    if not mf_id or not str(mf_id).strip():
        return None
    try:
        data = _fansdb_gql(
            """
            query($id: ID!) {
              findStudio(id: $id) {
                images { url width height }
              }
            }
            """,
            {"id": str(mf_id).strip()},
        )
        s = data.get("findStudio") or {}
        return best_image_url(s.get("images") or [])
    except Exception:
        return None


def favourites_refresh_entity_images(row_id: int) -> dict:
    """Set image_url from local art first, then TPDB → StashDB → FansDB for stored match IDs only."""
    row = db.favourite_get(row_id)
    if not row:
        return {"error": "Not found"}
    if int(row.get("matches_locked") or 0):
        return {"ok": True, "id": row_id, "skipped": True}
    kind = row["kind"]
    path = row.get("path") or ""
    mt = (row.get("match_tpdb_id") or "").strip() or None
    ms = (row.get("match_stashdb_id") or "").strip() or None
    mf = (row.get("match_fansdb_id") or "").strip() or None
    prev = row.get("image_url")
    if kind == "performer":
        if _favourite_find_local_poster_file(path):
            img: str | None = f"/api/favourites/folder-poster?row_id={row_id}"
        else:
            img = (
                _favourite_tpdb_performer_image(mt)
                or _favourite_stashdb_performer_image(ms)
                or _favourite_fansdb_performer_image(mf)
                or prev
            )
    else:
        if _favourite_find_local_studio_art(path):
            img = f"/api/favourites/folder-logo?row_id={row_id}"
        else:
            img = (
                _favourite_tpdb_studio_image(mt)
                or _favourite_stashdb_studio_image(ms)
                or _favourite_fansdb_studio_image(mf)
                or prev
            )
    db.favourite_update_matches(row_id, image_url=img)
    return {"ok": True, "id": row_id}


def build_performer_tvshow_nfo(data: dict) -> str:
    extras = data.get("extras") or {}
    plot_parts = []
    if data.get("bio"):
        plot_parts.append(data["bio"].strip())
    if data.get("aliases"):
        plot_parts.append("AKA: " + ", ".join(data["aliases"]))
    info = []
    for key, label in [
        ("birthday", "Birthday"), ("birthplace", "Birthplace"),
        ("career_start_year", "Active Since"), ("ethnicity", "Ethnicity"),
        ("nationality", "Nationality"), ("hair_colour", "Hair"),
        ("height", "Height"), ("weight", "Weight"),
        ("measurements", "Measurements"), ("tattoos", "Tattoos"),
        ("piercings", "Piercings"),
    ]:
        if extras.get(key):
            info.append(f"{label}: {extras[key]}")
    if info:
        plot_parts.append("\n".join(info))

    root = ET.Element("tvshow")
    ET.SubElement(root, "title").text  = data.get("name", "Unknown")
    ET.SubElement(root, "plot").text   = "\n\n".join(plot_parts)
    ET.SubElement(root, "mpaa").text   = "Adult"
    ET.SubElement(root, "genre").text  = "Adult"
    ET.SubElement(root, "tag").text    = "Porn"

    posters = data.get("posters") or []
    poster_url = None
    if posters:
        poster_url = posters[0].get("url") if isinstance(posters[0], dict) else posters[0]

    actor = ET.SubElement(root, "actor")
    ET.SubElement(actor, "name").text  = data.get("name", "Unknown")
    if poster_url:
        ET.SubElement(actor, "thumb").text = poster_url

    year = (extras.get("career_start_year") or
            (extras.get("birthday") or "")[:4] or "2000")
    ET.SubElement(root, "premiered").text = f"{year}-01-01"
    ET.SubElement(root, "year").text      = str(year)

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue().decode("utf-8")


def build_studio_tvshow_nfo(data: dict) -> str:
    root = ET.Element("tvshow")
    ET.SubElement(root, "title").text  = data.get("name") or data.get("title", "Unknown")
    ET.SubElement(root, "plot").text   = data.get("description") or data.get("bio") or ""
    ET.SubElement(root, "mpaa").text   = "Adult"
    ET.SubElement(root, "genre").text  = "Adult"
    ET.SubElement(root, "tag").text    = "Porn"
    ET.SubElement(root, "studio").text = data.get("name") or data.get("title", "")

    year = (data.get("year") or
            (data.get("founded") or "")[:4] or "2000")
    ET.SubElement(root, "premiered").text = f"{year}-01-01"
    ET.SubElement(root, "year").text      = str(year)

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue().decode("utf-8")


def create_tvshow_folder(
    name: str,
    dest_dir: Path,
    nfo_content: str,
    poster_url: str | None = None,
    *,
    logo_url: str | None = None,
) -> Path:
    folder = dest_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    nfo_path = folder / "tvshow.nfo"
    nfo_path.write_text(nfo_content, encoding="utf-8")
    if logo_url:
        download_image(logo_url, folder / "logo.png")
    if poster_url:
        download_image(poster_url, folder / "poster.jpg")
    return folder


# ---------------------------------------------------------------------------
# Prowlarr integration
# ---------------------------------------------------------------------------

def _prowlarr_headers() -> dict:
    s = db.get_settings()
    return {"X-Api-Key": s.get("prowlarr_api_key", ""), "Content-Type": "application/json"}


def _prowlarr_url() -> str:
    s = db.get_settings()
    return s.get("prowlarr_url", "").rstrip("/")


def _fetch_and_cache_indexers() -> list[dict]:
    """Fetch indexers from Prowlarr API and cache in DB."""
    base = _prowlarr_url()
    if not base:
        return []
    try:
        resp = requests.get(f"{base}/api/v1/indexer",
                            headers=_prowlarr_headers(), timeout=10)
        resp.raise_for_status()
        indexers = [{"id": i["id"], "name": i["name"],
                     "protocol": i.get("protocol", "torrent")}
                    for i in resp.json()]
        db.cache_indexers(indexers)
        return indexers
    except Exception:
        return []


def _get_indexers() -> list[dict]:
    """Get indexers from cache, fetching from Prowlarr if empty."""
    cached = db.get_cached_indexers()
    if cached:
        return cached
    return _fetch_and_cache_indexers()


def _parse_newznab_xml(xml_text: str, indexer_name: str, protocol: str) -> list[dict]:
    """Parse Newznab RSS XML into result dicts."""
    import xml.etree.ElementTree as ET
    out = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"newznab": "http://www.newznab.com/DTD/2010/feeds/attributes/",
              "torznab":  "http://torznab.com/schemas/2015/feed"}
        for item in root.findall(".//item"):
            title     = (item.findtext("title") or "").strip()
            guid      = item.findtext("guid") or ""
            size      = 0
            link      = item.findtext("link") or ""
            seeders   = None
            pub_date  = item.findtext("pubDate") or ""

            enc = item.find("enclosure")
            if enc is not None:
                size = int(enc.get("length") or 0)
                if not link:
                    link = enc.get("url") or ""

            # Get indexer ID from prowlarrindexer element
            indexer_elem = item.find("prowlarrindexer")
            parsed_indexer_id = None
            if indexer_elem is not None:
                try:
                    parsed_indexer_id = int(indexer_elem.get("id", 0))
                except Exception:
                    pass

            magnet_url = ""
            for attr in item.findall("newznab:attr", ns) + item.findall("torznab:attr", ns):
                name = attr.get("name", "")
                val  = attr.get("value", "")
                if name == "size" and not size:
                    size = int(val or 0)
                elif name == "seeders":
                    try: seeders = int(val)
                    except: pass
                elif name == "magneturl" and val:
                    magnet_url = val

            if not title:
                continue

            # Calculate age in hours from pubDate
            age_hours = None
            if pub_date:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub_date)
                    age_hours = (datetime.now(dt.tzinfo) - dt).total_seconds() / 3600
                except Exception:
                    pass

            # Prefer explicit magneturl attr, fall back to link if it's a magnet
            if not magnet_url and link.startswith("magnet:"):
                magnet_url = link

            out.append({
                "guid":         guid,
                "title":        title,
                "indexer":      indexer_name,
                "size_mb":      round(size / 1024 / 1024, 0),
                "seeders":      seeders,
                "age":          age_hours,
                "download_url": link if not link.startswith("magnet:") else "",
                "magnet":       magnet_url,
                "protocol":     protocol,
                "type":         "torrent" if protocol == "torrent" else "nzb",
                "indexer_id":   parsed_indexer_id,
            })
    except Exception:
        pass
    return out


def _search_indexer(base: str, api_key: str, indexer: dict, query: str) -> list[dict]:
    """Search a single indexer via Prowlarr's Newznab proxy."""
    iid      = indexer["id"]
    name     = indexer["name"]
    protocol = indexer.get("protocol", "torrent")
    try:
        resp = requests.get(
            f"{base}/{iid}/api",
            params={"t": "search", "q": query, "apikey": api_key,
                    "cat": "6000,6010,6020,6030,6040,6050,6060,6070,6080,6090"},
            timeout=20,
        )
        if resp.status_code == 200:
            return _parse_newznab_xml(resp.text, name, protocol)
        elif resp.status_code in (400, 404, 500):
            # Indexer may be broken - clear cache so it gets re-fetched next time
            db.clear_indexer_cache()
    except Exception:
        pass
    return []


def prowlarr_search(query: str) -> list[dict]:
    """Search all Prowlarr indexers via their Newznab proxy endpoints (parallel)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    base    = _prowlarr_url()
    api_key = db.get_settings().get("prowlarr_api_key", "")
    if not base or not api_key:
        return []

    indexers = _get_indexers()
    if not indexers:
        return []

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(indexers), 8)) as pool:
        futures = {pool.submit(_search_indexer, base, api_key, idx, query): idx for idx in indexers}
        for future in as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception:
                pass

    nzbs     = sorted([r for r in all_results if r["type"] == "nzb"],
                      key=lambda x: x.get("age") or 0)
    torrents = sorted([r for r in all_results if r["type"] == "torrent"],
                      key=lambda x: x.get("seeders") or 0, reverse=True)
    return nzbs[:20] + torrents[:20]


def prowlarr_grab(guid: str, indexer_id: int, is_torrent: bool) -> dict:
    """Send a result to the configured download client via Prowlarr."""
    s    = db.get_settings()
    base = _prowlarr_url()
    if not base:
        return {"error": "Prowlarr URL not configured"}
    client_name = s.get("prowlarr_torrent_client" if is_torrent else "prowlarr_nzb_client", "")
    try:
        # Resolve download client ID from name if configured
        download_client_id = None
        if client_name:
            try:
                cr = requests.get(f"{base}/api/v1/downloadclient",
                                   headers=_prowlarr_headers(), timeout=10)
                cr.raise_for_status()
                for c in cr.json():
                    if c.get("name") == client_name:
                        download_client_id = c["id"]
                        break
            except Exception:
                pass

        # Prowlarr grab endpoint
        payload = {"guid": guid, "indexerId": indexer_id}
        if download_client_id:
            payload["downloadClientId"] = download_client_id

        resp = requests.post(
            f"{base}/api/v1/search",
            json=payload,
            headers=_prowlarr_headers(),
            timeout=15,
        )
        if resp.status_code in (200, 201, 202, 204):
            return {"ok": True}
        # Return Prowlarr's error message for debugging
        try:
            err_detail = resp.json()
        except Exception:
            err_detail = resp.text[:200]
        return {"error": f"Prowlarr returned {resp.status_code}: {err_detail}"}
    except Exception as e:
        return {"error": str(e)}


def prowlarr_get_clients() -> list[dict]:
    """Get configured download clients from Prowlarr."""
    base = _prowlarr_url()
    if not base:
        return []
    try:
        resp = requests.get(f"{base}/api/v1/downloadclient",
                            headers=_prowlarr_headers(), timeout=10)
        resp.raise_for_status()
        return [{"id": c["id"], "name": c["name"],
                 "type": "torrent" if "torrent" in c.get("implementation", "").lower() else "nzb"}
                for c in resp.json()]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Download clients — combined queue/history for UI (matches Settings clients)
# ---------------------------------------------------------------------------


def _dl_resolve_nzb_settings(s: dict) -> dict | None:
    nzb_client = s.get("dl_nzb_client", "").lower().strip()
    nzb_host = s.get("dl_nzb_host", "").strip()
    nzb_port = s.get("dl_nzb_port", "").strip()
    nzb_user = s.get("dl_nzb_user", "").strip()
    nzb_pass = s.get("dl_nzb_pass", "")
    nzb_apikey = s.get("dl_nzb_api_key", "").strip()
    if not nzb_client and s.get("nzbget_url", "").strip():
        nzb_client = "nzbget"
        legacy_url = s.get("nzbget_url", "").strip().rstrip("/")
        m = re.match(r"https?://([^:/]+)(?::(\d+))?", legacy_url)
        if m:
            nzb_host = m.group(1)
            nzb_port = m.group(2) or "6789"
        nzb_user = s.get("nzbget_user", "").strip()
        nzb_pass = s.get("nzbget_pass", "")
    if not nzb_client or not nzb_host:
        return None
    if nzb_client not in ("nzbget", "sabnzbd"):
        return None
    return {
        "client": nzb_client,
        "host": nzb_host,
        "port": nzb_port,
        "user": nzb_user,
        "pass": nzb_pass,
        "apikey": nzb_apikey,
    }


def _dl_resolve_torrent_settings(s: dict) -> dict | None:
    torrent_client = s.get("dl_torrent_client", "").lower().strip()
    torrent_host = s.get("dl_torrent_host", "").strip()
    if not torrent_client or not torrent_host:
        return None
    if torrent_client not in ("qbittorrent", "transmission", "deluge"):
        return None
    return {
        "client": torrent_client,
        "host": torrent_host,
        "port": s.get("dl_torrent_port", "").strip(),
        "user": s.get("dl_torrent_user", "").strip(),
        "pass": s.get("dl_torrent_pass", ""),
    }


def _dl_parse_category_param(category: str | None, s: dict) -> str | None:
    """None = all categories. Otherwise exact match (case-insensitive) on client fields."""
    if category is not None:
        t = category.strip()
        if t in ("*", ""):
            return None
        return t
    v = (s.get("prowlarr_category") or "").strip()
    return v or None


def _dl_cat_match(item_cat: str | None, filt: str | None, s: dict | None = None) -> bool:
    """Match client category to filter. When filter is the scene Prowlarr category, also allow movie category."""
    if filt is None:
        return True
    ic = (item_cat or "").strip().lower()
    f = filt.strip().lower()
    if ic == f:
        return True
    if s:
        movie_cat = (s.get("prowlarr_category_movies") or "").strip().lower()
        scene_cat = (s.get("prowlarr_category") or "").strip().lower()
        if movie_cat and f == scene_cat and ic == movie_cat:
            return True
    return False


def _dl_resolve_import_dest_dir(
    s: dict,
    category: str | None,
    save_path: str,
    content_path: str,
) -> tuple[Path | None, str | None]:
    """
    Where to move videos after download-folder-style processing: scene queue (source_dir)
    or movie queue (movies_source_dir). Uses movie Prowlarr category and/or paths under
    movies_source_dir — same signals as the Downloads list filter.
    """
    movie_cat = (s.get("prowlarr_category_movies") or "").strip().lower()
    cat = (category or "").strip().lower()
    is_movie = bool(movie_cat and cat == movie_cat)
    if not is_movie:
        is_movie = _dl_path_under_movies_source(save_path, content_path, s)

    if is_movie:
        md = (s.get("movies_source_dir") or "").strip()
        if not md:
            return None, (
                "This job is treated as a movie (category or path under your Movies download folder), "
                "but Movies input folder is not set in Settings (Movies page)."
            )
        p = Path(md).expanduser()
        if not p.is_dir():
            return None, f"Movies input folder not found: {md}"
        return p, None

    sd = (s.get("source_dir") or "").strip()
    if not sd:
        return None, (
            "Scenes input folder is missing — set Input folder (files to process) "
            "(queue folder) under Scene Source Directory in Settings"
        )
    p = Path(sd).expanduser()
    if not p.is_dir():
        return None, f"Scenes input folder not found: {sd}"
    return p, None


def _dl_path_under_movies_source(save_path: str, content_path: str, s: dict) -> bool:
    """True if save/content path lies under configured movies_source_dir (movie download folder)."""
    root = (s.get("movies_source_dir") or "").strip()
    if not root:
        return False
    try:
        root_p = Path(root).expanduser().resolve()
    except Exception:
        return False
    for p in (save_path, content_path):
        if not (p or "").strip():
            continue
        try:
            pp = Path(p).expanduser().resolve()
            if pp == root_p or root_p in pp.parents:
                return True
        except Exception:
            continue
    return False


def _dl_torrent_matches_filter(
    cat: str | None,
    filt: str | None,
    save_path: str,
    content_path: str,
    s: dict,
) -> bool:
    """Category filter plus movie-category pairing; include torrents under movies_source_dir when filter is scene default."""
    if filt is None:
        return True
    if _dl_cat_match(cat, filt, s):
        return True
    scene_cat = (s.get("prowlarr_category") or "").strip().lower()
    if scene_cat and filt.strip().lower() == scene_cat:
        return _dl_path_under_movies_source(save_path, content_path, s)
    return False


def _qbittorrent_torrents_for_filter(sess, qb_base: str, filt: str | None, s: dict) -> list:
    """Fetch torrent list; when filter is scene default, also fetch movie Prowlarr category."""
    if filt is None:
        r = sess.get(f"{qb_base}/api/v2/torrents/info", timeout=25)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []

    scene_cat = (s.get("prowlarr_category") or "").strip()
    movie_cat = (s.get("prowlarr_category_movies") or "").strip()
    fl = filt.strip().lower()
    if (
        movie_cat
        and scene_cat
        and fl == scene_cat.lower()
        and movie_cat.strip().lower() != fl
    ):
        r1 = sess.get(
            f"{qb_base}/api/v2/torrents/info",
            params={"category": filt},
            timeout=25,
        )
        r1.raise_for_status()
        merged = list(r1.json() if isinstance(r1.json(), list) else [])
        by_hash = {(t.get("hash") or "").lower(): t for t in merged if t.get("hash")}
        r2 = sess.get(
            f"{qb_base}/api/v2/torrents/info",
            params={"category": movie_cat},
            timeout=25,
        )
        r2.raise_for_status()
        for t in r2.json() if isinstance(r2.json(), list) else []:
            h = (t.get("hash") or "").lower()
            if h and h not in by_hash:
                by_hash[h] = t
        return list(by_hash.values())

    r = sess.get(
        f"{qb_base}/api/v2/torrents/info",
        params={"category": filt},
        timeout=25,
    )
    r.raise_for_status()
    return r.json() if isinstance(r.json(), list) else []


def _nzbget_i64(lo, hi) -> int:
    return int(hi or 0) * (2**32) + int(lo or 0)


def _nzbget_rpc(host: str, port: str, method: str, params: list,
                user: str, password: str):
    p = port or "6789"
    url = f"http://{host}:{p}/jsonrpc"
    auth = (user, password) if user else None
    r = requests.post(
        url, json={"method": method, "params": params, "id": 1},
        auth=auth, timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data.get("result")


def _dl_collect_nzbget(ncfg: dict, filt: str | None, items: list, errors: list, s: dict) -> None:
    host, port = ncfg["host"], ncfg["port"]
    user, pw = ncfg["user"], ncfg["pass"]
    try:
        groups = _nzbget_rpc(host, port, "listgroups", [], user, pw)
        if not isinstance(groups, list):
            groups = []
        for g in groups:
            cat = str(g.get("Category") or "")
            if not _dl_cat_match(cat, filt, s):
                continue
            size = _nzbget_i64(g.get("FileSizeLo"), g.get("FileSizeHi"))
            rem = _nzbget_i64(g.get("RemainingSizeLo"), g.get("RemainingSizeHi"))
            if size <= 0 and g.get("FileSizeMB"):
                try:
                    size = int(float(g["FileSizeMB"])) * 1024 * 1024
                except (TypeError, ValueError):
                    size = 0
            prog = round(100.0 * (size - rem) / size, 1) if size > 0 else 0.0
            tid = g.get("NZBID")
            items.append({
                "id": f"nzbget-{tid}",
                "source": "nzb",
                "client": "nzbget",
                "name": g.get("NZBName") or g.get("Name") or "?",
                "category": cat,
                "progress_pct": prog,
                "size_mb": round(size / (1024 * 1024), 1) if size else None,
                "status": str(g.get("Status") or "QUEUED"),
                "queue": "active",
            })
    except Exception as e:
        errors.append(f"NZBGet queue: {e}")

    try:
        hist = None
        for params in ([False, 0, 200], [False], []):
            try:
                hist = _nzbget_rpc(host, port, "history", list(params), user, pw)
                break
            except Exception:
                hist = None
        if hist is None:
            raise RuntimeError("history RPC not supported or failed")
        if not isinstance(hist, list):
            hist = []
        for h in hist[:200]:
            cat = str(h.get("Category") or "")
            if not _dl_cat_match(cat, filt, s):
                continue
            size = _nzbget_i64(h.get("FileSizeLo"), h.get("FileSizeHi"))
            if size <= 0 and h.get("FileSizeMB"):
                try:
                    size = int(float(h["FileSizeMB"])) * 1024 * 1024
                except (TypeError, ValueError):
                    size = 0
            st = h.get("Status") or h.get("HistoryStatus") or "?"
            if isinstance(st, int):
                st = {1: "SUCCESS", 2: "FAILURE", 3: "DELETED"}.get(st, str(st))
            hid = h.get("NZBID") or h.get("Id") or len(items)
            st_u = str(st).upper()
            # NZBGet uses SUCCESS/UNPACK, SUCCESS/PAR, etc. — not the bare word "SUCCESS"
            ok = st_u.startswith("SUCCESS") or st_u in ("1", "COMPLETED")
            items.append({
                "id": f"nzbget-h-{hid}",
                "source": "nzb",
                "client": "nzbget",
                "name": h.get("Name") or h.get("NZBName") or "?",
                "category": cat,
                "progress_pct": 100.0 if ok else 0.0,
                "size_mb": round(size / (1024 * 1024), 1) if size else None,
                "status": str(st),
                "queue": "history",
            })
    except Exception as e:
        errors.append(f"NZBGet history: {e}")


def _dl_collect_sabnzbd(ncfg: dict, filt: str | None, items: list, errors: list, s: dict) -> None:
    host = ncfg["host"]
    port = ncfg["port"] or "8080"
    key = ncfg["apikey"]
    if not key:
        errors.append("SABnzbd: API key not configured")
        return
    base = f"http://{host}:{port}/sabnzbd/api"

    def _get(mode: str) -> dict:
        r = requests.get(
            base,
            params={"mode": mode, "apikey": key, "output": "json", "limit": 200},
            timeout=25,
        )
        r.raise_for_status()
        return r.json()

    try:
        data = _get("queue")
        q = data.get("queue") or {}
        slots = q.get("slots") or []
        for slot in slots:
            if not isinstance(slot, dict):
                continue
            cat = str(slot.get("cat") or "")
            if not _dl_cat_match(cat, filt, s):
                continue
            try:
                pct = float(str(slot.get("percentage") or "0").replace("%", ""))
            except ValueError:
                pct = 0.0
            try:
                mb = float(slot.get("mb") or 0)
            except (TypeError, ValueError):
                mb = 0
            nzo_id = str(slot.get("nzo_id") or slot.get("index") or len(items))
            items.append({
                "id": f"sab-q-{nzo_id}",
                "source": "nzb",
                "client": "sabnzbd",
                "name": slot.get("filename") or slot.get("nzb_name") or "?",
                "category": cat,
                "progress_pct": round(pct, 1),
                "size_mb": round(mb, 1) if mb else None,
                "status": str(slot.get("status") or "Downloading"),
                "queue": "active",
            })
    except Exception as e:
        errors.append(f"SABnzbd queue: {e}")

    try:
        data = _get("history")
        hsec = data.get("history") or {}
        slots = hsec.get("slots") or []
        for slot in slots:
            if not isinstance(slot, dict):
                continue
            cat = str(slot.get("category") or slot.get("cat") or "")
            if not _dl_cat_match(cat, filt, s):
                continue
            try:
                sz_b = int(slot.get("bytes") or slot.get("size") or 0)
            except (TypeError, ValueError):
                sz_b = 0
            status = str(slot.get("status") or "")
            hid = str(slot.get("nzo_id") or slot.get("nzb_id") or slot.get("nz_id") or len(items))
            items.append({
                "id": f"sab-h-{hid}",
                "source": "nzb",
                "client": "sabnzbd",
                "name": slot.get("name") or slot.get("nzb_name") or "?",
                "category": cat,
                "progress_pct": 100.0 if status.lower() in ("completed", "complete") else 0.0,
                "size_mb": round(sz_b / (1024 * 1024), 1) if sz_b else None,
                "status": status or "?",
                "queue": "history",
            })
    except Exception as e:
        errors.append(f"SABnzbd history: {e}")


def _qbittorrent_session(tcfg: dict) -> tuple[requests.Session, str]:
    port = tcfg["port"] or "8080"
    qb_base = f"http://{tcfg['host']}:{port}"
    sess = requests.Session()
    sess.headers.update({"Referer": f"{qb_base}/", "Origin": qb_base})
    login_r = sess.post(
        f"{qb_base}/api/v2/auth/login",
        data={"username": tcfg["user"], "password": tcfg["pass"]},
        timeout=12,
    )
    if login_r.status_code == 403:
        raise RuntimeError("qBittorrent 403 — check Web UI / IP ban")
    if (login_r.text or "").strip() == "Fails.":
        raise RuntimeError("qBittorrent login failed")
    return sess, qb_base


def _dl_collect_qbittorrent(tcfg: dict, filt: str | None, items: list, errors: list, s: dict) -> None:
    try:
        sess, qb_base = _qbittorrent_session(tcfg)
        torrents = _qbittorrent_torrents_for_filter(sess, qb_base, filt, s)
        if not isinstance(torrents, list):
            return
        for t in torrents:
            cat = str(t.get("category") or "")
            sp = (t.get("save_path") or "").strip()
            cp = (t.get("content_path") or "").strip()
            if not _dl_torrent_matches_filter(cat, filt, sp, cp, s):
                continue
            prog = round(float(t.get("progress") or 0) * 100.0, 1)
            sz = int(t.get("size") or 0)
            st = str(t.get("state") or "")
            qh = (t.get("hash") or "").strip().lower()
            if not qh:
                continue
            items.append({
                "id": f"qbit-{qh}",
                "source": "torrent",
                "client": "qbittorrent",
                "name": t.get("name") or "?",
                "category": cat,
                "progress_pct": prog,
                "size_mb": round(sz / (1024 * 1024), 1) if sz else None,
                "status": st,
                "queue": "active",
            })
    except Exception as e:
        errors.append(f"qBittorrent: {e}")


def _transmission_rpc_session(tcfg: dict) -> tuple[requests.Session, str]:
    tsess = requests.Session()
    if tcfg["user"]:
        tsess.auth = (tcfg["user"], tcfg["pass"])
    port = tcfg["port"] or "9091"
    base = f"http://{tcfg['host']}:{port}/transmission/rpc"
    try:
        sr = tsess.get(base, timeout=12)
    except Exception:
        sr = type("R", (), {"status_code": 0, "headers": {}})()
    tsess.headers["X-Transmission-Session-Id"] = sr.headers.get("X-Transmission-Session-Id", "") if sr.status_code == 409 else ""
    return tsess, base


def _dl_collect_transmission(tcfg: dict, filt: str | None, items: list, errors: list, s: dict) -> None:
    status_names = {
        0: "stopped", 1: "check-wait", 2: "checking", 3: "download-wait",
        4: "downloading", 5: "seed-wait", 6: "seeding",
    }
    try:
        tsess, base = _transmission_rpc_session(tcfg)
        body = {
            "method": "torrent-get",
            "arguments": {
                "fields": [
                    "id", "name", "status", "percentDone", "totalSize",
                    "rateDownload", "eta", "errorString", "labels", "downloadDir",
                ],
            },
        }
        r = tsess.post(base, json=body, timeout=25)
        r.raise_for_status()
        resp = r.json()
        if resp.get("result") != "success":
            raise RuntimeError(resp.get("result", resp))
        torrents = resp.get("arguments", {}).get("torrents") or []
        for t in torrents:
            labels = t.get("labels") or []
            cat = labels[0] if isinstance(labels, list) and labels else ""
            dd = (t.get("downloadDir") or "").strip()
            if not _dl_torrent_matches_filter(cat, filt, dd, "", s):
                continue
            pct = round(float(t.get("percentDone") or 0) * 100.0, 1)
            sz = int(t.get("totalSize") or 0)
            st_i = t.get("status")
            st = status_names.get(st_i, str(st_i))
            err = (t.get("errorString") or "").strip()
            if err:
                st = f"{st}: {err[:80]}"
            items.append({
                "id": f"tr-{t.get('id')}",
                "source": "torrent",
                "client": "transmission",
                "name": t.get("name") or "?",
                "category": cat,
                "progress_pct": pct,
                "size_mb": round(sz / (1024 * 1024), 1) if sz else None,
                "status": st,
                "queue": "active",
            })
    except Exception as e:
        errors.append(f"Transmission: {e}")


def _dl_collect_deluge(tcfg: dict, filt: str | None, items: list, errors: list, s: dict) -> None:
    port = tcfg["port"] or "8112"
    base = f"http://{tcfg['host']}:{port}"
    try:
        ds = requests.Session()
        lr = ds.post(
            f"{base}/json",
            json={"method": "auth.login", "params": [tcfg["pass"]], "id": 1},
            timeout=12,
        )
        lr.raise_for_status()
        if not lr.json().get("result"):
            raise RuntimeError("Deluge login failed")
        r2 = ds.post(
            f"{base}/json",
            json={
                "method": "core.get_torrents_status",
                "params": [
                    {},
                    ["name", "state", "progress", "total_wanted", "download_payload_rate", "save_path"],
                ],
                "id": 2,
            },
            timeout=25,
        )
        r2.raise_for_status()
        res = r2.json().get("result") or {}
        if not isinstance(res, dict):
            return
        for tid, t in res.items():
            if not isinstance(t, dict):
                continue
            cat = ""
            sp = str(t.get("save_path") or "").strip()
            if not _dl_torrent_matches_filter(cat, filt, sp, "", s):
                continue
            name = t.get("name") or "?"
            prog = float(t.get("progress") or 0)
            if 0 <= prog <= 1.0:
                prog = prog * 100.0
            tw = float(t.get("total_wanted") or 0)
            st = str(t.get("state") or "")
            items.append({
                "id": f"dl-{tid}",
                "source": "torrent",
                "client": "deluge",
                "name": name,
                "category": cat,
                "progress_pct": round(prog, 1),
                "size_mb": round(tw / (1024 * 1024), 1) if tw else None,
                "status": st,
                "queue": "active",
            })
    except Exception as e:
        errors.append(f"Deluge: {e}")


def _dl_item_import_ready(it: dict) -> bool:
    """True if the row is complete enough to run the same processing as download-folder watch."""
    q = it.get("queue")
    st = str(it.get("status") or "").upper()
    try:
        pct = float(it.get("progress_pct") if it.get("progress_pct") is not None else 0.0)
    except (TypeError, ValueError):
        pct = 0.0
    if q == "active":
        return pct >= 99.5
    if q == "history":
        if pct >= 99.0:
            return True
        # NZBGet: SUCCESS/UNPACK, SUCCESS/PAR, … — not always reflected in progress_pct
        if st.startswith("SUCCESS") or st in ("1", "COMPLETED", "COMPLETE") or st.startswith("COMPLETE"):
            return True
        return False
    return False


def _qbittorrent_content_path(sess, qb_base: str, hash_hex: str) -> Path | None:
    r = sess.get(
        f"{qb_base}/api/v2/torrents/info",
        params={"hashes": hash_hex},
        timeout=25,
    )
    if r.status_code != 200:
        return None
    arr = r.json()
    if not isinstance(arr, list) or not arr:
        return None
    t = arr[0]
    cp = (t.get("content_path") or "").strip()
    if cp:
        return Path(cp)
    save_path = (t.get("save_path") or "").strip()
    name = (t.get("name") or "").strip()
    if save_path and name:
        return Path(save_path) / name
    if save_path:
        return Path(save_path)
    return None


def _resolve_local_download_path(raw_path: Path, s: dict) -> Path | None:
    """
    Map paths reported by download clients (NAS/container) to paths visible on this host.

    1) URL-decode, normalise, and check existence.
    2) If the path is under download_watch_dir, movie_download_watch_dir, source_dir,
       movies_source_dir, or features_dir, resolve via that directory prefix (same paths as in
       Settings — no remapping needed when mounts match).
    """
    qs = _normalize_import_path_str(str(raw_path))
    if not qs:
        return None
    try:
        p = Path(qs).expanduser()
    except Exception:
        p = Path(qs)
    try:
        pnorm = Path(os.path.normpath(str(p)))
        for cand in (pnorm, p):
            try:
                if cand.exists():
                    return cand.resolve()
            except OSError:
                continue
    except OSError:
        pass

    def _try_anchor_prefix(anchor_str: str) -> Path | None:
        """
        If ``p`` is under ``anchor`` on disk, return the path via Settings' anchor.

        Uses ``os.path.realpath`` so NAS symlink layouts match (e.g. ``/share/Download/…`` in
        Top-Shelf vs ``/volume4/Downloads/…`` from NZBGet).
        """
        a = (anchor_str or "").strip()
        if not a:
            return None
        try:
            anchor = Path(a).expanduser()
            if not anchor.is_dir():
                return None
            ar = os.path.realpath(str(anchor))
            pr = os.path.realpath(str(p))
            if pr != ar and not pr.startswith(ar + os.sep):
                return None
            rel = os.path.relpath(pr, ar)
            if rel == ".." or rel.startswith(".."):
                return None
            candidate = anchor / rel
            if candidate.exists():
                return candidate.resolve()
        except (ValueError, OSError):
            return None
        return None

    for key in ("download_watch_dir", "movie_download_watch_dir", "source_dir", "movies_source_dir", "features_dir"):
        got = _try_anchor_prefix(s.get(key) or "")
        if got:
            return got

    try:
        lab = p.name
        if lab:
            par = p.parent
            if par.is_dir():
                alt = _find_matching_child_dir(par, lab)
                if alt is not None:
                    return alt
    except OSError:
        pass
    return None


def _find_matching_child_dir(parent: Path, label: str) -> Path | None:
    """
    Return ``parent/label`` if it exists; otherwise find a directory under ``parent`` whose name
    matches ``label`` ignoring case or Unicode NFC. Handles NAS/client vs filesystem naming drift.
    """
    if not label:
        return None
    try:
        if not parent.is_dir():
            return None
        direct = parent / label
        if direct.exists():
            return direct.resolve()
    except OSError:
        return None
    try:
        want = unicodedata.normalize("NFC", label).casefold()
    except Exception:
        want = label.casefold()
    try:
        for ch in parent.iterdir():
            if not ch.is_dir():
                continue
            try:
                cn = unicodedata.normalize("NFC", ch.name).casefold()
            except Exception:
                cn = ch.name.casefold()
            if cn == want:
                return ch.resolve()
    except OSError:
        pass
    alt2 = _find_overlap_child_dir(parent, label)
    if alt2 is not None:
        return alt2
    return None


def _find_overlap_child_dir(parent: Path, label: str) -> Path | None:
    """
    When the client path's last segment differs slightly from the folder on disk (truncation or
    extra ``.x264``-style suffix), pick the only subdirectory whose name shares a long prefix with
    ``label`` and similar length.
    """
    if not label or len(label) < 16:
        return None
    try:
        if not parent.is_dir():
            return None
        lf = unicodedata.normalize("NFC", label).casefold()
    except Exception:
        lf = label.casefold()
    stem = min(40, len(lf))
    if stem < 16:
        return None
    head = lf[:stem]
    hits: list[Path] = []
    try:
        for ch in parent.iterdir():
            if not ch.is_dir():
                continue
            try:
                cn = unicodedata.normalize("NFC", ch.name).casefold()
            except Exception:
                cn = ch.name.casefold()
            if cn == lf:
                continue
            if len(cn) < 8:
                continue
            if len(cn) >= stem:
                if cn[:stem] != head[:stem]:
                    continue
            else:
                if not head.startswith(cn):
                    continue
            if abs(len(cn) - len(lf)) > 28:
                continue
            hits.append(ch)
    except OSError:
        return None
    if len(hits) == 1:
        return hits[0].resolve()
    return None


def _download_client_dest_candidates(dest_raw: str, job_name: str) -> list[str]:
    """
    Download clients often report only the category folder (…/Features) or the full job path.
    Prefer ``base/job`` when the last component of ``base`` is not already the job folder name.
    Used for NZBGet and SABnzbd.
    """
    d = _normalize_import_path_str((dest_raw or "").strip())
    if not d:
        return []
    j = (job_name or "").replace(".nzb", "").strip()
    jn = _normalize_import_path_str(j) if j else ""
    if not jn:
        return [d]
    try:
        if Path(d).name.casefold() == unicodedata.normalize("NFC", jn).casefold():
            return [d]
    except Exception:
        if Path(d).name == jn:
            return [d]
    combined = os.path.normpath(d.rstrip("/") + "/" + jn)
    if combined != d:
        return [combined, d]
    return [d]


def _normalize_import_path_str(p: str) -> str:
    """Normalize client-reported paths for comparison (URL decoding, Windows backslashes, etc.)."""
    x = (p or "").strip().replace("\\", "/")
    if not x:
        return ""
    x = unquote(x)
    return os.path.normpath(x)


def _relative_path_under_client_root(client_abs: str, root: str) -> Path | None:
    """
    Path of ``client_abs`` relative to ``root`` when the client path is under that directory.
    Uses string prefix logic (not pathlib.relative_to) so odd filenames (e.g. brackets) and
    non-existent paths behave consistently.
    """
    ca = _normalize_import_path_str(client_abs)
    r = _normalize_import_path_str(root)
    if not r or not ca:
        return None
    if ca == r:
        return Path(".")
    sep = "/"
    prefix = r if r.endswith(sep) else r + sep
    if not ca.startswith(prefix):
        return None
    rel = ca[len(prefix) :].strip("/")
    return Path(rel) if rel else Path(".")


def _dest_dir_for_local_download_path(local: Path, s: dict) -> Path | None:
    """
    Scene queue (source_dir), movie queue (movies_source_dir), or movie library (features_dir)
    from where the file actually lives on this host (watch folders vs movies input).
    """
    watch = (s.get("download_watch_dir") or "").strip()
    mdw = (s.get("movie_download_watch_dir") or "").strip()
    ms = (s.get("movies_source_dir") or "").strip()
    sd = (s.get("source_dir") or "").strip()
    feat = (s.get("features_dir") or "").strip()
    try:
        lr = local.resolve()
    except OSError:
        return None
    try:
        if watch and sd:
            wr = Path(watch).expanduser().resolve()
            if lr == wr or wr in lr.parents:
                return Path(sd).expanduser()
        if mdw and feat:
            mwr = Path(mdw).expanduser().resolve()
            if lr == mwr or mwr in lr.parents:
                return Path(feat).expanduser()
        if ms:
            mr = Path(ms).expanduser().resolve()
            if lr == mr or mr in lr.parents:
                return Path(ms).expanduser()
    except OSError:
        return None
    return None


def _candidate_client_path_roots(save_path: str, content_path: str, reported: str) -> list[str]:
    """Possible torrent roots on the client (save_path, content parent, etc.) in try order."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in (save_path, content_path):
        x = _normalize_import_path_str(raw)
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    cp = _normalize_import_path_str(content_path)
    if cp:
        try:
            if Path(cp).suffix.lower() in VIDEO_EXTENSIONS:
                parent = _normalize_import_path_str(str(Path(cp).parent))
                if parent and parent not in seen:
                    seen.add(parent)
                    out.append(parent)
        except OSError:
            pass
    rp = _normalize_import_path_str(reported)
    if rp:
        parent = _normalize_import_path_str(str(Path(rp).parent))
        if parent and parent not in seen:
            seen.add(parent)
            out.append(parent)
    return out


def _resolve_local_download_path_via_save_mirror(
    reported: Path,
    save_path: str,
    content_path: str,
    s: dict,
    dest_dir: Path,
) -> Path | None:
    """
    When the client reports paths that only exist on another machine (e.g. /share/... on the
    torrent host) but the same files exist here under scene/movie watch directories or Movies input
    folder, map by path relative to each plausible client root onto local anchors.

    Tries multiple roots (save_path, content_path, parent of file video, etc.), then a
    basename-only match in each anchor directory.

    ``dest_dir`` is unused for picking anchors; callers should set import destination via
    ``_dest_dir_for_local_download_path`` when mirror succeeds.
    """
    _ = dest_dir  # kept for call-site compatibility
    rep_s = _normalize_import_path_str(str(reported))
    if not rep_s:
        return None
    watch = (s.get("download_watch_dir") or "").strip()
    mwatch = (s.get("movie_download_watch_dir") or "").strip()
    ms = (s.get("movies_source_dir") or "").strip()
    try:
        bn_early = Path(rep_s).name
    except OSError:
        bn_early = ""
    if bn_early:
        for anchor_str in (watch, mwatch, ms):
            if not anchor_str:
                continue
            anchor = Path(anchor_str).expanduser()
            if not anchor.is_dir():
                continue
            direct = anchor / bn_early
            try:
                if direct.exists():
                    return direct.resolve()
            except OSError:
                continue
    roots = _candidate_client_path_roots(save_path, content_path, rep_s)
    # Drop roots that equal the full reported path — they yield rel "." and would map to the
    # anchor dir instead of the job subfolder (NZB/SAB storage paths).
    roots = [r for r in roots if _normalize_import_path_str(r) != rep_s]
    if not roots:
        try:
            roots = [_normalize_import_path_str(str(Path(rep_s).parent))]
        except OSError:
            return None
    for sp in roots:
        rel = _relative_path_under_client_root(rep_s, sp)
        if rel is None:
            try:
                pp = _normalize_import_path_str(str(Path(rep_s).parent))
                if pp == sp:
                    rel = Path(Path(rep_s).name)
            except OSError:
                continue
        if rel is None:
            continue
        for anchor_str in (watch, mwatch, ms):
            if not anchor_str:
                continue
            anchor = Path(anchor_str).expanduser()
            if not anchor.is_dir():
                continue
            candidate = anchor / rel
            try:
                if candidate.exists():
                    return candidate.resolve()
            except OSError:
                continue
    return None


def _path_parts_no_root(norm: str) -> list[str]:
    """Path segments without leading drive/root (for building relative tails)."""
    try:
        p = Path(_normalize_import_path_str(norm))
        parts = list(p.parts)
    except Exception:
        return []
    out: list[str] = []
    for x in parts:
        if not x or x == "/" or x == "\\":
            continue
        if len(x) == 2 and x[1] == ":":
            continue
        out.append(x)
    return out


def _relative_tail_candidates_from_client_path(norm: str) -> list[str]:
    """
    All meaningful relative paths from the client-reported absolute path.

    Includes every suffix (…/Top-Shelf/Warez/…/Folder) and, when present, the path from
    ``Top-Shelf`` onward so we match Docker roots like ``/downloads`` → ``Top-Shelf/Warez/…``
    without a spurious ``Download/`` segment from the NAS path.
    """
    parts = _path_parts_no_root(norm)
    if not parts:
        return []
    tails: list[str] = []
    for i in range(len(parts)):
        tails.append("/".join(parts[i:]))
    low = [p.lower() for p in parts]
    for marker in ("top-shelf", "warez", "features", "series"):
        try:
            idx = low.index(marker)
        except ValueError:
            continue
        tails.append("/".join(parts[idx:]))
        if marker in ("top-shelf", "warez"):
            break
    seen: set[str] = set()
    out: list[str] = []
    for t in tails:
        t = (t or "").strip().strip("/")
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _bfs_find_dir_named(root: Path, dirname: str, max_depth: int = 14) -> Path | None:
    """Breadth-first search for a directory whose name equals ``dirname`` (exact)."""
    if not dirname:
        return None
    from collections import deque

    q: deque[tuple[Path, int]] = deque([(root, 0)])
    while q:
        cur, d = q.popleft()
        if d > max_depth:
            continue
        try:
            for ch in cur.iterdir():
                if not ch.is_dir():
                    continue
                if ch.name == dirname:
                    return ch.resolve()
                if d < max_depth:
                    q.append((ch, d + 1))
        except OSError:
            continue
    return None


def _find_local_download_under_watch_dirs(
    s: dict,
    *,
    job_name: str = "",
    client_reported_path: str = "",
) -> Path | None:
    """
    Locate the completed download under configured local dirs only (ignore client host prefix).

    Uses scene/movie watch dirs plus scene/movie *input* dirs as search roots. Tries relative
    tails from the client path (including from ``Top-Shelf`` onward), job/NZB name, basename,
    and a bounded directory-name search (handles ``..`` in folder names that break glob).
    """
    root_keys = (
        "download_watch_dir",
        "movie_download_watch_dir",
        "movies_source_dir",
        "features_dir",
        "source_dir",
    )
    roots: list[Path] = []
    seen_r: set[str] = set()
    for key in root_keys:
        raw = (s.get(key) or "").strip()
        if not raw:
            continue
        try:
            p = Path(raw).expanduser()
            if not p.is_dir():
                continue
            rp = str(p.resolve())
            if rp in seen_r:
                continue
            seen_r.add(rp)
            roots.append(p)
        except OSError:
            continue
    if not roots:
        return None

    rel_tails: list[str] = []
    cp = (client_reported_path or "").strip()
    if cp:
        try:
            norm = _normalize_import_path_str(cp)
            rel_tails.extend(_relative_tail_candidates_from_client_path(norm))
            rel_tails.append(Path(norm).name)
        except Exception:
            pass

    jn = (job_name or "").strip()
    name_candidates: list[str] = []
    if jn:
        name_candidates.append(jn.replace(".nzb", "").strip())
        name_candidates.append(jn)

    seen: set[str] = set()
    path_candidates: list[str] = []
    for t in rel_tails + name_candidates:
        t = (t or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        path_candidates.append(t)
    # If watch dir is …/Top-Shelf, also try path below it (drop leading Top-Shelf/)
    for t in list(path_candidates):
        if "/" in t and t.lower().startswith("top-shelf/"):
            sub = t.split("/", 1)[1].strip()
            if sub and sub not in seen:
                seen.add(sub)
                path_candidates.append(sub)
    # Movie watch is often …/Warez/Features — avoid doubling Warez/Features/ when joining root.
    for t in list(path_candidates):
        low = t.lower()
        if low.startswith("warez/features/") and "/" in t:
            sub = t.split("/", 2)[2].strip()
            if sub and sub not in seen:
                seen.add(sub)
                path_candidates.append(sub)

    for root in roots:
        for c in path_candidates:
            if "/" in c or "\\" in c:
                try:
                    hit = root / c.replace("\\", "/").lstrip("/")
                    if hit.exists():
                        return hit.resolve()
                except OSError:
                    continue
            else:
                try:
                    hit = root / c
                    if hit.exists():
                        return hit.resolve()
                    alt = _find_matching_child_dir(root, c)
                    if alt is not None:
                        return alt
                except OSError:
                    continue

        for c in path_candidates:
            if "/" in c or "\\" in c:
                continue
            if ".." in c or "*" in c or "[" in c or "?" in c:
                found = _bfs_find_dir_named(root, c)
                if found:
                    return found
                continue
            for depth in range(0, 9):
                pat = ("/".join(["*"] * depth) + "/" + c) if depth else c
                try:
                    for hit in root.glob(pat):
                        if hit.exists():
                            return hit.resolve()
                except OSError:
                    continue
            found = _bfs_find_dir_named(root, c)
            if found:
                return found
    return None


def _download_path_missing_message(s: dict, reported_path: str) -> str:
    """User-facing hint when a client-reported path is not visible to this process."""
    bits = []
    for key, label in (
        ("download_watch_dir", "Scene watch directory"),
        ("movie_download_watch_dir", "Movie watch directory"),
        ("source_dir", "Scene source folder"),
        ("movies_source_dir", "Movies input folder"),
        ("features_dir", "Movies Library Directory"),
    ):
        raw = (s.get(key) or "").strip()
        if not raw:
            continue
        try:
            ap = Path(raw).expanduser()
            ok = ap.is_dir()
            bits.append(f"{label} {raw!r} → {'ok' if ok else 'not found on this host'}")
        except OSError:
            bits.append(f"{label} {raw!r} → not accessible")
    cfg = (" (" + "; ".join(bits) + ")") if bits else ""
    return (
        f"Download path not found on this host: {reported_path}{cfg}. "
        "If the file exists under your scene or movie watch directory (or Movies input folder) with the same path "
        "relative to the torrent save path, import should still work; otherwise align bind mounts so that path "
        "is visible here, or set those directories to the roots that match the client’s folder layout."
    )


def _download_import_by_id(dl_id: str) -> dict:
    """Resolve download path and run _process_download_entry (same as download watch)."""
    s = db.get_settings()

    def _import_ok():
        db.mark_download_import_done(dl_id)
        return {"ok": True}

    remove_after = s.get("download_import_remove_client", "false").lower() == "true"

    # --- qBittorrent ---
    if dl_id.startswith("qbit-"):
        h = dl_id[5:].strip().lower()
        if len(h) < 8:
            return {"error": "Invalid torrent id"}
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "qbittorrent":
            return {"error": "qBittorrent not configured"}
        try:
            sess, qb_base = _qbittorrent_session(tcfg)
            r = sess.get(
                f"{qb_base}/api/v2/torrents/info",
                params={"hashes": h},
                timeout=25,
            )
            r.raise_for_status()
            arr = r.json()
            if not arr:
                return {"error": "Torrent not found in client"}
            prog = float(arr[0].get("progress") or 0)
            if prog < 0.999:
                return {"error": "Download is not complete yet"}
            t0 = arr[0]
            cat = str(t0.get("category") or "")
            sp = (t0.get("save_path") or "").strip()
            cp = (t0.get("content_path") or "").strip()
            dest_dir, derr = _dl_resolve_import_dest_dir(s, cat, sp, cp)
            if derr or not dest_dir:
                return {"error": derr or "Could not resolve import destination"}
            path = _qbittorrent_content_path(sess, qb_base, h)
            if not path:
                return {"error": "Torrent not found in client"}
            raw_reported = str(path)
            path = _resolve_local_download_path(path, s)
            used_mirror = False
            if not path:
                path = _resolve_local_download_path_via_save_mirror(
                    Path(raw_reported), sp, cp, s, dest_dir,
                )
                used_mirror = path is not None
            found_via_watch = False
            if not path:
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=(t0.get("name") or "").strip(),
                    client_reported_path=raw_reported,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": _download_path_missing_message(s, raw_reported)}
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            _process_download_entry(path, dest_dir)
            if remove_after:
                sess.post(
                    f"{qb_base}/api/v2/torrents/delete",
                    data={"hashes": h, "deleteFiles": "true"},
                    timeout=20,
                )
            emit(f"IMPORT qBittorrent {h} → {dest_dir.name} ({path.name})")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    # --- Transmission ---
    if dl_id.startswith("tr-"):
        tid_s = dl_id[3:].strip()
        try:
            tid = int(tid_s)
        except ValueError:
            return {"error": "Invalid Transmission id"}
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "transmission":
            return {"error": "Transmission not configured"}
        try:
            tsess, base = _transmission_rpc_session(tcfg)
            body = {
                "method": "torrent-get",
                "arguments": {
                    "ids": [tid],
                    "fields": ["id", "name", "percentDone", "downloadDir", "status", "labels"],
                },
            }
            r = tsess.post(base, json=body, timeout=25)
            r.raise_for_status()
            resp = r.json()
            if resp.get("result") != "success":
                return {"error": str(resp.get("result", resp))}
            torrents = resp.get("arguments", {}).get("torrents") or []
            if not torrents:
                return {"error": "Torrent not found"}
            t = torrents[0]
            if float(t.get("percentDone") or 0) < 0.999:
                return {"error": "Download is not complete yet"}
            dd = (t.get("downloadDir") or "").strip()
            nm = (t.get("name") or "").strip()
            if not dd or not nm:
                return {"error": "Could not resolve download path"}
            labels = t.get("labels") or []
            cat = labels[0] if isinstance(labels, list) and labels else ""
            cp = str(Path(dd) / nm)
            dest_dir, derr = _dl_resolve_import_dest_dir(s, cat, dd, cp)
            if derr or not dest_dir:
                return {"error": derr or "Could not resolve import destination"}
            raw_tr = str(Path(dd) / nm)
            path = Path(dd) / nm
            path = _resolve_local_download_path(path, s)
            used_mirror = False
            if not path:
                path = _resolve_local_download_path_via_save_mirror(
                    Path(raw_tr), dd, raw_tr, s, dest_dir,
                )
                used_mirror = path is not None
            found_via_watch = False
            if not path:
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=nm,
                    client_reported_path=raw_tr,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": _download_path_missing_message(s, raw_tr)}
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            _process_download_entry(path, dest_dir)
            if remove_after:
                tsess.post(
                    base,
                    json={
                        "method": "torrent-remove",
                        "arguments": {"ids": [tid], "delete-local-data": True},
                        "tag": 1,
                    },
                    timeout=20,
                )
            emit(f"IMPORT Transmission {tid} → {dest_dir.name}")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    # --- Deluge ---
    if dl_id.startswith("dl-"):
        tid_hex = dl_id[3:].strip()
        if not tid_hex:
            return {"error": "Invalid Deluge id"}
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "deluge":
            return {"error": "Deluge not configured"}
        port = tcfg["port"] or "8112"
        base = f"http://{tcfg['host']}:{port}"
        try:
            ds = requests.Session()
            ds.post(
                f"{base}/json",
                json={"method": "auth.login", "params": [tcfg["pass"]], "id": 1},
                timeout=12,
            )
            r2 = ds.post(
                f"{base}/json",
                json={
                    "method": "core.get_torrent_status",
                    "params": [
                        tid_hex,
                        ["name", "progress", "base_path", "save_path"],
                    ],
                    "id": 2,
                },
                timeout=25,
            )
            r2.raise_for_status()
            st = r2.json().get("result") or {}
            if not isinstance(st, dict):
                return {"error": "Torrent not found"}
            prog_raw = float(st.get("progress") or 0)
            prog_norm = (prog_raw / 100.0) if prog_raw > 1.0 else prog_raw
            if prog_norm < 0.999:
                return {"error": "Download is not complete yet"}
            path = None
            for key in ("base_path", "save_path"):
                v = st.get(key)
                if v and str(v).strip():
                    path = Path(str(v).strip())
                    break
            if not path:
                return {"error": "Could not resolve Deluge content path"}
            raw_reported = str(path)
            sp = str(path) if path.is_dir() else str(path.parent)
            dest_dir, derr = _dl_resolve_import_dest_dir(s, "", sp, str(path))
            if derr or not dest_dir:
                return {"error": derr or "Could not resolve import destination"}
            path = _resolve_local_download_path(path, s)
            used_mirror = False
            if not path:
                path = _resolve_local_download_path_via_save_mirror(
                    Path(raw_reported), sp, str(raw_reported), s, dest_dir,
                )
                used_mirror = path is not None
            found_via_watch = False
            if not path:
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=(st.get("name") or "").strip(),
                    client_reported_path=raw_reported,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": _download_path_missing_message(s, raw_reported)}
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            _process_download_entry(path, dest_dir)
            if remove_after:
                ds.post(
                    f"{base}/json",
                    json={
                        "method": "core.remove_torrent",
                        "params": [tid_hex, True],
                        "id": 3,
                    },
                    timeout=20,
                )
            emit(f"IMPORT Deluge {tid_hex[:12]}… → {dest_dir.name}")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    # --- SABnzbd ---
    if dl_id.startswith("sab-q-") or dl_id.startswith("sab-h-"):
        nzo_id = dl_id.split("-", 2)[2]
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "sabnzbd":
            return {"error": "SABnzbd not configured"}
        host = ncfg["host"]
        port = ncfg["port"] or "8080"
        key = ncfg["apikey"]
        if not key:
            return {"error": "SABnzbd API key not configured"}
        api = f"http://{host}:{port}/sabnzbd/api"
        try:
            mode = "queue" if dl_id.startswith("sab-q-") else "history"
            r = requests.get(
                api,
                params={"mode": mode, "apikey": key, "output": "json", "limit": 500},
                timeout=25,
            )
            r.raise_for_status()
            data = r.json()
            slots = (data.get("queue") or {}).get("slots") if mode == "queue" else (data.get("history") or {}).get("slots")
            slots = slots or []
            slot = None
            for sl in slots:
                if not isinstance(sl, dict):
                    continue
                sid = str(sl.get("nzo_id") or sl.get("nzb_id") or "")
                if sid == nzo_id:
                    slot = sl
                    break
            if not slot:
                return {"error": "Job not found in SABnzbd"}
            sab_cat = str(slot.get("cat") or slot.get("category") or "")
            if mode == "queue":
                try:
                    pct = float(str(slot.get("percentage") or "0").replace("%", ""))
                except ValueError:
                    pct = 0.0
                if pct < 99.5:
                    return {"error": "Download is not complete yet"}
            else:
                st = str(slot.get("status") or "").lower()
                if st not in ("completed", "complete"):
                    return {"error": "History entry is not completed"}
            storage = (slot.get("storage") or "").strip()
            if not storage:
                return {"error": "SABnzbd did not report a storage path yet"}
            storage_raw = _normalize_import_path_str(storage)
            sab_nm = (
                (slot.get("filename") or slot.get("name") or slot.get("nzb_name") or "")
                .strip()
            )
            path = None
            for cand in _download_client_dest_candidates(storage, sab_nm):
                p = _resolve_local_download_path(Path(cand), s)
                if p:
                    path = p
                    break
            used_mirror = False
            if not path:
                dest_dir_try, derr_try = _dl_resolve_import_dest_dir(
                    s, sab_cat, storage_raw, storage_raw
                )
                if not derr_try and dest_dir_try:
                    for cand in _download_client_dest_candidates(storage, sab_nm):
                        path = _resolve_local_download_path_via_save_mirror(
                            Path(cand),
                            str(Path(cand).parent),
                            cand,
                            s,
                            dest_dir_try,
                        )
                        if path:
                            used_mirror = True
                            break
            found_via_watch = False
            if not path:
                jn = sab_nm.replace(".nzb", "").strip()
                client_reported = (
                    os.path.normpath(storage_raw.rstrip("/") + "/" + _normalize_import_path_str(jn))
                    if jn
                    else storage_raw
                )
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=sab_nm,
                    client_reported_path=client_reported,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": f"Path not found: {storage}"}
            dest_dir = None
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            if dest_dir is None:
                dest_dir, derr = _dl_resolve_import_dest_dir(
                    s, sab_cat, storage_raw, str(path)
                )
                if derr or not dest_dir:
                    return {"error": derr or "Could not resolve import destination"}
            _process_download_entry(path, dest_dir)
            if remove_after:
                requests.get(
                    api,
                    params={
                        "mode": "queue" if mode == "queue" else "history",
                        "name": "delete",
                        "value": nzo_id,
                        "apikey": key,
                        "output": "json",
                        **({"del_files": "1"} if mode == "history" else {}),
                    },
                    timeout=20,
                )
            emit(f"IMPORT SABnzbd {nzo_id} → pipeline")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    # --- NZBGet ---
    if dl_id.startswith("nzbget-") and not dl_id.startswith("nzbget-h-"):
        tid_s = dl_id[7:]
        try:
            tid = int(tid_s)
        except ValueError:
            return {"error": "Invalid NZBGet id"}
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "nzbget":
            return {"error": "NZBGet not configured"}
        host, port = ncfg["host"], ncfg["port"]
        user, pw = ncfg["user"], ncfg["pass"]
        try:
            groups = _nzbget_rpc(host, port, "listgroups", [], user, pw)
            if not isinstance(groups, list):
                return {"error": "NZBGet listgroups failed"}
            g = None
            for x in groups:
                if int(x.get("NZBID", -1)) == tid:
                    g = x
                    break
            if not g:
                return {"error": "NZB not found in NZBGet queue"}
            ng_cat = str(g.get("Category") or "")
            rem = _nzbget_i64(g.get("RemainingSizeLo"), g.get("RemainingSizeHi"))
            if rem > 0:
                return {"error": "Download is not complete yet"}
            dest = (g.get("DestDir") or "").strip()
            if not dest:
                return {"error": "NZBGet did not report DestDir"}
            dest_raw = _normalize_import_path_str(dest)
            ng_nm = (g.get("NZBName") or g.get("Name") or "").strip()
            path = None
            for cand in _download_client_dest_candidates(dest, ng_nm):
                p = _resolve_local_download_path(Path(cand), s)
                if p:
                    path = p
                    break
            used_mirror = False
            if not path:
                dest_dir_try, derr_try = _dl_resolve_import_dest_dir(
                    s, ng_cat, dest_raw, dest_raw
                )
                if not derr_try and dest_dir_try:
                    for cand in _download_client_dest_candidates(dest, ng_nm):
                        path = _resolve_local_download_path_via_save_mirror(
                            Path(cand),
                            str(Path(cand).parent),
                            cand,
                            s,
                            dest_dir_try,
                        )
                        if path:
                            used_mirror = True
                            break
            found_via_watch = False
            if not path:
                jn = ng_nm.replace(".nzb", "").strip()
                client_reported = (
                    os.path.normpath(dest_raw.rstrip("/") + "/" + _normalize_import_path_str(jn))
                    if jn
                    else dest_raw
                )
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=ng_nm,
                    client_reported_path=client_reported,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": f"Path not found: {dest}"}
            dest_dir = None
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            if dest_dir is None:
                dest_dir, derr = _dl_resolve_import_dest_dir(
                    s, ng_cat, dest_raw, str(path)
                )
                if derr or not dest_dir:
                    return {"error": derr or "Could not resolve import destination"}
            _process_download_entry(path, dest_dir)
            if remove_after:
                _nzbget_rpc(host, port, "editqueue", ["GroupDelete", 0, "", [tid]], user, pw)
            emit(f"IMPORT NZBGet queue {tid} → pipeline")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    if dl_id.startswith("nzbget-h-"):
        hid_s = dl_id[9:]
        try:
            hid = int(hid_s)
        except ValueError:
            return {"error": "Invalid NZBGet history id"}
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "nzbget":
            return {"error": "NZBGet not configured"}
        host, port = ncfg["host"], ncfg["port"]
        user, pw = ncfg["user"], ncfg["pass"]
        try:
            hist = _nzbget_rpc(host, port, "history", [False, 0, 500], user, pw)
            if not isinstance(hist, list):
                return {"error": "NZBGet history failed"}
            h = None
            for x in hist:
                if int(x.get("NZBID", x.get("Id", -1))) == hid:
                    h = x
                    break
            if not h:
                return {"error": "History entry not found"}
            st = str(h.get("Status") or h.get("HistoryStatus") or "")
            if isinstance(st, int):
                st = {1: "SUCCESS", 2: "FAILURE"}.get(st, str(st))
            if "SUCCESS" not in st.upper() and st not in ("1",):
                return {"error": "History entry did not complete successfully"}
            dest = (h.get("DestDir") or h.get("FinalDir") or "").strip()
            name = (h.get("Name") or h.get("NZBName") or "").strip()
            if not dest:
                return {"error": "NZBGet did not report a destination path"}
            dest_raw = _normalize_import_path_str(dest)
            dest_for_res = dest_raw
            nh_cat = str(h.get("Category") or "")
            path = None
            for cand in _download_client_dest_candidates(dest, name):
                p = _resolve_local_download_path(Path(cand), s)
                if p:
                    path = p
                    break
            used_mirror = False
            if not path:
                dest_dir_try, derr_try = _dl_resolve_import_dest_dir(
                    s, nh_cat, dest_for_res, dest_for_res
                )
                if not derr_try and dest_dir_try:
                    for cand in _download_client_dest_candidates(dest, name):
                        path = _resolve_local_download_path_via_save_mirror(
                            Path(cand),
                            dest_for_res,
                            cand,
                            s,
                            dest_dir_try,
                        )
                        if path:
                            used_mirror = True
                            break
            found_via_watch = False
            if not path:
                jn = name.replace(".nzb", "").strip()
                raw_dest = (h.get("DestDir") or h.get("FinalDir") or "").strip()
                client_reported = (
                    os.path.normpath(dest_raw.rstrip("/") + "/" + _normalize_import_path_str(jn))
                    if jn
                    else _normalize_import_path_str(raw_dest)
                )
                path = _find_local_download_under_watch_dirs(
                    s,
                    job_name=name,
                    client_reported_path=client_reported,
                )
                found_via_watch = path is not None
            if not path:
                return {"error": f"Path not found: {dest}"}
            dest_dir = None
            if used_mirror or found_via_watch:
                d2 = _dest_dir_for_local_download_path(path, s)
                if d2 is not None:
                    dest_dir = d2
            if dest_dir is None:
                nh_cat = str(h.get("Category") or "")
                dest_for_res = _normalize_import_path_str(
                    (h.get("DestDir") or h.get("FinalDir") or "").strip(),
                )
                dest_dir, derr = _dl_resolve_import_dest_dir(
                    s, nh_cat, dest_for_res, str(path)
                )
                if derr or not dest_dir:
                    return {"error": derr or "Could not resolve import destination"}
            _process_download_entry(path, dest_dir)
            if remove_after:
                try:
                    _nzbget_rpc(host, port, "historydelete", [True, [hid], True], user, pw)
                except Exception:
                    _nzbget_rpc(host, port, "editqueue", ["HistoryDelete", hid, "", []], user, pw)
            emit(f"IMPORT NZBGet history {hid} → pipeline")
            return _import_ok()
        except Exception as e:
            return {"error": str(e)}

    return {"error": "Unsupported download id (or client not configured)"}


def _auto_import_completed_poll() -> None:
    """When enabled, import completed client jobs into the scene or movie input folder (same as manual Import)."""
    if processing_state.get("running"):
        return
    s = db.get_settings()
    if s.get("download_auto_import_enabled", "false").lower() != "true":
        return
    try:
        data = download_clients_combined_status(None)
    except Exception as e:
        emit(f"AUTO-IMPORT poll error: {e}")
        return
    for it in data.get("items") or []:
        if not it.get("import_ready"):
            continue
        dl_id = it.get("id")
        if not dl_id or db.was_download_import_done(dl_id):
            continue
        result = _download_import_by_id(dl_id)
        if result.get("ok"):
            emit(f"AUTO-IMPORT {dl_id} ({(it.get('name') or '')[:80]})")
        else:
            err = result.get("error", "") or ""
            # Path-not-found is usually a client/server path mismatch (NAS); avoid cluttering the pipeline log.
            if err.startswith("Path not found:"):
                _log.warning("AUTO-IMPORT skipped %s: %s", dl_id, err)
            else:
                emit(f"AUTO-IMPORT skipped {dl_id}: {err}")


def _download_remove_by_id(dl_id: str) -> dict:
    """Remove job from download client without importing (torrents: delete data)."""
    s = db.get_settings()
    if dl_id.startswith("qbit-"):
        h = dl_id[5:].strip().lower()
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "qbittorrent":
            return {"error": "qBittorrent not configured"}
        sess, qb_base = _qbittorrent_session(tcfg)
        gr = sess.post(
            f"{qb_base}/api/v2/torrents/delete",
            data={"hashes": h, "deleteFiles": "true"},
            timeout=20,
        )
        if gr.status_code == 200:
            return {"ok": True}
        return {"error": f"qBittorrent HTTP {gr.status_code}"}

    if dl_id.startswith("tr-"):
        try:
            tid = int(dl_id[3:])
        except ValueError:
            return {"error": "Invalid id"}
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "transmission":
            return {"error": "Transmission not configured"}
        tsess, base = _transmission_rpc_session(tcfg)
        r = tsess.post(
            base,
            json={
                "method": "torrent-remove",
                "arguments": {"ids": [tid], "delete-local-data": True},
                "tag": 1,
            },
            timeout=20,
        )
        if r.status_code == 200 and r.json().get("result") == "success":
            return {"ok": True}
        return {"error": r.text[:200]}

    if dl_id.startswith("dl-"):
        tid_hex = dl_id[3:].strip()
        tcfg = _dl_resolve_torrent_settings(s)
        if not tcfg or tcfg.get("client") != "deluge":
            return {"error": "Deluge not configured"}
        port = tcfg["port"] or "8112"
        base = f"http://{tcfg['host']}:{port}"
        ds = requests.Session()
        ds.post(
            f"{base}/json",
            json={"method": "auth.login", "params": [tcfg["pass"]], "id": 1},
            timeout=12,
        )
        r = ds.post(
            f"{base}/json",
            json={"method": "core.remove_torrent", "params": [tid_hex, True], "id": 2},
            timeout=20,
        )
        if r.status_code == 200:
            return {"ok": True}
        return {"error": r.text[:200]}

    if dl_id.startswith("sab-q-") or dl_id.startswith("sab-h-"):
        nzo_id = dl_id.split("-", 2)[2]
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "sabnzbd":
            return {"error": "SABnzbd not configured"}
        host = ncfg["host"]
        port = ncfg["port"] or "8080"
        key = ncfg["apikey"]
        mode = "queue" if dl_id.startswith("sab-q-") else "history"
        params = {
            "mode": mode,
            "name": "delete",
            "value": nzo_id,
            "apikey": key,
            "output": "json",
        }
        if mode == "history":
            params["del_files"] = "1"
        r = requests.get(f"http://{host}:{port}/sabnzbd/api", params=params, timeout=20)
        if r.status_code == 200:
            return {"ok": True}
        return {"error": r.text[:200]}

    if dl_id.startswith("nzbget-") and not dl_id.startswith("nzbget-h-"):
        try:
            tid = int(dl_id[7:])
        except ValueError:
            return {"error": "Invalid id"}
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "nzbget":
            return {"error": "NZBGet not configured"}
        host, port = ncfg["host"], ncfg["port"]
        user, pw = ncfg["user"], ncfg["pass"]
        try:
            _nzbget_rpc(host, port, "editqueue", ["GroupDelete", 0, "", [tid]], user, pw)
            return {"ok": True}
        except Exception as e:
            return {"error": str(e)}

    if dl_id.startswith("nzbget-h-"):
        try:
            hid = int(dl_id[9:])
        except ValueError:
            return {"error": "Invalid id"}
        ncfg = _dl_resolve_nzb_settings(s)
        if not ncfg or ncfg.get("client") != "nzbget":
            return {"error": "NZBGet not configured"}
        host, port = ncfg["host"], ncfg["port"]
        user, pw = ncfg["user"], ncfg["pass"]
        try:
            _nzbget_rpc(host, port, "historydelete", [True, [hid], True], user, pw)
            return {"ok": True}
        except Exception:
            try:
                _nzbget_rpc(host, port, "editqueue", ["HistoryDelete", hid, "", []], user, pw)
                return {"ok": True}
            except Exception as e:
                return {"error": str(e)}

    return {"error": "Unsupported download id"}


def download_clients_combined_status(category: str | None) -> dict:
    """NZB + torrent rows from whichever clients are selected in Settings."""
    s = db.get_settings()
    filt = _dl_parse_category_param(category, s)
    items: list = []
    errors: list = []
    notes: list = []

    ncfg = _dl_resolve_nzb_settings(s)
    if ncfg:
        if ncfg["client"] == "nzbget":
            _dl_collect_nzbget(ncfg, filt, items, errors, s)
        else:
            _dl_collect_sabnzbd(ncfg, filt, items, errors, s)

    tcfg = _dl_resolve_torrent_settings(s)
    if tcfg:
        tc = tcfg["client"]
        if tc == "qbittorrent":
            _dl_collect_qbittorrent(tcfg, filt, items, errors, s)
        elif tc == "transmission":
            if filt is not None:
                notes.append(
                    "Transmission: filter uses labels plus download dir vs Movies source folder when set."
                )
            _dl_collect_transmission(tcfg, filt, items, errors, s)
        elif tc == "deluge":
            if filt is not None:
                notes.append(
                    "Deluge: filter uses save path vs Movies source folder when category is unset."
                )
            _dl_collect_deluge(tcfg, filt, items, errors, s)

    items.sort(key=lambda x: (x.get("queue") != "active", (x.get("name") or "").lower()))
    for it in items:
        it["import_ready"] = _dl_item_import_ready(it)

    return {
        "category_filter": filt,
        "default_category": (s.get("prowlarr_category") or "Top-Shelf").strip(),
        "movie_category": (s.get("prowlarr_category_movies") or "").strip(),
        "movies_source_dir": (s.get("movies_source_dir") or "").strip(),
        "import_remove_after": s.get("download_import_remove_client", "false").lower() == "true",
        "auto_import_enabled": s.get("download_auto_import_enabled", "false").lower() == "true",
        "items": items,
        "errors": errors,
        "notes": notes,
        "configured": {
            "nzb": (ncfg or {}).get("client"),
            "torrent": (tcfg or {}).get("client"),
        },
    }


def _tpdb_scene_thumb(scene: dict) -> str | None:
    p0 = (scene.get("posters") or [None])[0]
    if not p0:
        return None
    if isinstance(p0, str):
        return p0
    if isinstance(p0, dict):
        return p0.get("url")
    return None


def tpdb_performer_scenes(performer_id: str, limit: int = 8) -> list[dict]:
    """Fetch recent scenes for a performer from TPDB."""
    try:
        resp = requests.get(
            f"https://api.theporndb.net/performers/{performer_id}/scenes",
            params={"page": 1, "per_page": limit},
            headers=_tpdb_headers(),
            timeout=15,
        )
        if resp.status_code == 200:
            scenes = resp.json().get("data") or []
            return [{
                "id":       str(s.get("id", "")),
                "title":    s.get("title", ""),
                "date":     s.get("date", ""),
                "studio":   (s.get("site") or {}).get("name", ""),
                "thumb":    _tpdb_scene_thumb(s),
            } for s in scenes]
    except Exception:
        pass
    return []


def tpdb_studio_scenes(studio_id: str, limit: int = 8) -> list[dict]:
    """Fetch recent scenes for a studio from TPDB."""
    try:
        resp = requests.get(
            f"https://api.theporndb.net/sites/{studio_id}/scenes",
            params={"page": 1, "per_page": limit},
            headers=_tpdb_headers(),
            timeout=15,
        )
        if resp.status_code == 200:
            scenes = resp.json().get("data") or []
            return [{
                "id":       str(s.get("id", "")),
                "title":    s.get("title", ""),
                "date":     s.get("date", ""),
                "studio":   (s.get("site") or {}).get("name", ""),
                "thumb":    _tpdb_scene_thumb(s),
            } for s in scenes]
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(next: str = "/scenes"):
    with open("static/login.html") as f:
        html = f.read().replace("__NEXT__", next)
    return html


@app.post("/api/auth/login")
async def auth_login(request: Request):
    from fastapi.responses import JSONResponse
    body = await request.json()
    password  = body.get("password", "")
    next_path = body.get("next", "/")
    ph = db.get_password_hash()
    if not ph:
        # No password set - allow through
        return JSONResponse({"ok": True})
    if not bcrypt.checkpw(password.encode(), ph.encode()):
        return JSONResponse({"error": "Incorrect password"}, status_code=401)
    token = secrets.token_urlsafe(32)
    hours = db.get_session_hours()
    db.create_session(token, hours)
    resp = JSONResponse({"ok": True, "next": next_path})
    resp.set_cookie(COOKIE_NAME, token, httponly=True, samesite="lax",
                    max_age=hours * 3600)
    return resp


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    from fastapi.responses import JSONResponse
    token = request.cookies.get(COOKIE_NAME, "")
    if token:
        db.delete_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(COOKIE_NAME)
    return resp


@app.post("/api/auth/set-password")
async def auth_set_password(request: Request):
    from fastapi.responses import JSONResponse
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorised"}, status_code=401)
    body = await request.json()
    password = body.get("password", "").strip()
    if len(password) < 6:
        return JSONResponse({"error": "Password must be at least 6 characters"}, status_code=400)
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    db.set_password(hashed)
    return JSONResponse({"ok": True})


@app.post("/api/auth/remove-password")
async def auth_remove_password(request: Request):
    from fastapi.responses import JSONResponse
    if not _is_authenticated(request):
        return JSONResponse({"error": "Unauthorised"}, status_code=401)
    db.set_password(None)
    return JSONResponse({"ok": True})


@app.get("/api/health")
async def health_check():
    """Public liveness probe; used by the boot splash until the app is ready."""
    return {"ok": True}


@app.get("/api/auth/status")
async def auth_status(request: Request):
    return {
        "password_set":   db.get_password_hash() is not None,
        "session_hours":  db.get_session_hours(),
        "authenticated":  _is_authenticated(request),
    }


@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/scenes", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    with open("static/queue.html") as f:
        return f.read()


@app.get("/queue", response_class=HTMLResponse)
async def queue_page():
    with open("static/queue.html") as f:
        return f.read()


@app.get("/tv", response_class=HTMLResponse)
async def tv_page():
    with open("static/queue.html") as f:
        return f.read()


@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    with open("static/queue.html") as f:
        return f.read()


@app.get("/api/queue")
async def get_queue():
    settings = db.get_settings()
    source_dir = Path(settings.get("source_dir", ""))
    if not source_dir.exists():
        return JSONResponse({"files": [], "error": f"Source dir not found: {source_dir}"})

    # Mark any tracked files that no longer exist on disk as removed
    retryable = db.get_retry_files()
    for fname in retryable:
        if not (source_dir / fname).exists():
            db.update_file(fname, status="removed")

    history = {r["filename"]: r["status"] for r in db.get_history(limit=10000)}
    files = []
    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
            prev_status = history.get(f.name)
            files.append({
                "filename":         f.name,
                "size_mb":          round(f.stat().st_size / 1024 / 1024, 1),
                "has_phash":        db.get_phash(f.name) is not None,
                "previously_filed": prev_status == "filed",
                "prev_status":      prev_status,
            })
    return {"files": files, "source_dir": str(source_dir)}


@app.get("/history", response_class=HTMLResponse)
async def history_page():
    with open("static/history.html") as f:
        return f.read()


@app.get("/api/history")
async def get_history(status: str = None, page: int = 1, per_page: int = 20,
                      sort_by: str = "processed_at", sort_dir: str = "DESC",
                      filter_text: str = None):
    return db.get_history_paged(status=status or None, page=page, per_page=per_page,
                                sort_by=sort_by, sort_dir=sort_dir,
                                filter_text=filter_text or None)


@app.get("/api/stats")
async def get_stats():
    return db.get_stats()


@app.get("/api/status")
async def get_status():
    return {"running": processing_state["running"],
            "current_file": processing_state["current_file"]}


@app.get("/api/settings")
async def get_settings():
    return {
        "settings": db.get_settings(),
        "directories": db.get_directories(),
    }


@app.post("/api/settings")
async def save_settings(payload: dict):
    db.save_settings(payload.get("settings", {}))
    db.save_directories(payload.get("directories", []))
    _apply_retry_schedule()
    _apply_tpdb_sync_schedule()
    _apply_favourites_schedule()
    scheduler.add_job(_refresh_hourly_content_cache, "interval", hours=1, id="content_cache_refresh", replace_existing=True)
    _restart_watcher()
    _restart_download_watcher()
    return {"saved": True}


@app.post("/api/retry/now")
async def retry_now(background_tasks: BackgroundTasks):
    """Manually trigger a retry run immediately."""
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    background_tasks.add_task(run_retry_pipeline)
    return {"started": True}


@app.get("/api/retry/status")
async def retry_status():
    """Return next scheduled retry time."""
    job = scheduler.get_job("retry")
    s = db.get_settings()
    return {
        "enabled":     s.get("retry_enabled", "true").lower() == "true",
        "next_run":    str(job.next_run_time) if job and job.next_run_time else None,
        "hour":        s.get("retry_hour", "1"),
        "frequency_h": s.get("retry_frequency_h", "24"),
        "pending":     len(db.get_retry_files()),
    }


@app.post("/api/run/all")
async def run_all(background_tasks: BackgroundTasks):
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    background_tasks.add_task(run_pipeline, None)
    return {"started": True}


@app.post("/api/run/file/{filename:path}")
async def run_file(filename: str, background_tasks: BackgroundTasks):
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    background_tasks.add_task(run_pipeline, [filename])
    return {"started": True, "filename": filename}


@app.post("/api/search")
async def search_scenes(payload: dict):
    """
    Search all three databases for scenes.
    Accepts: term (free text), title, performer, studio, date_from, date_to
    """
    term      = payload.get("term", "").strip() or None
    title     = payload.get("title", "").strip() or None
    performer = payload.get("performer", "").strip() or None
    studio    = payload.get("studio", "").strip() or None
    date_from = payload.get("date_from", "").strip() or None
    date_to   = payload.get("date_to", "").strip() or None

    if not any([term, title, performer, studio, date_from, date_to]):
        return JSONResponse({"error": "Provide at least one search field"}, status_code=400)

    try:
        results = search_all_databases(
            term=term, title=title, performer=performer,
            studio=studio, date_from=date_from, date_to=date_to,
        )
        # Format for UI
        formatted = []
        for scene in results:
            perf_names = [p["performer"]["name"] for p in scene.get("performers", [])]
            formatted.append({
                "id":          scene.get("id"),
                "title":       scene.get("title") or "Unknown",
                "studio":      (scene.get("studio") or {}).get("name") or "Unknown",
                "date":        scene.get("release_date") or "",
                "performers":  ", ".join(perf_names),
                "source":      scene.get("_source", ""),
                "image":       best_image_url(scene.get("images") or []),
                "_raw":        scene,
            })
        return {"results": formatted, "count": len(formatted)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/file/manual")
async def file_manual(payload: dict, background_tasks: BackgroundTasks):
    """
    File a specific video using a manually selected scene rather than phash lookup.
    Accepts: filename (str), scene (dict matching stash-box scene shape)
    """
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    filename = payload.get("filename", "").strip()
    scene    = payload.get("scene")

    if not filename or not scene:
        return JSONResponse({"error": "filename and scene are required"}, status_code=400)

    settings = db.get_settings()
    source_dir = Path(settings.get("source_dir", ""))
    video = source_dir / filename

    if not video.exists():
        return JSONResponse({"error": f"File not found: {filename}"}, status_code=404)

    def run():
        processing_state["running"] = True
        processing_state["log"] = []
        processing_state["current_file"] = filename
        try:
            db.upsert_file(filename)
            emit(f"FILE {filename}")
            emit("  Source: manual selection")
            result = file_scene_from_match(video, scene, source="Manual")
            emit(f"  Result: {result.get('status')}")
        except Exception as e:
            emit(f"  ERROR: {e}")
        finally:
            processing_state["running"] = False
            processing_state["current_file"] = None
            emit("---")

    background_tasks.add_task(run)
    return {"started": True, "filename": filename}


@app.get("/library", response_class=HTMLResponse)
async def library_page():
    with open("static/library.html") as f:
        return f.read()


@app.get("/favourites", response_class=HTMLResponse)
async def favourites_page():
    with open("static/favourites.html") as f:
        return f.read()


def _favourites_row_api(r: dict) -> dict:
    out = dict(r)
    ml = out.get("matches_locked")
    out["matches_locked"] = bool(int(ml)) if ml is not None else False
    fav = out.get("is_favourite")
    try:
        out["is_favourite"] = bool(int(fav)) if fav is not None else False
    except (TypeError, ValueError):
        out["is_favourite"] = bool(fav)
    aj = out.pop("aliases_json", None) or ""
    try:
        out["aliases"] = json.loads(aj) if aj.strip() else []
    except json.JSONDecodeError:
        out["aliases"] = []
    gfj = out.pop("gender_filters_json", None) or ""
    try:
        out["gender_filters"] = json.loads(gfj) if gfj.strip() else []
    except json.JSONDecodeError:
        out["gender_filters"] = []
    pm = out.get("path_missing")
    try:
        out["path_missing"] = bool(int(pm)) if pm is not None else False
    except (TypeError, ValueError):
        out["path_missing"] = bool(pm)
    return out


@app.get("/api/favourites")
async def api_favourites_list():
    rows = [_favourites_row_api(dict(x)) for x in db.favourite_list()]
    perf = [x for x in rows if x.get("kind") == "performer"]
    stud = [x for x in rows if x.get("kind") == "studio"]
    s = db.get_settings()
    return {
        "performers": perf,
        "studios": stud,
        "settings": {
            "favourites_scan_enabled": s.get("favourites_scan_enabled", "false"),
            "favourites_scan_hour": s.get("favourites_scan_hour", "3"),
        },
    }


@app.get("/api/favourites/folder-poster")
async def api_favourites_folder_poster(row_id: int):
    """Serve performer folder root poster.jpg or folder.jpg for favourites image_url."""
    row = db.favourite_get(row_id)
    if not row or row.get("kind") != "performer":
        return JSONResponse({"error": "Not found"}, status_code=404)
    p = _favourite_find_local_poster_file(row.get("path") or "")
    if not p or not p.is_file():
        return JSONResponse({"error": "Not found"}, status_code=404)
    mt, _ = mimetypes.guess_type(str(p))
    return FileResponse(p, media_type=mt or "image/jpeg")


@app.get("/api/favourites/folder-logo")
async def api_favourites_folder_logo(row_id: int):
    """Serve studio folder local art (logo.png, clearlogo.png, poster.jpg, folder.jpg)."""
    row = db.favourite_get(row_id)
    if not row or row.get("kind") != "studio":
        return JSONResponse({"error": "Not found"}, status_code=404)
    p = _favourite_find_local_studio_art(row.get("path") or "")
    if not p or not p.is_file():
        return JSONResponse({"error": "Not found"}, status_code=404)
    mt, _ = mimetypes.guess_type(str(p))
    return FileResponse(p, media_type=mt or "image/jpeg")


@app.get("/api/favourites/entity-panel")
async def api_favourites_entity_panel(row_id: int):
    """Poster, TPDB scenes, and metadata for the favourites detail panel."""
    row = db.favourite_get(row_id)
    if not row:
        return JSONResponse({"error": "Not found"}, status_code=404)
    r = _favourites_row_api(dict(row))
    scenes: list[dict] = []
    tid = (r.get("match_tpdb_id") or "").strip()
    if tid:
        if r.get("kind") == "studio":
            scenes = tpdb_studio_scenes(tid, 5)
        else:
            scenes = tpdb_performer_scenes(tid, 5)
    return {"row": r, "tpdb_scenes": scenes}


@app.get("/api/favourites/index-progress")
async def api_favourites_index_progress():
    """Poll scan / refresh-all progress (in-memory)."""
    with _favourites_index_lock:
        return dict(_favourites_index_progress)


@app.post("/api/favourites/scan")
async def api_favourites_scan(
    background_tasks: BackgroundTasks,
    prune_missing: bool = False,
    only_missing: bool = True,
):
    folders = _favourites_collect_folder_rows()
    _favourites_progress_start(
        "scan_missing" if only_missing else "scan_full", len(folders)
    )

    def run():
        try:
            favourites_scan_index(
                prune_missing=prune_missing,
                folders=folders,
                progress_init=False,
                only_missing=only_missing,
            )
        except Exception as e:
            emit(f"FAVOURITES scan error: {e}")
            _favourites_progress_finish()

    background_tasks.add_task(run)
    return {"started": True}


@app.post("/api/favourites/refresh")
async def api_favourites_refresh(payload: dict = Body(...)):
    row_id = int(payload.get("id") or 0)
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    scrape = payload.get("scrape_aliases", True)
    only_missing = bool(payload.get("only_missing", False))
    return favourites_refresh_entity_row(
        row_id,
        scrape_aliases=bool(scrape),
        only_missing=only_missing,
    )


def _favourites_refresh_all_loop(rows: list) -> None:
    done = 0
    for row in rows:
        nm = str(row.get("folder_name") or "")
        _favourites_progress_update(nm, done)
        try:
            favourites_refresh_entity_row(
                int(row["id"]),
                scrape_aliases=True,
                only_missing=False,
            )
        except Exception as e:
            emit(f"FAVOURITES refresh row {row.get('id')}: {e}")
        done += 1
        _favourites_progress_update(nm, done)


@app.post("/api/favourites/refresh-all")
async def api_favourites_refresh_all(background_tasks: BackgroundTasks):
    rows = db.favourite_list()
    n = len(rows)
    _favourites_progress_start("refresh_all", n)

    def run():
        try:
            _favourites_refresh_all_loop(rows)
        except Exception as e:
            emit(f"FAVOURITES refresh-all error: {e}")
        finally:
            _favourites_progress_finish()

    background_tasks.add_task(run)
    return {"started": True, "count": n}


def _favourites_refresh_images_all_loop(rows: list) -> None:
    done = 0
    for row in rows:
        nm = str(row.get("folder_name") or "")
        _favourites_progress_update(nm, done)
        try:
            favourites_refresh_entity_images(int(row["id"]))
        except Exception as e:
            emit(f"FAVOURITES refresh images row {row.get('id')}: {e}")
        done += 1
        _favourites_progress_update(nm, done)


@app.post("/api/favourites/refresh-images-all")
async def api_favourites_refresh_images_all(background_tasks: BackgroundTasks):
    rows = db.favourite_list()
    n = len(rows)
    _favourites_progress_start("refresh_images_all", n)

    def run():
        try:
            _favourites_refresh_images_all_loop(rows)
        except Exception as e:
            emit(f"FAVOURITES refresh-images-all error: {e}")
        finally:
            _favourites_progress_finish()

    background_tasks.add_task(run)
    return {"started": True, "count": n}


@app.post("/api/favourites/star")
async def api_favourites_star(payload: dict = Body(...)):
    row_id = int(payload.get("id") or 0)
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    db.favourite_set_star(row_id, bool(payload.get("is_favourite")))
    return {"ok": True}


@app.post("/api/favourites/lock")
async def api_favourites_lock(payload: dict = Body(...)):
    row_id = int(payload.get("id") or 0)
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    if not db.favourite_get(row_id):
        return JSONResponse({"error": "Not found"}, status_code=404)
    db.favourite_set_matches_locked(row_id, bool(payload.get("matches_locked")))
    return {"ok": True}


@app.post("/api/favourites/lock-all")
async def api_favourites_lock_all():
    n = db.favourite_set_all_matches_locked(True)
    return {"ok": True, "updated": n}


@app.post("/api/favourites/unlock-all")
async def api_favourites_unlock_all():
    n = db.favourite_set_all_matches_locked(False)
    return {"ok": True, "updated": n}


@app.post("/api/favourites/clear-matches")
async def api_favourites_clear_matches(payload: dict = Body(...)):
    row_id = int(payload.get("id") or 0)
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    if not db.favourite_get(row_id):
        return JSONResponse({"error": "Not found"}, status_code=404)
    db.favourite_clear_all_match_ids(row_id)
    return {"ok": True}


@app.post("/api/favourites/delete")
async def api_favourites_delete(payload: dict = Body(...)):
    """Remove one favourites index row (folder links). Does not delete files on disk."""
    row_id = int(payload.get("id") or 0)
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    if not db.favourite_get(row_id):
        return JSONResponse({"error": "Not found"}, status_code=404)
    db.favourite_delete(row_id)
    return {"ok": True}


@app.post("/api/favourites/unmatch")
async def api_favourites_unmatch(payload: dict = Body(...)):
    row_id = int(payload.get("id") or 0)
    source = (payload.get("source") or "").strip().upper()
    if not row_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    row = db.favourite_get(row_id)
    if not row:
        return JSONResponse({"error": "Not found"}, status_code=404)
    try:
        db.favourite_clear_source_match(row_id, source)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    row = db.favourite_get(row_id)
    if row:
        kind = row.get("kind")
        mt = (row.get("match_tpdb_id") or "").strip() or None
        ms = (row.get("match_stashdb_id") or "").strip() or None
        mf = (row.get("match_fansdb_id") or "").strip() or None
        if kind == "performer":
            sd = _favourite_performer_sort_birth_date(mt, ms, mf)
        else:
            sd = _favourite_studio_sort_birth_date(mt)
        db.favourite_set_sort_birth_date(row_id, sd)
    return {"ok": True}


@app.post("/api/favourites/match")
async def api_favourites_match(payload: dict = Body(...)):
    """Apply a chosen TPDB / StashDB / FansDB hit from manual search. Not gated by matches_locked."""
    row_id = int(payload.get("row_id") or 0)
    source = (payload.get("source") or "").strip().upper()
    ext_id = (payload.get("external_id") or "").strip()
    name = (payload.get("name") or "").strip()
    image = payload.get("image")
    if not row_id or not source or not ext_id:
        return JSONResponse(
            {"error": "row_id, source, and external_id required"},
            status_code=400,
        )
    kw: dict = {}
    if source == "TPDB":
        kw = {
            "match_tpdb_id": ext_id,
            "match_tpdb_name": name,
        }
    elif source == "STASHDB":
        kw = {
            "match_stashdb_id": ext_id,
            "match_stashdb_name": name,
        }
    elif source == "FANSDB":
        kw = {
            "match_fansdb_id": ext_id,
            "match_fansdb_name": name,
        }
    else:
        return JSONResponse({"error": "source must be TPDB, StashDB, or FansDB"}, status_code=400)
    row_pre = db.favourite_get(row_id)
    if row_pre and row_pre.get("kind") == "performer" and _favourite_find_local_poster_file(
        row_pre.get("path") or ""
    ):
        kw["image_url"] = f"/api/favourites/folder-poster?row_id={row_id}"
    elif row_pre and row_pre.get("kind") == "studio" and _favourite_find_local_studio_art(
        row_pre.get("path") or ""
    ):
        kw["image_url"] = f"/api/favourites/folder-logo?row_id={row_id}"
    elif image:
        kw["image_url"] = image
    db.favourite_update_matches(row_id, **kw)
    row = db.favourite_get(row_id)
    if row:
        kind = row.get("kind")
        mt = (row.get("match_tpdb_id") or "").strip() or None
        ms = (row.get("match_stashdb_id") or "").strip() or None
        mf = (row.get("match_fansdb_id") or "").strip() or None
        if kind == "performer":
            sd = _favourite_performer_sort_birth_date(mt, ms, mf)
        else:
            sd = _favourite_studio_sort_birth_date(mt)
        db.favourite_set_sort_birth_date(row_id, sd)
    return {"ok": True}


@app.get("/movies", response_class=HTMLResponse)
async def movies_page():
    with open("static/movies.html") as f:
        return f.read()


@app.get("/downloads", response_class=HTMLResponse)
async def downloads_page():
    with open("static/downloads.html") as f:
        return f.read()


@app.get("/api/movies/search")
async def movies_search(q: str = "", year: str = "", page: int = 1):
    try:
        base_params = {}
        if q.strip():
            base_params["parse"] = q.strip()
        if year:
            base_params["year"] = year
        return _collect_filtered_tpdb_movies(base_params, page=page, page_size=20)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/movies/tpdb/latest")
async def movies_tpdb_latest(page: int = 1):
    """Get latest TPDB movie releases, filtered by content categories."""
    try:
        p = max(1, int(page or 1))
        if p == 1:
            fk = _movies_cache_filter_key()
            with _content_cache_lock:
                age = time.time() - _movies_latest_cache["ts"]
                cache_ok = (
                    _movies_latest_cache["results"]
                    and age < _MOVIES_CACHE_TTL
                    and _movies_latest_cache.get("filter_key") == fk
                )
                if cache_ok:
                    return {
                        "results": _movies_latest_cache["results"],
                        "cached": True,
                        "cache_age_s": int(age),
                        "page": 1,
                        "total_pages": 1,
                    }
            payload = _collect_filtered_tpdb_movies({}, page=1, page_size=20)
            with _content_cache_lock:
                _movies_latest_cache["results"] = payload.get("results", [])
                _movies_latest_cache["ts"] = time.time()
                _movies_latest_cache["filter_key"] = fk
            return payload
        return _collect_filtered_tpdb_movies({}, page=p, page_size=20)
    except Exception as e:
        return {"results": [], "error": str(e)}


@app.get("/api/movies/tpdb-test")
async def movies_tpdb_test():
    """Debug: raw TPDB /movies response."""
    try:
        url = "https://api.theporndb.net/movies"
        params = {"page": 1, "limit": 10}
        headers = _tpdb_headers()
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        return {
            "status": resp.status_code,
            "url": resp.url,
            "headers_sent": {k: v[:20] + "..." if len(v) > 20 else v for k, v in headers.items()},
            "response_text": resp.text[:500],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/movies/tpdb/{movie_id}")
async def movies_tpdb_detail(movie_id: str):
    """Get full TPDB movie detail."""
    try:
        resp = requests.get(f"https://api.theporndb.net/movies/{movie_id}",
                            headers=_tpdb_headers(), timeout=15)
        if resp.status_code != 200:
            return JSONResponse({"error": f"TPDB returned {resp.status_code}"}, status_code=resp.status_code)
        m = resp.json().get("data") or {}
        # image = front cover (portrait), back_image = back cover
        poster = m.get("image") or m.get("poster") or m.get("poster_image")
        bg = m.get("back_image") or m.get("poster")
        if not poster:
            posters_obj = m.get("posters") or {}
            if isinstance(posters_obj, dict):
                poster = posters_obj.get("full") or posters_obj.get("large")
        performers = []
        performer_links = []
        for p in (m.get("performers") or []):
            if isinstance(p, dict):
                perf = p.get("performer") or p
                name = perf.get("name") or perf.get("full_name") or ""
                slug = perf.get("slug") or perf.get("_id") or perf.get("id")
                url = ""
                if slug:
                    url = f"https://theporndb.net/performers/{slug}"
                elif perf.get("url"):
                    url = str(perf.get("url"))
                if name:
                    performers.append(name)
                    performer_links.append({"name": name, "url": url})
            elif isinstance(p, str):
                performers.append(p)
                performer_links.append({"name": p, "url": ""})
        scenes = []
        for s in (m.get("scenes") or []):
            thumb = s.get("poster_image") or s.get("image")
            scenes.append({
                "id": str(s.get("_id") or s.get("id") or ""),
                "title": s.get("title") or "",
                "date": s.get("date") or "",
                "thumb": thumb,
            })
        tmdb_id = (
            m.get("tmdb_id")
            or (m.get("tmdb") or {}).get("id")
            or (m.get("movie") or {}).get("tmdb_id")
        )
        tmdb_url = ""
        if tmdb_id:
            tmdb_url = f"https://www.themoviedb.org/movie/{tmdb_id}"
        else:
            tmdb_url = f"https://www.themoviedb.org/search/movie?query={quote_plus(m.get('title') or '')}"

        return {
            "id":          str(m.get("_id") or m.get("id") or ""),
            "slug":        m.get("slug") or m.get("_id") or str(m.get("id", "")),
            "title":       m.get("title") or "",
            "date":        m.get("date") or "",
            "year":        (m.get("date") or "")[:4],
            "studio":      (m.get("site") or {}).get("name") or (m.get("studio") or {}).get("name") or "",
            "duration":    m.get("duration") or 0,
            "synopsis":    m.get("description") or m.get("synopsis") or "",
            "poster":      poster,
            "background":  bg,
            "performers":  performers,
            "performer_links": performer_links,
            "directors":   m.get("directors") or [],
            "scenes":      scenes,
            "url":         f"https://theporndb.net/movies/{m.get('slug') or m.get('_id') or movie_id}",
            "tmdb_url":    tmdb_url,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/movies/detail/{tmdb_id}")
async def movies_detail(tmdb_id: str):
    try:
        movie = get_tmdb_movie(tmdb_id)
        return {"movie": movie}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/movies/tpdb-test")
async def movies_tpdb_test():
    """Debug: raw TPDB /movies response."""
    try:
        url = "https://api.theporndb.net/movies"
        params = {"page": 1, "limit": 10}
        headers = _tpdb_headers()
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        return {
            "status": resp.status_code,
            "url": resp.url,
            "headers_sent": {k: v[:20] + "..." if len(v) > 20 else v for k, v in headers.items()},
            "response_headers": dict(resp.headers),
            "response_text": resp.text[:500],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/movies/queue")
async def movies_queue():
    settings = db.get_settings()
    source_dir = Path((settings.get("movies_source_dir") or "").strip())
    if not source_dir.exists():
        return {
            "files": [],
            "error": f"Movies source dir not found or not set: {source_dir or '(empty)'}",
        }
    rows = db.get_movie_rows_map()
    files = []
    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
            row = rows.get(f.name)
            prev_status = (row or {}).get("status") if row else None
            if prev_status == "pending":
                prev_status = None
            files.append({
                "filename": f.name,
                "size_mb": round(f.stat().st_size / 1024 / 1024, 1),
                "has_phash": False,
                "previously_filed": prev_status == "filed",
                "prev_status": prev_status,
                "match_title": (row or {}).get("title"),
                "match_source": (row or {}).get("match_source"),
            })
    return {"files": files, "source_dir": str(source_dir)}


@app.get("/api/movies/tmdb-search")
async def movies_tmdb_search(q: str = "", year: str = ""):
    """TMDB movie search (for queue manual pick)."""
    try:
        if not (db.get_settings().get("api_key_tmdb") or "").strip():
            return {"results": [], "error": "TMDB API key not configured"}
        return {"results": search_tmdb(q.strip(), year.strip() or None)}
    except Exception as e:
        return JSONResponse({"results": [], "error": str(e)}, status_code=500)


@app.get("/api/movies/history")
async def movies_history():
    return {"history": db.get_movie_history()}


@app.get("/api/movies/stats")
async def movies_stats():
    return db.get_movie_stats()


@app.post("/api/movies/file")
async def file_movie_endpoint(payload: dict, background_tasks: BackgroundTasks):
    """File a video as a movie using TMDB id or TPDB id (TMDB takes precedence if both sent)."""
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    filename = payload.get("filename", "").strip()
    tmdb_id = payload.get("tmdb_id", "").strip()
    tpdb_id = payload.get("tpdb_id", "").strip()

    if not filename or (not tmdb_id and not tpdb_id):
        return JSONResponse(
            {"error": "filename and either tmdb_id or tpdb_id required"},
            status_code=400,
        )

    settings = db.get_settings()
    source_dir = Path((settings.get("movies_source_dir") or "").strip())
    video = source_dir / filename

    if not video.exists():
        return JSONResponse({"error": f"File not found: {filename}"}, status_code=404)

    def run():
        processing_state["running"] = True
        processing_state["log"] = []
        processing_state["current_file"] = filename
        try:
            emit(f"FILE {filename}")
            if tmdb_id:
                emit(f"  Fetching TMDB details for ID {tmdb_id}...")
                movie = get_tmdb_movie(tmdb_id)
                file_movie(video, movie, match_source="tmdb")
            else:
                emit(f"  Fetching TPDB movie {tpdb_id}...")
                detail = _fetch_tpdb_movie_detail(tpdb_id)
                if not detail:
                    raise RuntimeError("TPDB movie not found")
                md = _tpdb_detail_as_movie_filing_dict(detail)
                tid = md.pop("_tpdb_id", None) or tpdb_id
                file_movie(video, md, tpdb_id=str(tid), match_source="tpdb")
        except Exception as e:
            emit(f"  ERROR: {e}")
            db.update_movie(filename, status="error", error=str(e))
        finally:
            processing_state["running"] = False
            processing_state["current_file"] = None
            emit("---")

    background_tasks.add_task(run)
    return {"started": True, "filename": filename}


@app.post("/api/movies/run/all")
async def movies_run_all(background_tasks: BackgroundTasks):
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    def run():
        run_movie_pipeline(None)

    background_tasks.add_task(run)
    return {"started": True}


@app.post("/api/movies/run/file/{filename:path}")
async def movies_run_file(filename: str, background_tasks: BackgroundTasks):
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    def run():
        run_movie_pipeline([filename])

    background_tasks.add_task(run)
    return {"started": True, "filename": filename}


@app.post("/api/file/manual/metadata")
async def file_manual_metadata(payload: dict, background_tasks: BackgroundTasks):
    """
    File a video using manually entered metadata, bypassing all database lookups.
    Accepts: filename, title, studio, date (YYYY-MM-DD), performers (comma-separated),
             plot (optional), image_url (optional)
    """
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    filename       = payload.get("filename", "").strip()
    title          = payload.get("title", "").strip()
    studio         = payload.get("studio", "").strip()
    date           = payload.get("date", "").strip()
    performers     = payload.get("performers", "").strip()
    plot           = payload.get("plot", "").strip()
    image_url      = payload.get("image_url", "").strip()
    thumb_data_url = payload.get("thumb_data_url", "").strip()

    if not all([filename, title, studio, date]):
        return JSONResponse({"error": "filename, title, studio and date are required"}, status_code=400)

    settings   = db.get_settings()
    source_dir = Path(settings.get("source_dir", ""))
    video      = source_dir / filename

    if not video.exists():
        return JSONResponse({"error": f"File not found: {filename}"}, status_code=404)

    # Build a scene dict in stash-box shape so file_scene_from_match can handle it
    perf_list = [p.strip() for p in performers.split(",") if p.strip()]
    scene = {
        "title":           title,
        "release_date":    date,
        "studio":          {"name": studio},
        "performers":      [{"performer": {"name": n, "gender": ""}} for n in perf_list],
        "images":          [{"url": image_url, "width": 0, "height": 0}] if image_url else [],
        "_plot":           plot,
        "_thumb_data_url": thumb_data_url,
    }

    def run():
        processing_state["running"] = True
        processing_state["log"] = []
        processing_state["current_file"] = filename
        try:
            db.upsert_file(filename)
            emit(f"FILE {filename}")
            emit("  Source: manual metadata")
            result = file_scene_from_match(video, scene, source="Manual")
            emit(f"  Result: {result.get('status')}")
        except Exception as e:
            emit(f"  ERROR: {e}")
            db.update_file(filename, status="error", error=str(e))
        finally:
            processing_state["running"] = False
            processing_state["current_file"] = None
            emit("---")

    background_tasks.add_task(run)
    return {"started": True, "filename": filename}


@app.get("/api/parse/filename")
async def parse_filename_endpoint(filename: str):
    """Parse a filename and return extracted metadata fields."""
    settings = db.get_settings()
    result = parse_filename(filename, settings)
    return result


# ---------------------------------------------------------------------------
# Library issue finder
# ---------------------------------------------------------------------------

NFO_SKIP = {"tvshow.nfo", "season.nfo", "season-all.nfo", "season-specials.nfo"}


def scan_library_issues(root_dirs: list[str], movie_dirs: list[str] = None) -> dict:
    """
    Scan library directories for common issues:
    - NFO without matching video
    - Video without NFO
    - Missing thumbnail (-thumb.jpg)
    - Empty season folders

    movie_dirs: directories where each subfolder is a movie folder.
    For movies, NFO must match the video filename or be named movie.nfo.
    """
    issues = {
        "nfo_no_video":  [],
        "video_no_nfo":  [],
        "missing_thumb": [],
        "empty_folder":  [],
    }

    movie_dir_set = set(str(Path(d)) for d in (movie_dirs or []) if d)

    for root_dir in root_dirs:
        base = Path(root_dir)
        if not base.exists():
            continue

        is_movie_root = str(base) in movie_dir_set

        for dirpath, dirnames, filenames in os.walk(base):
            dp = Path(dirpath)
            videos = [f for f in filenames if Path(f).suffix.lower() in VIDEO_EXTENSIONS]
            nfos   = [f for f in filenames if f.endswith(".nfo") and f.lower() not in NFO_SKIP]

            # Empty folder check
            if not videos and not nfos and not filenames:
                issues["empty_folder"].append(str(dp))
                continue

            # Movie folder logic - one level below the movie root
            if is_movie_root and dp.parent == base:
                for video in videos:
                    stem = Path(video).stem
                    # Accept: matching name NFO or movie.nfo
                    has_nfo = f"{stem}.nfo" in filenames or "movie.nfo" in filenames
                    if not has_nfo:
                        issues["video_no_nfo"].append(str(dp / video))
                    # NFO without video (orphan NFOs, excluding movie.nfo)
                for nfo in nfos:
                    if nfo.lower() == "movie.nfo":
                        continue
                    stem = Path(nfo).stem
                    if not any(Path(v).stem == stem for v in videos):
                        issues["nfo_no_video"].append(str(dp / nfo))
                continue  # No thumb check for movies - artwork naming varies too much

            # Standard scene library logic
            for nfo in nfos:
                stem = Path(nfo).stem
                if not any(Path(v).stem == stem for v in videos):
                    issues["nfo_no_video"].append(str(dp / nfo))

            for video in videos:
                stem = Path(video).stem
                if f"{stem}.nfo" not in filenames:
                    issues["video_no_nfo"].append(str(dp / video))

            for video in videos:
                stem = Path(video).stem
                if f"{stem}-thumb.jpg" not in filenames:
                    issues["missing_thumb"].append(str(dp / video))

    return {k: sorted(v) for k, v in issues.items()}


@app.get("/api/library/scan")
async def library_scan(background_tasks: BackgroundTasks):
    """Trigger a library issue scan across all configured dirs."""
    settings = db.get_settings()
    dirs = [
        settings.get("series_dir", ""),
        settings.get("features_dir", ""),
    ]
    # Add performer dirs
    for d in db.get_directories():
        dirs.append(d["path"])
    dirs = [d for d in dirs if d]

    features_dir = settings.get("features_dir", "")
    movie_dirs = [features_dir] if features_dir else []

    def run():
        results = scan_library_issues(dirs, movie_dirs=movie_dirs)
        db.save_setting("_library_scan_results", __import__("json").dumps(results))
        db.save_setting("_library_scan_time", __import__("datetime").datetime.now().isoformat(timespec="seconds"))

    background_tasks.add_task(run)
    return {"started": True}


@app.get("/api/library/results")
async def library_results():
    settings = db.get_settings()
    raw = settings.get("_library_scan_results")
    scan_time = settings.get("_library_scan_time", "")
    if not raw:
        return {"results": None, "scan_time": None}
    import json as _json
    return {"results": _json.loads(raw), "scan_time": scan_time}


@app.post("/api/stashdb/resolve")
async def stashdb_resolve(payload: dict):
    """
    Resolve studio and performer names against StashDB.
    Returns exact matches and near-matches for confirmation.
    """
    s = db.get_settings()
    api_key = s.get("api_key_stashdb", "")
    if not api_key:
        return JSONResponse({"error": "No StashDB API key configured"}, status_code=400)

    studio_name    = payload.get("studio", "").strip()
    performer_names = [p.strip() for p in payload.get("performers", "").split(",") if p.strip()]

    result = {"studio": None, "performers": [], "needs_confirm": False}

    # Resolve studio
    if studio_name:
        matches = stashdb_search_studio(studio_name, api_key)
        exact = next((m for m in matches if m["name"].lower() == studio_name.lower()), None)
        if exact:
            result["studio"] = {"id": exact["id"], "name": exact["name"], "confirmed": True}
        elif matches:
            result["studio"] = {"id": None, "name": studio_name, "suggestions": matches[:3], "confirmed": False}
            result["needs_confirm"] = True
        else:
            result["studio"] = {"id": None, "name": studio_name, "suggestions": [], "confirmed": False}

    # Resolve performers
    for pname in performer_names:
        matches = stashdb_search_performer(pname, api_key)
        exact = next((m for m in matches if m["name"].lower() == pname.lower()), None)
        if exact:
            result["performers"].append({"id": exact["id"], "name": exact["name"], "confirmed": True})
        elif matches:
            result["performers"].append({"id": None, "name": pname, "suggestions": matches[:3], "confirmed": False})
            result["needs_confirm"] = True
        else:
            result["performers"].append({"id": None, "name": pname, "suggestions": [], "confirmed": False})

    return result


@app.post("/api/stashdb/submit-manual")
async def stashdb_submit_manual(payload: dict):
    """
    Submit a manually-created scene to StashDB.
    Expects resolved studio_id, performer_ids, plus title/date/phash etc.
    """
    s = db.get_settings()
    api_key = s.get("api_key_stashdb", "")
    if not api_key:
        return JSONResponse({"error": "No StashDB API key configured"}, status_code=400)

    filename = payload.get("filename", "")
    phash    = db.get_phash(filename) if filename else None

    # Get video duration for fingerprint accuracy
    duration = 0
    if filename:
        s = db.get_settings()
        source_dir = Path(s.get("source_dir", ""))
        video_path = source_dir / filename
        if not video_path.exists():
            # Try destination path from history
            hist = [r for r in db.get_history(limit=1000) if r["filename"] == filename]
            if hist and hist[0].get("destination"):
                video_path = Path(hist[0]["destination"])
        try:
            dur = get_video_duration(video_path)
            if dur:
                duration = int(dur)
        except Exception:
            pass

    result = stashdb_submit_scene(
        title         = payload.get("title", ""),
        date          = payload.get("date", ""),
        studio_id     = payload.get("studio_id"),
        performer_ids = payload.get("performer_ids", []),
        phash         = phash,
        details       = payload.get("plot", ""),
        image_url     = payload.get("image_url", ""),
        api_key       = api_key,
        duration      = duration,
    )
    return result


@app.post("/api/thumb/generate")
async def generate_thumb(payload: dict):
    """Generate a thumbnail from a video at a random percent between 10-90."""
    import random
    filename  = payload.get("filename", "").strip()
    percent   = payload.get("percent")
    if percent is None:
        percent = random.uniform(10, 90)

    s = db.get_settings()
    source_dir = Path(s.get("source_dir", ""))
    video = source_dir / filename
    if not video.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)

    data_url = capture_frame_as_base64(video, float(percent))
    if not data_url:
        return JSONResponse({"error": "Frame capture failed"}, status_code=500)
    return {"data_url": data_url, "percent": round(percent, 1)}


@app.post("/api/download/process-all")
async def process_all_downloads(background_tasks: BackgroundTasks):
    """Manually trigger processing of all existing entries in scene and movie download watch folders."""
    s = db.get_settings()
    dl_dir = Path(s.get("download_watch_dir", ""))
    mdl_dir = Path((s.get("movie_download_watch_dir") or "").strip())
    fd = (s.get("features_dir") or "").strip()
    dest_movie = Path(fd).expanduser() if fd else None

    def collect_entries(base: Path) -> list[Path]:
        if not base.exists() or not str(base):
            return []
        return [e for e in base.iterdir() if e.name not in (".", "..")]

    scene_entries = collect_entries(dl_dir)
    movie_entries = collect_entries(mdl_dir)
    if not scene_entries and not movie_entries:
        return {"started": False, "message": "No entries found (scene or movie watch dirs missing or empty)"}

    def run():
        for entry in scene_entries:
            try:
                ep = entry.resolve()
                dest_dir, derr = _dl_resolve_import_dest_dir(
                    s, "", str(ep.parent), str(ep),
                )
                if derr or not dest_dir:
                    emit(f"  SKIP {entry.name}: {derr}")
                    continue
                _process_download_entry(entry, dest_dir)
            except Exception as e:
                emit(f"  ERROR processing {entry.name}: {e}")
        if movie_entries:
            if not fd or not dest_movie or not dest_movie.is_dir():
                for entry in movie_entries:
                    emit(f"  SKIP {entry.name}: Movies Library Directory not set or not found")
            else:
                for entry in movie_entries:
                    try:
                        _process_download_entry(entry, dest_movie)
                    except Exception as e:
                        emit(f"  ERROR processing movie watch {entry.name}: {e}")

    background_tasks.add_task(run)
    total = len(scene_entries) + len(movie_entries)
    return {"started": True, "count": total}


@app.get("/api/scan/status")
async def scan_status():
    """Return pending scan info for all media servers."""
    now = time.time()
    with _scan_lock:
        pending = {
            server: max(0, int(fire_at - now))
            for server, fire_at in _scan_pending.items()
            if fire_at > now
        }
    return {"pending": pending}


@app.get("/api/watcher/status")
async def watcher_status():
    s = db.get_settings()
    with _pending_lock:
        pending = [{"filename": f, "fires_in": max(0, int(t - time.time()))}
                   for f, t in _pending_files.items()]
    with _download_lock:
        dl_pending = [{"path": p, "fires_in": max(0, int(t - time.time()))}
                      for p, t in _pending_downloads.items()]
        md_pending = [{"path": p, "fires_in": max(0, int(t - time.time()))}
                      for p, t in _pending_movie_downloads.items()]
    dl_dir = Path(s.get("download_watch_dir", ""))
    mdl_dir = Path((s.get("movie_download_watch_dir") or "").strip())
    return {
        "enabled":             s.get("folder_watch_enabled", "true").lower() == "true",
        "watching":            observer is not None and observer.is_alive() if observer else False,
        "hold_secs":           int(s.get("folder_watch_hold_secs", "60")),
        "pending":             pending,
        "download_enabled":    s.get("download_watch_enabled", "false").lower() == "true",
        "download_watching":   download_observer is not None and download_observer.is_alive() if download_observer else False,
        "download_dir":        str(dl_dir),
        "download_dir_exists": dl_dir.exists(),
        "movie_download_dir":        str(mdl_dir),
        "movie_download_dir_exists": mdl_dir.exists() if str(mdl_dir) else False,
        "download_hold_secs":  int(s.get("download_watch_hold_secs", "300")),
        "download_pending":    dl_pending + md_pending,
    }


@app.get("/scenes", response_class=HTMLResponse)
async def scenes_page():
    with open("static/scenes.html") as f:
        return f.read()


@app.get("/metadata", response_class=HTMLResponse)
async def metadata_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/scenes", status_code=302)


@app.get("/api/library/check")
async def library_check(name: str):
    """Check if a performer/studio name exists in any library directory."""
    s    = db.get_settings()
    norm = normalise(name)
    dirs = [
        s.get("series_dir",   ""),
        s.get("features_dir", ""),
    ] + [d["path"] for d in db.get_directories()]

    for dir_path in dirs:
        if not dir_path:
            continue
        base = Path(dir_path)
        if not base.exists():
            continue
        for folder in base.iterdir():
            if folder.is_dir() and normalise(folder.name) == norm:
                return {"found": True, "path": str(folder), "dir": str(base)}
    return {"found": False}


@app.get("/api/metadata/search")
async def metadata_search(
    q: str,
    type: str = "performer",
    genders: str = "",
    strict: str = "",
):
    if not q.strip():
        return JSONResponse({"error": "Query required"}, status_code=400)
    if type == "studio":
        results = search_studios(q.strip())
    else:
        allow: frozenset[str] | None = None
        if genders.strip():
            parts = [
                x.strip()
                for x in genders.split(",")
                if x.strip() in ALLOWED_GENDER_BUCKETS
            ]
            if parts:
                allow = frozenset(parts)
        # Default strict (exact normalised name): Stash/TPDB/Fans search is fuzzy; disable with strict=0 (e.g. Scenes browse).
        s = (strict or "").strip().lower()
        strict_name = s not in ("0", "false", "no", "off")
        results = search_performers(
            q.strip(),
            gender_allowlist=allow,
            strict_name_match=strict_name,
        )
    return {"results": results}


@app.post("/api/metadata/create")
async def metadata_create(payload: dict):
    """
    Fetch full metadata and create tvshow.nfo + poster in the chosen directory.
    Accepts: type (performer|studio), source (TPDB|FansDB), id, dest_dir
    """
    mtype   = payload.get("type", "performer")
    source  = payload.get("source", "TPDB")
    mid     = payload.get("id", "")
    dest    = payload.get("dest_dir", "")

    if not all([mid, dest]):
        return JSONResponse({"error": "id and dest_dir required"}, status_code=400)

    dest_path = Path(dest)
    if not dest_path.exists():
        return JSONResponse({"error": f"Directory not found: {dest}"}, status_code=404)

    if mtype == "performer":
        data = fetch_performer_detail(source, mid)
        if not data:
            return JSONResponse({"error": "Failed to fetch performer data"}, status_code=500)
        name       = data.get("name", "Unknown")
        nfo        = build_performer_tvshow_nfo(data)
        posters    = data.get("posters") or []
        poster_url = posters[0].get("url") if posters and isinstance(posters[0], dict) else (posters[0] if posters else None)
        folder = create_tvshow_folder(name, dest_path, nfo, poster_url)
        has_poster = poster_url is not None
    else:
        data = fetch_studio_detail(source, mid)
        if not data:
            return JSONResponse({"error": "Failed to fetch studio data"}, status_code=500)
        name      = data.get("name") or data.get("title", "Unknown")
        nfo       = build_studio_tvshow_nfo(data)
        logo_url = _tpdb_scalar_media_url(data.get("logo"))
        poster_url = _tpdb_scalar_media_url(data.get("poster"))
        if not logo_url and not poster_url:
            poster_url = _tpdb_site_image_url(data)
        folder = create_tvshow_folder(
            name, dest_path, nfo, poster_url, logo_url=logo_url
        )
        has_poster = bool(logo_url or poster_url)

    return {
        "success":    True,
        "name":       name,
        "folder":     str(folder),
        "has_poster": has_poster,
    }


@app.get("/api/scenes/recent")
async def scenes_recent(source: str, id: str, type: str = "performer", slug: str = "", name: str = ""):
    """Get recent scenes for a performer or studio from all sources.
    Returns scenes grouped by source: {tpdb: [...], stashdb: [...], fansdb: [...]}"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    entity_type = type
    performer_name = name
    settings = db.get_settings()

    def _fetch_tpdb_scenes():
        """Fetch scenes from TPDB API."""
        try:
            # If source is TPDB, use the ID directly; otherwise search by name
            tpdb_id = id if source == "TPDB" else None
            tpdb_slug = slug if source == "TPDB" else None

            if not tpdb_id and performer_name:
                if entity_type == "performer":
                    r = requests.get(TPDB_PERFORMER_SEARCH, params={"q": performer_name},
                                     headers=_tpdb_headers(), timeout=10)
                else:
                    r = requests.get(TPDB_STUDIO_SEARCH, params={"q": performer_name},
                                     headers=_tpdb_headers(), timeout=10)
                if r.status_code == 200:
                    data = r.json().get("data") or []
                    if data:
                        tpdb_id = str(data[0].get("id") or data[0].get("_id") or "")
                        tpdb_slug = data[0].get("slug") or data[0].get("_id") or tpdb_id

            if not tpdb_id:
                return []

            lookups = []
            if tpdb_slug and tpdb_slug != tpdb_id:
                lookups.append(tpdb_slug)
            lookups.append(tpdb_id)

            for lookup in lookups:
                url = f"https://api.theporndb.net/{'performers' if entity_type == 'performer' else 'sites'}/{lookup}/scenes"
                resp = requests.get(url, params={"page": 1, "per_page": 20},
                                    headers=_tpdb_headers(), timeout=15)
                if resp.status_code != 200:
                    continue
                data_raw = resp.json().get("data")
                if not isinstance(data_raw, list):
                    continue
                out = []
                for s in data_raw:
                    genders = _extract_movie_genders(s)
                    if not _passes_content_filter(genders):
                        continue
                    if not _passes_tag_filter(s):
                        continue
                    thumb = s.get("poster_image") or s.get("image") or s.get("poster")
                    if not thumb:
                        posters = s.get("posters") or []
                        if posters:
                            thumb = posters[0] if isinstance(posters[0], str) else posters[0].get("url")
                    out.append({
                        "id":     str(s.get("_id") or s.get("id") or ""),
                        "title":  s.get("title", ""),
                        "date":   s.get("date", ""),
                        "studio": (s.get("site") or {}).get("name", ""),
                        "thumb":  thumb,
                    })
                return out
        except Exception:
            pass
        return []

    def _fetch_stashbox_scenes(endpoint: str, api_key: str):
        """Fetch scenes from a stash-box instance (StashDB or FansDB)."""
        if not api_key:
            return []
        try:
            search_name = performer_name or name
            if not search_name:
                return []

            # First find the performer ID on this stash-box
            search_gql = """
            query($term: String!) {
              searchPerformer(term: $term, limit: 1) { id name }
            }
            """
            r = requests.post(endpoint,
                json={"query": search_gql, "variables": {"term": search_name}},
                headers={"ApiKey": api_key, "Content-Type": "application/json"},
                timeout=10)
            if r.status_code != 200:
                return []
            performers = (r.json().get("data") or {}).get("searchPerformer") or []
            if not performers:
                return []
            perf_id = performers[0]["id"]

            # Now query scenes for this performer
            scenes_gql = """
            query($input: SceneQueryInput!) {
              queryScenes(input: $input) {
                scenes {
                  id title release_date
                  studio { name }
                  images { url width height }
                }
              }
            }
            """
            variables = {
                "input": {
                    "performers": {
                        "value": [perf_id],
                        "modifier": "INCLUDES"
                    },
                    "per_page": 20,
                    "page": 1,
                    "sort": "DATE",
                    "direction": "DESC"
                }
            }
            r2 = requests.post(endpoint,
                json={"query": scenes_gql, "variables": variables},
                headers={"ApiKey": api_key, "Content-Type": "application/json"},
                timeout=15)
            if r2.status_code != 200:
                return []
            scenes_data = (r2.json().get("data") or {}).get("queryScenes", {}).get("scenes") or []
            out = []
            for s in scenes_data:
                imgs = s.get("images") or []
                thumb = imgs[0].get("url") if imgs and isinstance(imgs[0], dict) else None
                out.append({
                    "id":     s.get("id", ""),
                    "title":  s.get("title", ""),
                    "date":   s.get("release_date", ""),
                    "studio": (s.get("studio") or {}).get("name", ""),
                    "thumb":  thumb,
                })
            return out
        except Exception:
            pass
        return []

    # Fetch from all sources in parallel
    result = {"tpdb": [], "stashdb": [], "fansdb": []}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {}
        futures["tpdb"] = pool.submit(_fetch_tpdb_scenes)
        if settings.get("api_key_stashdb"):
            futures["stashdb"] = pool.submit(_fetch_stashbox_scenes, STASHDB_ENDPOINT, settings["api_key_stashdb"])
        if settings.get("api_key_fansdb"):
            futures["fansdb"] = pool.submit(_fetch_stashbox_scenes, FANSDB_ENDPOINT, settings["api_key_fansdb"])
        for key, future in futures.items():
            try:
                result[key] = future.result()
            except Exception:
                pass

    # For backward compat, also return flat "scenes" from first non-empty source
    scenes = result["tpdb"] or result["stashdb"] or result["fansdb"]
    return {"scenes": scenes, "sources": result}


# ---------------------------------------------------------------------------
# TPDB Feed & Favourites Sync
# ---------------------------------------------------------------------------

_feed_cache: dict = {
    "recent": {"scenes": [], "ts": 0, "source": "", "filter_key": None},
    "random": {"scenes": [], "ts": 0, "source": "", "filter_key": None},
    "favourites": {"scenes": [], "ts": 0, "source": "", "filter_key": None},
}
_movies_latest_cache: dict = {"results": [], "ts": 0, "filter_key": None}
_FEED_CACHE_TTL = 3600  # 1 hour
_MOVIES_CACHE_TTL = 3600  # 1 hour


def _movies_cache_filter_key() -> tuple:
    """Fingerprint for latest-movies cache (categories + tag blacklist)."""
    return _content_filters_fingerprint()


def _fetch_performer_scenes(name: str, headers: dict) -> list[dict]:
    """Fetch recent scenes for a single performer from TPDB. Used in thread pool."""
    scenes = []
    try:
        resp = requests.get(
            TPDB_PERFORMER_SEARCH,
            params={"q": name},
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        perf_data = resp.json().get("data") or []
        if not perf_data:
            return []

        perf = perf_data[0]
        perf_id = perf.get("id") or perf.get("_id")
        if not perf_id:
            return []

        resp2 = requests.get(
            f"https://api.theporndb.net/performers/{perf_id}/scenes",
            headers=headers,
            timeout=10,
        )
        if resp2.status_code != 200:
            return []

        for sc in (resp2.json().get("data") or [])[:4]:
            genders = _extract_movie_genders(sc)
            if not _passes_content_filter(genders):
                continue
            if not _passes_tag_filter(sc):
                continue
            thumb = ""
            bg = sc.get("background") or {}
            if isinstance(bg, dict):
                thumb = bg.get("small") or bg.get("medium") or bg.get("url") or ""
            if not thumb:
                thumb = sc.get("poster_image") or sc.get("image") or sc.get("poster") or ""
            if not thumb:
                posters = sc.get("posters") or []
                if posters:
                    thumb = posters[0] if isinstance(posters[0], str) else posters[0].get("url", "")

            scenes.append({
                "id": str(sc.get("_id") or sc.get("id") or ""),
                "title": sc.get("title", ""),
                "date": sc.get("date", ""),
                "studio": (sc.get("site") or {}).get("name", ""),
                "thumb": thumb,
                "link": "",
                "source": "feed",
                "performer": name,
            })
    except Exception as e:
        emit(f"FEED error fetching {name}: {e}")
    return scenes


def _tpdb_scene_rows_from_api_payload(scenes_data: list, *, label: str, slice_n: int) -> list[dict]:
    """Turn raw TPDB /scenes list into feed cards (filters + thumb)."""
    out = []
    for sc in (scenes_data or [])[:slice_n]:
        genders = _extract_movie_genders(sc)
        if not _passes_content_filter(genders):
            continue
        if not _passes_tag_filter(sc):
            continue
        thumb = ""
        bg = sc.get("background") or {}
        if isinstance(bg, dict):
            thumb = bg.get("small") or bg.get("medium") or bg.get("url") or ""
        if not thumb:
            thumb = sc.get("poster_image") or sc.get("image") or sc.get("poster") or ""
        if not thumb:
            posters = sc.get("posters") or []
            if posters:
                thumb = posters[0] if isinstance(posters[0], str) else posters[0].get("url", "")
        out.append({
            "id": str(sc.get("_id") or sc.get("id") or ""),
            "title": sc.get("title", ""),
            "date": sc.get("date", ""),
            "studio": (sc.get("site") or {}).get("name", ""),
            "thumb": thumb,
            "link": "",
            "source": "feed",
            "performer": label,
        })
    return out


def _fetch_tpdb_performer_scenes_by_id(perf_id: str, label: str, headers: dict, slice_n: int = 6) -> list[dict]:
    """Recent TPDB scenes for a performer when we already have the TPDB id."""
    if not (perf_id or "").strip():
        return []
    try:
        resp2 = requests.get(
            f"https://api.theporndb.net/performers/{perf_id.strip()}/scenes",
            headers=headers,
            timeout=12,
        )
        if resp2.status_code != 200:
            return []
        return _tpdb_scene_rows_from_api_payload(
            resp2.json().get("data") or [], label=label, slice_n=slice_n,
        )
    except Exception as e:
        emit(f"FEED error performer id {perf_id}: {e}")
        return []


def _fetch_tpdb_site_scenes_by_id(site_id: str, label: str, headers: dict, slice_n: int = 6) -> list[dict]:
    """Recent TPDB scenes for a site/studio when we already have the TPDB id."""
    if not (site_id or "").strip():
        return []
    try:
        resp2 = requests.get(
            f"https://api.theporndb.net/sites/{site_id.strip()}/scenes",
            headers=headers,
            timeout=12,
        )
        if resp2.status_code != 200:
            return []
        return _tpdb_scene_rows_from_api_payload(
            resp2.json().get("data") or [], label=label, slice_n=slice_n,
        )
    except Exception as e:
        emit(f"FEED error site id {site_id}: {e}")
        return []


def _fetch_tpdb_starred_favourites_feed(limit: int = 24) -> list[dict]:
    """Scenes featuring heart-starred Favourites rows that have a TPDB link."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    s = db.get_settings()
    if not (s.get("api_key_tpdb") or "").strip():
        return []

    rows = db.favourite_starred_with_tpdb_ids()
    if not rows:
        emit("FEED starred favourites: no starred entities with TPDB id")
        return []

    headers = _tpdb_headers()
    # Cap fan-out so the feed stays snappy
    cap = min(len(rows), 12)
    rows = rows[:cap]
    slice_n = max(3, min(8, (limit + cap - 1) // cap))

    all_scenes: list[dict] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for r in rows:
            tid = (r.get("match_tpdb_id") or "").strip()
            label = (r.get("folder_name") or "").strip() or tid
            kind = (r.get("kind") or "").strip().lower()
            if kind == "studio":
                futures.append(pool.submit(_fetch_tpdb_site_scenes_by_id, tid, label, headers, slice_n))
            else:
                futures.append(pool.submit(_fetch_tpdb_performer_scenes_by_id, tid, label, headers, slice_n))
        for fut in as_completed(futures):
            try:
                all_scenes.extend(fut.result())
            except Exception:
                pass

    seen: set[str] = set()
    deduped: list[dict] = []
    for sc in all_scenes:
        sid = sc.get("id") or ""
        if sid and sid not in seen:
            seen.add(sid)
            deduped.append(sc)
    deduped.sort(key=lambda x: x.get("date", ""), reverse=True)
    emit(f"FEED starred favourites: {len(deduped)} scenes from {len(rows)} entities")
    return deduped[:limit]


def _fetch_tpdb_feed(limit: int = 24) -> list[dict]:
    """Build a personalised feed from library performer directories.

    Uses a thread pool for parallel API calls and caches results for 15 min.
    """
    import random
    from concurrent.futures import ThreadPoolExecutor, as_completed

    s = db.get_settings()
    api_key = s.get("api_key_tpdb", "")
    if not api_key:
        return []

    # Gather performer names from library directories
    performer_dirs = db.get_directories("performer")
    performer_names = []
    for entry in performer_dirs:
        base = Path(entry["path"])
        if not base.is_dir():
            continue
        for folder in base.iterdir():
            if folder.is_dir() and not folder.name.startswith("."):
                performer_names.append(folder.name)

    if not performer_names:
        emit("FEED no performer folders found in library")
        return []

    # Sample up to 8 performers
    sample = random.sample(performer_names, min(8, len(performer_names)))
    emit(f"FEED fetching scenes for {len(sample)} performers (parallel)...")

    headers = _tpdb_headers()
    all_scenes = []

    # Parallel fetch — up to 4 concurrent requests
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch_performer_scenes, name, headers): name for name in sample}
        for future in as_completed(futures):
            result = future.result()
            all_scenes.extend(result)

    # Deduplicate by id, sort by date descending
    seen = set()
    deduped = []
    for sc in all_scenes:
        if sc["id"] and sc["id"] not in seen:
            seen.add(sc["id"])
            deduped.append(sc)

    deduped.sort(key=lambda x: x.get("date", ""), reverse=True)
    result = deduped[:limit]

    emit(f"FEED built {len(result)} scenes from {len(sample)} performers")
    return result


def _tpdb_scene_payload_to_feed_card(s_item: dict, source: str = "api") -> dict | None:
    """Single scene resource → feed card, or None if filtered out."""
    genders = _extract_movie_genders(s_item)
    if not _passes_content_filter(genders):
        return None
    if not _passes_tag_filter(s_item):
        return None
    thumb = s_item.get("poster_image") or s_item.get("image") or s_item.get("poster") or ""
    if not thumb:
        posters = s_item.get("posters") or []
        if posters:
            thumb = posters[0] if isinstance(posters[0], str) else posters[0].get("url", "")
        bg = s_item.get("background") or {}
        if not thumb and isinstance(bg, dict):
            thumb = bg.get("small") or bg.get("medium") or bg.get("url") or ""
    return {
        "id": str(s_item.get("_id") or s_item.get("id") or ""),
        "title": s_item.get("title", ""),
        "date": s_item.get("date", ""),
        "studio": (s_item.get("site") or {}).get("name", ""),
        "thumb": thumb,
        "link": "",
        "source": source,
    }


def _tpdb_atom_feed_scene_ids(xml_text: str) -> list[str]:
    """Parse TPDB Atom feeds; return scene UUIDs in document order."""
    import re
    uuid_re = re.compile(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        re.I,
    )
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall("atom:entry", ns) or root.findall("entry")
    out: list[str] = []
    for entry in entries:
        sid = None
        for link in list(entry.findall("atom:link", ns)) + list(entry.findall("link")):
            href = (link.get("href") or "").strip()
            m = uuid_re.search(href)
            if m:
                sid = m.group(1).lower()
                break
        if not sid:
            id_el = entry.find("atom:id", ns) or entry.find("id")
            if id_el is not None and (id_el.text or "").strip():
                m = uuid_re.search(id_el.text)
                if m:
                    sid = m.group(1).lower()
        if sid:
            out.append(sid)
    return out


def _fetch_tpdb_feed_from_favorites_rss(limit: int = 24) -> list[dict]:
    """Scenes from TPDB Atom feeds for your favourite performers/sites (same account as API key)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not (db.get_settings().get("api_key_tpdb") or "").strip():
        return []

    headers = _tpdb_headers()
    feed_urls = (
        "https://theporndb.net/feeds/scenes/recently-added-by-favorite-performers",
        "https://theporndb.net/feeds/scenes/recently-added-by-favorite-sites",
    )
    seen: set[str] = set()
    ordered_ids: list[str] = []
    for url in feed_urls:
        try:
            resp = requests.get(url, headers=headers, timeout=22)
            if resp.status_code != 200:
                continue
            for sid in _tpdb_atom_feed_scene_ids(resp.text):
                if sid not in seen:
                    seen.add(sid)
                    ordered_ids.append(sid)
        except Exception as e:
            emit(f"FEED RSS fetch error ({url}): {e}")

    if not ordered_ids:
        emit("FEED RSS: no entries (add favourite performers/sites on ThePornDB, or check API key)")
        return []

    take = ordered_ids[: max(limit * 2, limit)]

    def _one(sid: str) -> tuple[str, dict | None]:
        try:
            resp = requests.get(
                f"https://api.theporndb.net/scenes/{sid}",
                headers=_tpdb_headers(),
                timeout=14,
            )
            if resp.status_code != 200:
                return sid, None
            data = resp.json().get("data") or {}
            card = _tpdb_scene_payload_to_feed_card(data, source="rss_favorites")
            return sid, card
        except Exception:
            return sid, None

    by_id: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_one, sid) for sid in take]
        for fut in as_completed(futures):
            try:
                sid, card = fut.result()
                if card:
                    by_id[sid] = card
            except Exception:
                pass

    ordered = [by_id[sid] for sid in take if sid in by_id]
    ordered.sort(key=lambda x: x.get("date", ""), reverse=True)
    result = ordered[:limit]
    emit(f"FEED RSS favourites: {len(result)} scenes (from {len(ordered_ids)} feed ids)")
    return result


def _fetch_tpdb_feed_from_api(limit: int = 24) -> list[dict]:
    """Fallback: fetch latest scenes from TPDB API (not personalised)."""
    try:
        resp = requests.get(
            "https://api.theporndb.net/scenes",
            params={"page": 1, "per_page": limit},
            headers=_tpdb_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json().get("data") or []
        out = []
        for s_item in data:
            card = _tpdb_scene_payload_to_feed_card(s_item, source="api")
            if card:
                out.append(card)
        return out
    except Exception:
        return []


def _fetch_tpdb_recent_feed_scenes(limit: int = 24) -> tuple[list[dict], str]:
    """Recent tab: TPDB favourite RSS → library performer sample → global API."""
    scenes = _fetch_tpdb_feed_from_favorites_rss(limit)
    if scenes:
        return scenes, "rss_favorites"
    scenes = _fetch_tpdb_feed(limit)
    if scenes:
        return scenes, "feed"
    scenes = _fetch_tpdb_feed_from_api(limit)
    return scenes, "api"


def _refresh_hourly_content_cache():
    """Background refresh: all three TPDB scene feed modes + movies latest cache."""
    try:
        fk = _content_filters_fingerprint()
        now = time.time()

        recent_scenes, recent_src = _fetch_tpdb_recent_feed_scenes(24)
        scenes_rand = _fetch_tpdb_feed(24)
        source_rand = "feed"
        if not scenes_rand:
            scenes_rand = _fetch_tpdb_feed_from_api(24)
            source_rand = "api"
        scenes_fav = _fetch_tpdb_starred_favourites_feed(24)
        source_fav = "favourites" if scenes_fav else "favourites_empty"

        with _content_cache_lock:
            _feed_cache["recent"] = {
                "scenes": recent_scenes,
                "ts": now,
                "source": recent_src,
                "filter_key": fk,
            }
            _feed_cache["random"] = {
                "scenes": scenes_rand,
                "ts": now,
                "source": source_rand,
                "filter_key": fk,
            }
            _feed_cache["favourites"] = {
                "scenes": scenes_fav,
                "ts": now,
                "source": source_fav,
                "filter_key": fk,
            }

        movies_payload = _collect_filtered_tpdb_movies({}, page=1, page_size=20)
        with _content_cache_lock:
            _movies_latest_cache["results"] = movies_payload.get("results", [])
            _movies_latest_cache["ts"] = time.time()
            _movies_latest_cache["filter_key"] = _movies_cache_filter_key()
        emit(
            f"CACHE hourly refresh: recent={len(recent_scenes)} random={len(scenes_rand)} "
            f"favourites={len(scenes_fav)} movies={len(_movies_latest_cache['results'])}"
        )
    except Exception as e:
        emit(f"CACHE hourly refresh error: {e}")


def _sync_library_to_tpdb_favourites() -> dict:
    """Sync library performer + studio directories → TPDB favourites.

    For each performer/studio folder in the library, search TPDB for a match
    and add them to the user's TPDB favourites.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    s = db.get_settings()
    api_key = s.get("api_key_tpdb", "")
    if not api_key:
        return {"error": "No TPDB API key configured"}

    headers = _tpdb_headers()

    # Gather performer names from all performer directories
    performer_dirs = db.get_directories("performer")
    performer_names = []
    for entry in performer_dirs:
        base = Path(entry["path"])
        if not base.is_dir():
            continue
        for folder in base.iterdir():
            if folder.is_dir() and not folder.name.startswith("."):
                performer_names.append(folder.name)

    # Gather studio names from series directory
    series_dir = s.get("series_dir", "").strip()
    studio_names = []
    if series_dir and Path(series_dir).is_dir():
        for folder in Path(series_dir).iterdir():
            if folder.is_dir() and not folder.name.startswith("."):
                studio_names.append(folder.name)

    if not performer_names and not studio_names:
        return {"performers": {"added": 0, "skipped": 0, "failed": 0},
                "studios": {"added": 0, "skipped": 0, "failed": 0},
                "message": "No library folders found"}

    emit(f"SYNC→FAVS syncing {len(performer_names)} performers + {len(studio_names)} studios")

    def _add_performer(name: str) -> str:
        try:
            resp = requests.get(TPDB_PERFORMER_SEARCH, params={"q": name},
                                headers=headers, timeout=10)
            if resp.status_code != 200:
                return "failed"
            perf_data = resp.json().get("data") or []
            if not perf_data:
                return "failed"
            perf_id = perf_data[0].get("id") or perf_data[0].get("_id")
            if not perf_id:
                return "failed"
            fav_resp = requests.post(
                f"https://api.theporndb.net/performers/{perf_id}/favorite",
                headers=headers, timeout=10)
            return "added" if fav_resp.status_code in (200, 201) else "skipped"
        except Exception:
            return "failed"

    def _add_studio(name: str) -> str:
        try:
            resp = requests.get(TPDB_STUDIO_SEARCH, params={"q": name},
                                headers=headers, timeout=10)
            if resp.status_code != 200:
                return "failed"
            site_data = resp.json().get("data") or []
            if not site_data:
                return "failed"
            site_id = site_data[0].get("id") or site_data[0].get("_id")
            if not site_id:
                return "failed"
            fav_resp = requests.post(
                f"https://api.theporndb.net/sites/{site_id}/favorite",
                headers=headers, timeout=10)
            return "added" if fav_resp.status_code in (200, 201) else "skipped"
        except Exception:
            return "failed"

    def _count_results(futures_dict):
        added = skipped = failed = 0
        for future in as_completed(futures_dict):
            r = future.result()
            if r == "added": added += 1
            elif r == "skipped": skipped += 1
            else: failed += 1
        return {"added": added, "skipped": skipped, "failed": failed}

    with ThreadPoolExecutor(max_workers=4) as pool:
        perf_futures = {pool.submit(_add_performer, n): n for n in performer_names}
        studio_futures = {pool.submit(_add_studio, n): n for n in studio_names}
        perf_result = _count_results(perf_futures)
        studio_result = _count_results(studio_futures)

    emit(f"SYNC→FAVS performers: {perf_result} | studios: {studio_result}")
    return {"performers": perf_result, "studios": studio_result}


def sync_tpdb_favourites() -> dict:
    """Sync TPDB favourites with library directories — create missing TVShow folders."""
    s = db.get_settings()
    if s.get("tpdb_sync_enabled", "false") != "true":
        return {"skipped": True}

    api_key = s.get("api_key_tpdb", "")
    if not api_key:
        return {"error": "No TPDB API key configured"}

    performer_dir = s.get("tpdb_sync_performer_dir", "").strip()
    studio_dir = s.get("tpdb_sync_studio_dir", "").strip()
    created = []

    # Fetch favourited performers via RSS
    try:
        resp = requests.get(
            "https://theporndb.net/feeds/scenes/recently-added-by-favorite-performers",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=20,
        )
        if resp.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            # Extract unique performer names from feed entries
            performer_names = set()
            for entry in root.findall("atom:entry", ns) or root.findall("entry"):
                cat_els = entry.findall("atom:category", ns) or entry.findall("category")
                for cat in cat_els:
                    term = cat.get("term", "")
                    if term:
                        performer_names.add(term)

            if performer_dir and Path(performer_dir).is_dir():
                existing = {d.name.lower() for d in Path(performer_dir).iterdir() if d.is_dir()}
                for name in performer_names:
                    if name.lower() not in existing:
                        folder = Path(performer_dir) / name
                        try:
                            folder.mkdir(parents=True, exist_ok=True)
                            # Create basic tvshow.nfo
                            nfo = build_performer_tvshow_nfo({"name": name})
                            (folder / "tvshow.nfo").write_text(nfo, encoding="utf-8")
                            created.append(str(folder))
                            emit(f"SYNC created performer folder: {folder}")
                        except Exception as e:
                            emit(f"SYNC error creating {folder}: {e}")
    except Exception as e:
        emit(f"SYNC performer feed error: {e}")

    # Also sync library → TPDB favourites if enabled
    to_favs_result = {}
    if s.get("tpdb_sync_to_favs", "false") == "true":
        to_favs_result = _sync_library_to_tpdb_favourites()

    return {"created": created, "count": len(created), "to_favs": to_favs_result}


@app.get("/api/scenes/feed")
async def scenes_feed(mode: str = "recent"):
    """TPDB scenes feed: ``recent`` (favourite RSS → library sample → global API), ``random`` (library folder sample + API fallback), or ``favourites`` (starred + TPDB id)."""
    fk = _content_filters_fingerprint()
    m = (mode or "recent").strip().lower()
    if m == "library":
        m = "random"
    if m not in ("recent", "random", "favourites"):
        m = "recent"

    with _content_cache_lock:
        bucket = _feed_cache.get(m) or {}
        age = time.time() - float(bucket.get("ts") or 0)
        scenes_cached = bucket.get("scenes") or []
        if (
            scenes_cached
            and age < _FEED_CACHE_TTL
            and bucket.get("filter_key") == fk
        ):
            return {
                "scenes": scenes_cached,
                "source": bucket.get("source", ""),
                "feed_mode": m,
                "cached": True,
                "cache_age_s": int(age),
            }

    if m == "favourites":
        scenes = _fetch_tpdb_starred_favourites_feed(24)
        source = "favourites" if scenes else ""
        if not scenes:
            source = "favourites_empty"
    elif m == "recent":
        scenes, source = _fetch_tpdb_recent_feed_scenes(24)
    else:
        scenes = _fetch_tpdb_feed(24)
        source = "feed"
        if not scenes:
            scenes = _fetch_tpdb_feed_from_api(24)
            source = "api"

    with _content_cache_lock:
        _feed_cache[m] = {
            "scenes": scenes,
            "ts": time.time(),
            "source": source,
            "filter_key": fk,
        }
    return {
        "scenes": scenes,
        "source": source,
        "feed_mode": m,
    }


@app.post("/api/tpdb/sync")
async def tpdb_sync_endpoint():
    """Manually trigger TPDB favourites sync."""
    result = sync_tpdb_favourites()
    return result


@app.get("/api/prowlarr/search")
async def prowlarr_search_endpoint(q: str):
    if not q.strip():
        return JSONResponse({"error": "Query required"}, status_code=400)
    # Search Prowlarr indexers directly
    results = prowlarr_search(q.strip())
    return {"results": results}


@app.post("/api/prowlarr/grab")
async def prowlarr_grab_endpoint(payload: dict):
    guid         = payload.get("guid", "")
    indexer_id   = payload.get("indexer_id")
    is_torrent   = payload.get("type", "torrent") == "torrent"
    download_url = payload.get("download_url", "")
    if not guid and not download_url:
        return JSONResponse({"error": "guid or download_url required"}, status_code=400)

    s = db.get_settings()
    kind = str(payload.get("kind") or payload.get("content") or "").strip().lower()
    base_cat = (s.get("prowlarr_category") or "Top-Shelf").strip() or "Top-Shelf"
    if kind in ("movie", "movies"):
        movie_cat = (s.get("prowlarr_category_movies") or "").strip()
        category = movie_cat or base_cat
    else:
        category = base_cat

    # Determine client settings — new dl_ fields, with legacy nzbget_ fallback
    nzb_client  = s.get("dl_nzb_client", "").lower().strip()
    nzb_host    = s.get("dl_nzb_host", "").strip()
    nzb_port    = s.get("dl_nzb_port", "").strip()
    nzb_user    = s.get("dl_nzb_user", "").strip()
    nzb_pass    = s.get("dl_nzb_pass", "")
    nzb_apikey  = s.get("dl_nzb_api_key", "").strip()

    # Legacy fallback for existing NZBGet settings
    if not nzb_client and s.get("nzbget_url", "").strip():
        nzb_client = "nzbget"
        legacy_url = s.get("nzbget_url", "").strip().rstrip("/")
        m = re.match(r'https?://([^:/]+)(?::(\d+))?', legacy_url)
        if m:
            nzb_host = m.group(1)
            nzb_port = m.group(2) or "6789"
        nzb_user = s.get("nzbget_user", "").strip()
        nzb_pass = s.get("nzbget_pass", "")

    torrent_client = s.get("dl_torrent_client", "").lower().strip()
    torrent_host   = s.get("dl_torrent_host", "").strip()
    torrent_port   = s.get("dl_torrent_port", "").strip()
    torrent_user   = s.get("dl_torrent_user", "").strip()
    torrent_pass   = s.get("dl_torrent_pass", "")

    # Helper: fetch torrent URL, handling magnet redirects
    def _fetch_torrent(url):
        """Returns (magnet_url, torrent_bytes) — one will be set."""
        r = requests.get(url, timeout=20, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location", "")
            if loc.startswith("magnet:"):
                emit("GRAB download link redirected to magnet")
                return loc, None
            r = requests.get(loc, timeout=20, allow_redirects=True)
        if r.status_code == 200:
            return None, r.content
        raise Exception(f"Failed to fetch torrent (HTTP {r.status_code})")

    # ---- NZBs ----
    if download_url and not is_torrent:
        if not nzb_client or not nzb_host:
            return {"error": "No NZB download client configured — add one in Settings"}
        try:
            r = requests.get(download_url, timeout=20, allow_redirects=True)
            emit(f"GRAB nzb fetch → {r.status_code}")
            if r.status_code != 200:
                return {"error": f"Failed to fetch NZB file (HTTP {r.status_code})"}

            import base64 as b64
            nzb_content  = r.content
            nzb_filename = (download_url.split("file=")[-1] if "file=" in download_url else "release") + ".nzb"

            if nzb_client == "nzbget":
                port = nzb_port or "6789"
                emit(f"GRAB NZBGet → {nzb_host}:{port} user={nzb_user!r}")
                auth = (nzb_user, nzb_pass) if nzb_user else None
                gr = requests.post(
                    f"http://{nzb_host}:{port}/jsonrpc",
                    json={"method": "append", "params": [
                        nzb_filename, b64.b64encode(nzb_content).decode(),
                        category, 0, False, False, "", 0, "SCORE"
                    ]},
                    auth=auth, timeout=15,
                )
                emit(f"GRAB NZBGet → {gr.status_code} {gr.text[:120]}")
                if gr.status_code == 200:
                    result = gr.json().get("result")
                    if result and result > 0:
                        return {"ok": True}
                    return {"error": f"NZBGet rejected the file (result={result})"}
                return {"error": f"NZBGet HTTP {gr.status_code} — check credentials in Settings"}

            elif nzb_client == "sabnzbd":
                port = nzb_port or "8080"
                emit(f"GRAB SABnzbd → {nzb_host}:{port}")
                gr = requests.post(
                    f"http://{nzb_host}:{port}/sabnzbd/api",
                    data={"mode": "addfile", "apikey": nzb_apikey, "cat": category, "output": "json"},
                    files={"nzbfile": (nzb_filename, nzb_content)},
                    timeout=15,
                )
                emit(f"GRAB SABnzbd → {gr.status_code} {gr.text[:120]}")
                if gr.status_code == 200:
                    return {"ok": True}
                return {"error": f"SABnzbd HTTP {gr.status_code}"}

        except Exception as e:
            emit(f"GRAB nzb error: {e}")
            return {"error": f"NZB download failed: {e}"}

    # ---- Torrents ----
    if download_url and is_torrent:
        if not torrent_client or not torrent_host:
            return {"error": "No torrent download client configured — add one in Settings"}
        is_magnet = download_url.startswith("magnet:")
        try:
            if torrent_client == "qbittorrent":
                port = torrent_port or "8080"
                qb_base = f"http://{torrent_host}:{port}"
                emit(f"GRAB qBittorrent → {torrent_host}:{port} user={torrent_user!r}")
                sess = requests.Session()
                # qBittorrent 4.6.1+ requires Referer/Origin for CSRF protection
                sess.headers.update({
                    "Referer": f"{qb_base}/",
                    "Origin": qb_base,
                })
                login_r = sess.post(
                    f"{qb_base}/api/v2/auth/login",
                    data={"username": torrent_user, "password": torrent_pass}, timeout=10,
                )
                emit(f"GRAB qBittorrent login → {login_r.status_code} {login_r.text[:60]}")
                if login_r.status_code == 403:
                    return {"error": "qBittorrent returned 403 — check Web UI is enabled, and host IP is not banned. Try restarting qBittorrent."}
                if login_r.text.strip() == "Fails.":
                    return {"error": "qBittorrent login failed — wrong username or password in Settings"}
                if is_magnet:
                    gr = sess.post(
                        f"{qb_base}/api/v2/torrents/add",
                        data={"urls": download_url, "category": category}, timeout=15,
                    )
                else:
                    magnet, torrent_bytes = _fetch_torrent(download_url)
                    if magnet:
                        gr = sess.post(
                            f"{qb_base}/api/v2/torrents/add",
                            data={"urls": magnet, "category": category}, timeout=15,
                        )
                    else:
                        gr = sess.post(
                            f"{qb_base}/api/v2/torrents/add",
                            data={"category": category},
                            files={"torrents": ("release.torrent", torrent_bytes)}, timeout=15,
                        )
                emit(f"GRAB qBittorrent → {gr.status_code} {gr.text[:80]}")
                if gr.status_code == 200:
                    return {"ok": True}
                return {"error": f"qBittorrent HTTP {gr.status_code}"}

            elif torrent_client == "transmission":
                port = torrent_port or "9091"
                emit(f"GRAB Transmission → {torrent_host}:{port}")
                tsess = requests.Session()
                if torrent_user:
                    tsess.auth = (torrent_user, torrent_pass)
                try:
                    sr = tsess.get(f"http://{torrent_host}:{port}/transmission/rpc", timeout=10)
                except Exception:
                    sr = type('R', (), {'status_code': 0, 'headers': {}})()
                tsess.headers["X-Transmission-Session-Id"] = sr.headers.get("X-Transmission-Session-Id", "") if sr.status_code == 409 else ""
                if is_magnet:
                    args = {"filename": download_url}
                else:
                    magnet, torrent_bytes = _fetch_torrent(download_url)
                    if magnet:
                        args = {"filename": magnet}
                    else:
                        args = {"metainfo": base64.b64encode(torrent_bytes).decode()}
                gr = tsess.post(
                    f"http://{torrent_host}:{port}/transmission/rpc",
                    json={"method": "torrent-add", "arguments": args}, timeout=15,
                )
                emit(f"GRAB Transmission → {gr.status_code} {gr.text[:80]}")
                if gr.status_code == 200 and gr.json().get("result") == "success":
                    return {"ok": True}
                return {"error": f"Transmission: {gr.text[:120]}"}

            elif torrent_client == "deluge":
                port = torrent_port or "8112"
                emit(f"GRAB Deluge → {torrent_host}:{port}")
                dsess = requests.Session()
                dsess.post(
                    f"http://{torrent_host}:{port}/json",
                    json={"method": "auth.login", "params": [torrent_pass], "id": 1}, timeout=10,
                )
                if is_magnet:
                    gr = dsess.post(
                        f"http://{torrent_host}:{port}/json",
                        json={"method": "core.add_torrent_magnet", "params": [download_url, {}], "id": 2}, timeout=15,
                    )
                else:
                    magnet, torrent_bytes = _fetch_torrent(download_url)
                    if magnet:
                        gr = dsess.post(
                            f"http://{torrent_host}:{port}/json",
                            json={"method": "core.add_torrent_magnet", "params": [magnet, {}], "id": 2}, timeout=15,
                        )
                    else:
                        gr = dsess.post(
                            f"http://{torrent_host}:{port}/json",
                            json={"method": "core.add_torrent_file", "params": ["release.torrent", base64.b64encode(torrent_bytes).decode(), {}], "id": 2}, timeout=15,
                        )
                emit(f"GRAB Deluge → {gr.status_code} {gr.text[:80]}")
                if gr.status_code == 200 and gr.json().get("result"):
                    return {"ok": True}
                return {"error": f"Deluge: {gr.json().get('error', gr.text[:120])}"}

            else:
                return {"error": f"Unknown torrent client: {torrent_client}"}

        except Exception as e:
            emit(f"GRAB torrent error: {e}")
            return {"error": f"Torrent download failed: {e}"}

    client_type = "torrent client" if is_torrent else "NZB client"
    return {"error": f"No {client_type} configured — add one in Settings under Download Clients"}


@app.get("/api/prowlarr/indexers")
async def prowlarr_indexers_endpoint():
    """Get cached indexers, refreshing from Prowlarr if needed."""
    indexers = _get_indexers()
    return {"indexers": indexers, "count": len(indexers)}


@app.post("/api/prowlarr/indexers/refresh")
async def prowlarr_indexers_refresh():
    """Force refresh of indexer cache from Prowlarr."""
    indexers = _fetch_and_cache_indexers()
    return {"indexers": indexers, "count": len(indexers)}


@app.get("/api/prowlarr/clients")
async def prowlarr_clients_endpoint():
    return {"clients": prowlarr_get_clients()}


@app.get("/api/downloads")
async def api_downloads(category: str | None = None):
    """Active + recent NZB history and torrent list from Settings download clients."""
    return download_clients_combined_status(category)


@app.post("/api/downloads/import")
async def api_downloads_import(payload: dict = Body(...)):
    """Run download-folder-style processing on a completed client job (scene → scenes input, movie → movies input)."""
    dl_id = str(payload.get("id") or "").strip()
    if not dl_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    result = _download_import_by_id(dl_id)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/downloads/remove")
async def api_downloads_remove(payload: dict = Body(...)):
    """Remove a job from the download client (does not run import)."""
    dl_id = (payload.get("id") or "").strip()
    if not dl_id:
        return JSONResponse({"error": "id required"}, status_code=400)
    result = _download_remove_by_id(dl_id)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.get("/api/prowlarr/status")
async def prowlarr_status():
    s = db.get_settings()
    base = s.get("prowlarr_url", "").rstrip("/")
    if not base:
        return {"connected": False, "error": "Not configured"}
    try:
        resp = requests.get(f"{base}/api/v1/system/status",
                            headers=_prowlarr_headers(), timeout=8)
        resp.raise_for_status()
        d = resp.json()
        return {"connected": True, "version": d.get("version")}
    except Exception as e:
        return {"connected": False, "error": str(e)}


@app.get("/api/metadata/preview")
async def metadata_preview(type: str, source: str, id: str):
    """Fetch preview data (image, bio, meta, links) without creating any files."""
    if type == "performer":
        data = fetch_performer_detail(source, id)
        if not data:
            return {"image": None, "bio": "", "meta": "", "links": []}
        extras  = data.get("extras") or {}
        posters = data.get("posters") or []
        image   = posters[0].get("url") if posters and isinstance(posters[0], dict) else (posters[0] if posters else None)
        bio     = data.get("bio") or ""
        aliases = data.get("aliases") or []
        slug    = data.get("slug") or data.get("_id") or id
        meta_parts = [f"<span>{source}</span>"]
        if extras.get("birthday"):       meta_parts.append(f"Born: <span>{extras['birthday']}</span>")
        if extras.get("birthplace"):     meta_parts.append(f"From: <span>{extras['birthplace']}</span>")
        if extras.get("career_start_year"): meta_parts.append(f"Active: <span>{extras['career_start_year']}</span>")
        if extras.get("ethnicity"):      meta_parts.append(f"Ethnicity: <span>{extras['ethnicity']}</span>")
        if extras.get("measurements"):   meta_parts.append(f"Stats: <span>{extras['measurements']}</span>")
        if aliases: meta_parts.append(f"AKA: <span>{', '.join(aliases[:3])}</span>")

        # Build links — cross-reference all databases
        links = []
        performer_name = data.get("name") or ""
        settings = db.get_settings()

        # TPDB link
        if source == "TPDB":
            links.append({"label": "TPDB", "url": f"https://theporndb.net/performers/{slug}"})
        elif performer_name and settings.get("api_key_tpdb"):
            try:
                r = requests.get(TPDB_PERFORMER_SEARCH, params={"q": performer_name},
                                 headers=_tpdb_headers(), timeout=8)
                if r.status_code == 200:
                    pd = r.json().get("data") or []
                    if pd:
                        tslug = pd[0].get("slug") or pd[0].get("_id") or str(pd[0].get("id", ""))
                        links.append({"label": "TPDB", "url": f"https://theporndb.net/performers/{tslug}"})
            except Exception:
                pass

        # StashDB link + external URLs
        if settings.get("api_key_stashdb"):
            try:
                gql = 'query($t:String!){searchPerformer(term:$t,limit:1){id name urls{url type}}}'
                r = requests.post("https://stashdb.org/graphql",
                    json={"query": gql, "variables": {"t": performer_name}},
                    headers={"ApiKey": settings['api_key_stashdb'],
                             "Content-Type": "application/json"}, timeout=8)
                if r.status_code == 200:
                    sp = (r.json().get("data") or {}).get("searchPerformer") or []
                    if sp:
                        links.append({"label": "StashDB", "url": f"https://stashdb.org/performers/{sp[0]['id']}"})
                        # Add external URLs from StashDB performer profile
                        for u in (sp[0].get("urls") or []):
                            url = u.get("url", "")
                            if url:
                                label = _url_label(url)
                                # Don't duplicate links we already have
                                if not any(l["url"] == url for l in links):
                                    links.append({"label": label, "url": url})
            except Exception:
                pass

        # FansDB link
        if source == "FansDB":
            links.append({"label": "FansDB", "url": f"https://fansdb.cc/performers/{id}"})
        elif performer_name and settings.get("api_key_fansdb"):
            try:
                gql = "query($term:String!){searchPerformer(term:$term,limit:1){id name}}"
                data = _fansdb_gql(gql, {"term": performer_name})
                sp = (data or {}).get("searchPerformer") or []
                if sp:
                    links.append({"label": "FansDB", "url": f"https://fansdb.cc/performers/{sp[0]['id']}"})
            except Exception:
                pass

        # Pull external links from TPDB extras if available
        for link_item in (data.get("urls") or data.get("links") or []):
            if isinstance(link_item, str):
                links.append({"label": _url_label(link_item), "url": link_item})
            elif isinstance(link_item, dict):
                url = link_item.get("url") or link_item.get("link") or ""
                label = link_item.get("type") or link_item.get("label") or _url_label(url)
                if url:
                    links.append({"label": label, "url": url})

        return {"image": image, "bio": bio if bio else "", "meta": " &middot; ".join(meta_parts),
                "slug": slug, "links": links}
    else:
        data = fetch_studio_detail(source, id)
        if not data:
            return {"image": None, "bio": "", "meta": "", "links": []}
        image = data.get("logo") or data.get("poster")
        bio   = data.get("description") or data.get("bio") or ""
        slug  = data.get("slug") or data.get("_id") or id
        links = []
        if source == "TPDB":
            links.append({"label": "TPDB", "url": f"https://theporndb.net/sites/{slug}"})
        # Pull external links
        for link_item in (data.get("urls") or data.get("links") or data.get("url") and [data["url"]] or []):
            if isinstance(link_item, str):
                links.append({"label": _url_label(link_item), "url": link_item})
            elif isinstance(link_item, dict):
                url = link_item.get("url") or link_item.get("link") or ""
                label = link_item.get("type") or link_item.get("label") or _url_label(url)
                if url:
                    links.append({"label": label, "url": url})
        return {"image": image, "bio": bio if bio else "", "meta": f"<span>{source}</span>",
                "links": links}


def _url_label(url: str) -> str:
    """Extract a short label from a URL domain."""
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower().replace("www.", "")
        labels = {
            "twitter.com": "Twitter", "x.com": "Twitter",
            "instagram.com": "Instagram",
            "reddit.com": "Reddit",
            "onlyfans.com": "OnlyFans",
            "fansly.com": "Fansly",
            "pornhub.com": "Pornhub",
            "xvideos.com": "XVideos",
            "theporndb.net": "TPDB",
            "stashdb.org": "StashDB",
            "fansdb.cc": "FansDB",
            "babepedia.com": "Babepedia",
            "freeones.com": "FreeOnes",
            "iafd.com": "IAFD",
        }
        return labels.get(domain, domain.split(".")[0].title())
    except Exception:
        return "Link"


@app.get("/api/metadata/spotlight")
async def metadata_spotlight():
    """Return a random performer from the library as a spotlight for the detail panel."""
    import random as _random

    performer_dirs = db.get_directories("performer")
    performer_names = []
    for entry in performer_dirs:
        base = Path(entry["path"])
        if not base.is_dir():
            continue
        for folder in base.iterdir():
            if folder.is_dir() and not folder.name.startswith("."):
                performer_names.append(folder.name)

    if not performer_names:
        return {"found": False}

    name = _random.choice(performer_names)

    # Search TPDB for this performer
    try:
        resp = requests.get(
            TPDB_PERFORMER_SEARCH,
            params={"q": name},
            headers=_tpdb_headers(),
            timeout=10,
        )
        if resp.status_code != 200:
            return {"found": False}
        perf_data = resp.json().get("data") or []
        if not perf_data:
            return {"found": False}

        p = perf_data[0]
        perf_id = str(p.get("id") or p.get("_id") or "")
        posters = p.get("posters") or []
        image = posters[0] if posters and isinstance(posters[0], str) else (
            posters[0].get("url") if posters else None)

        # Fetch full detail for bio/extras
        detail = fetch_performer_detail("TPDB", perf_id)
        bio = ""
        meta_parts = [f"<span>TPDB</span>"]
        slug = ""
        if detail:
            bio = detail.get("bio") or ""
            slug = detail.get("slug") or ""
            extras = detail.get("extras") or {}
            aliases = detail.get("aliases") or []
            full_posters = detail.get("posters") or []
            if full_posters:
                image = full_posters[0].get("url") if isinstance(full_posters[0], dict) else full_posters[0]
            if extras.get("birthday"):       meta_parts.append(f"Born: <span>{extras['birthday']}</span>")
            if extras.get("birthplace"):     meta_parts.append(f"From: <span>{extras['birthplace']}</span>")
            if extras.get("career_start_year"): meta_parts.append(f"Active: <span>{extras['career_start_year']}</span>")
            if extras.get("ethnicity"):      meta_parts.append(f"Ethnicity: <span>{extras['ethnicity']}</span>")
            if extras.get("measurements"):   meta_parts.append(f"Stats: <span>{extras['measurements']}</span>")
            if aliases: meta_parts.append(f"AKA: <span>{', '.join(aliases[:3])}</span>")

        return {
            "found": True,
            "name": p.get("name", name),
            "id": perf_id,
            "slug": slug,
            "source": "TPDB",
            "image": image,
            "bio": bio,
            "meta": " &middot; ".join(meta_parts),
        }
    except Exception:
        return {"found": False}


@app.get("/api/metadata/dirs")
async def metadata_dirs():
    """Return all configured library directories for the destination picker."""
    s = db.get_settings()
    dirs = []
    for label, path in [
        ("Series",   s.get("series_dir",   "")),
        ("Features", s.get("features_dir", "")),
    ]:
        if path:
            dirs.append({"label": label, "path": path})
    for d in db.get_directories():
        dirs.append({"label": d["label"], "path": d["path"]})
    return {"dirs": dirs}


@app.get("/api/log/stream")
async def log_stream():
    async def event_generator() -> AsyncGenerator[str, None]:
        for line in processing_state["log"]:
            yield f"data: {json.dumps(line)}\n\n"
        while True:
            try:
                msg = await asyncio.wait_for(log_queue.get(), timeout=15.0)
                yield f"data: {json.dumps(msg)}\n\n"
            except asyncio.TimeoutError:
                yield 'data: {"ping":true}\n\n'
    return StreamingResponse(event_generator(), media_type="text/event-stream")
