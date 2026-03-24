"""
Top-Shelf - FastAPI backend

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8891 --reload
"""

import asyncio
import base64
import io
import json
import secrets
import math
import os
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import AsyncGenerator

import bcrypt
import imagehash
import requests
import threading
import time
from apscheduler.schedulers.background import BackgroundScheduler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image

import database as db

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "1.1.0"

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
download_observer = None  # download folder watchdog observer
_pending_files: dict     = {}  # filename -> scheduled time (scene watcher)
_pending_downloads: dict = {}  # path -> scheduled time (download watcher)
_pending_lock    = threading.Lock()
_download_lock   = threading.Lock()


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
            _pending_files[path.name] = time.time() + hold

    def on_moved(self, event):
        # Handle files moved/renamed into the folder
        self.on_created(type("E", (), {"is_directory": False, "src_path": event.dest_path})())


def _check_pending_files():
    """Called by scheduler every 30s - fires pipeline for files past their hold time."""
    if processing_state["running"]:
        return
    now = time.time()
    ready = []
    with _pending_lock:
        for fname, fire_at in list(_pending_files.items()):
            if now >= fire_at:
                ready.append(fname)
                del _pending_files[fname]
    if ready:
        s = db.get_settings()
        source_dir = Path(s.get("source_dir", ""))
        to_run = [f for f in ready if (source_dir / f).exists()]
        if to_run:
            run_pipeline(to_run)


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


def _restart_watcher():
    """Restart watcher with current settings - called after settings save."""
    global observer
    if observer:
        observer.stop()
        observer = None
    _start_watcher()

app = FastAPI(title="Top-Shelf")

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

COOKIE_NAME   = "ts_session"
LOGIN_PATH    = "/login"
PUBLIC_PATHS  = {"/login", "/api/auth/login", "/api/auth/logout"}


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
    scheduler.remove_all_jobs()
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


@app.on_event("startup")
async def startup():
    global log_queue
    log_queue = asyncio.Queue()
    db.init_db()
    _migrate_sidecar_phashes()
    _apply_retry_schedule()
    scheduler.add_job(_check_pending_files, "interval", seconds=30, id="pending_check")
    scheduler.add_job(db.purge_expired_sessions, "interval", hours=1, id="session_purge")
    scheduler.add_job(_check_pending_downloads, "interval", seconds=30, id="download_check")
    scheduler.start()
    _start_watcher()
    _start_download_watcher()


@app.on_event("shutdown")
async def shutdown():
    global observer, download_observer
    if observer:
        observer.stop()
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
    studio { name }
    performers { performer { name gender } }
    images { url width height }
  }
}
"""

TPDB_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [[FingerprintQueryInput]]!) {
  findScenesBySceneFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { name }
    performers { performer { name gender } }
    images { url width height }
  }
}
"""


SEARCH_SCENES_QUERY = """
query SearchScenes($term: String!, $limit: Int) {
  searchScene(term: $term, limit: $limit) {
    id title release_date
    studio { name }
    performers { performer { name gender } }
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
      studio { name }
      performers { performer { name gender } }
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
        ("ThePornDB", TPDB_ENDPOINT,    keys["tpdb"]),
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

    def _add(scenes, source_name):
        for scene in (scenes or []):
            sid = scene.get("id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                scene["_source"] = source_name
                all_results.append(scene)

    for source_name, endpoint, api_key in sources:
        if not api_key:
            continue
        try:
            if combined:
                _add(search_scenes_on_db(endpoint, api_key, combined), source_name)
            if date_from or date_to:
                _add(query_scenes_by_date(endpoint, api_key, combined,
                                          date_from=date_from, date_to=date_to),
                     source_name)
        except Exception:
            pass

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
        emit(f"  WARNING: StashDB failed ({e}), trying ThePornDB...")
    try:
        m = query_stashbox(phash_hex, TPDB_ENDPOINT, keys["tpdb"], TPDB_QUERY, "scene")
        if m: return m, "ThePornDB"
    except Exception as e:
        emit(f"  WARNING: ThePornDB failed ({e}), trying FansDB...")
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
    with _download_lock:
        for path_str, fire_at in list(_pending_downloads.items()):
            if now >= fire_at:
                ready.append(path_str)
                del _pending_downloads[path_str]
    if ready:
        s = db.get_settings()
        dest_dir = Path(s.get("source_dir", ""))
        if not dest_dir.exists():
            return
        for path_str in ready:
            entry = Path(path_str)
            if entry.exists():
                try:
                    _process_download_entry(entry, dest_dir)
                except Exception as e:
                    emit(f"  Download process error: {e}")


class DownloadWatchHandler(FileSystemEventHandler):
    """Watches the download folder and queues new entries after a hold period."""

    def _queue(self, path_str: str) -> None:
        s = db.get_settings()
        if s.get("download_watch_enabled", "false").lower() != "true":
            return
        hold = int(s.get("download_watch_hold_secs", "300"))
        with _download_lock:
            # Only queue the top-level entry (folder or file)
            entry = Path(path_str)
            dl_dir = Path(s.get("download_watch_dir", ""))
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


def _start_download_watcher() -> None:
    global download_observer
    s = db.get_settings()
    if s.get("download_watch_enabled", "false").lower() != "true":
        return
    dl_dir = Path(s.get("download_watch_dir", ""))
    if not dl_dir.exists():
        return
    if download_observer:
        download_observer.stop()
    download_observer = Observer()
    download_observer.schedule(DownloadWatchHandler(), str(dl_dir), recursive=True)
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
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9 ]", "", text.lower())
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
  searchStudio(term: $term) { id name }
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
    """Search StashDB for a studio by name. Returns list of {id, name}."""
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
    studio     = (scene.get("studio") or {}).get("name") or ""
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
        result = find_performer_dir(performers, performer_dirs)
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

    # Push metadata to local Stash and trigger media server scans
    phash_val = db.get_phash(filename)
    push_to_stash(scene, destination, source, phash=phash_val)
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


def file_movie(video: Path, movie: dict) -> dict:
    """File a movie using TMDB metadata."""
    filename = video.name
    db.upsert_movie(filename)
    settings = db.get_settings()
    features_dir = Path(settings.get("features_dir", ""))

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
    db.update_movie(filename, status="filed", tmdb_id=movie.get("id"),
                    title=title, year=year, overview=movie.get("overview"),
                    poster_url=movie.get("poster_url"), destination=destination)

    # Trigger media server scans
    trigger_media_scans(destination)

    emit("  DONE")
    return {"status": "filed", "destination": destination}


# ---------------------------------------------------------------------------
# Performer / Studio metadata scraper
# ---------------------------------------------------------------------------

TPDB_PERFORMER_SEARCH = "https://api.theporndb.net/performers"
TPDB_PERFORMER_DETAIL = "https://api.theporndb.net/performers/{id}"
TPDB_STUDIO_SEARCH    = "https://api.theporndb.net/sites"
TPDB_STUDIO_DETAIL    = "https://api.theporndb.net/sites/{id}"

FANSDB_PERFORMER_SEARCH_GQL = """
query($name: String!) {
  queryPerformers(input: { name: $name, page: 1, per_page: 10 }) {
    performers { id name }
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


def _fansdb_gql(query: str, variables: dict) -> dict:
    s = db.get_settings()
    resp = requests.post(
        FANSDB_ENDPOINT,
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {s.get('api_key_fansdb', '')}",
                 "Content-Type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", {})


def search_performers(name: str) -> list[dict]:
    results = []
    # ThePornDB
    try:
        resp = requests.get(TPDB_PERFORMER_SEARCH, params={"q": name},
                            headers=_tpdb_headers(), timeout=15)
        if resp.status_code == 200:
            for p in (resp.json().get("data") or [])[:5]:
                posters = p.get("posters") or []
                img = posters[0] if posters and isinstance(posters[0], str) else (posters[0].get("url") if posters else None)
                results.append({
                    "source": "TPDB",
                    "id":     str(p.get("id", "")),
                    "slug":   p.get("slug") or p.get("_id") or str(p.get("id", "")),
                    "name":   p["name"],
                    "image":  img,
                })
    except Exception:
        pass
    # FansDB
    try:
        data = _fansdb_gql(FANSDB_PERFORMER_SEARCH_GQL, {"name": name})
        for p in (data.get("queryPerformers") or {}).get("performers", []):
            results.append({"source": "FansDB", "id": str(p["id"]), "name": p["name"]})
    except Exception:
        pass
    return results


def search_studios(name: str) -> list[dict]:
    results = []
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
                    "image":  s.get("logo") or s.get("poster"),
                })
    except Exception:
        pass
    return results


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


def create_tvshow_folder(name: str, dest_dir: Path, nfo_content: str,
                          poster_url: str = None) -> Path:
    folder = dest_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    nfo_path = folder / "tvshow.nfo"
    nfo_path.write_text(nfo_content, encoding="utf-8")
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


def whisparr_search(query: str) -> list[dict]:
    """Search via Whisparr's release search endpoint - returns NZBs and torrents."""
    s    = db.get_settings()
    base = s.get("whisparr_url", "").rstrip("/")
    key  = s.get("whisparr_api_key", "")
    if not base or not key:
        return []
    try:
        resp = requests.get(
            f"{base}/api/v3/release",
            params={"term": query},
            headers={"X-Api-Key": key},
            timeout=30,
        )
        if resp.status_code != 200:
            return []
        results = resp.json()
        if not isinstance(results, list):
            return []
        out = []
        for r in results:
            protocol = r.get("protocol", "torrent").lower()
            out.append({
                "guid":         r.get("guid", ""),
                "title":        r.get("title", ""),
                "indexer":      r.get("indexer", ""),
                "size_mb":      round((r.get("size") or 0) / 1024 / 1024, 0),
                "seeders":      r.get("seeders"),
                "age":          r.get("ageHours"),
                "download_url": r.get("downloadUrl", ""),
                "magnet":       r.get("magnetUrl", ""),
                "protocol":     protocol,
                "type":         "torrent" if protocol == "torrent" else "nzb",
                "indexer_id":   r.get("indexerId"),
                "guid_whisparr": r.get("guid", ""),
            })
        nzbs     = sorted([r for r in out if r["type"] == "nzb"],
                          key=lambda x: x.get("age") or 0)
        torrents = sorted([r for r in out if r["type"] == "torrent"],
                          key=lambda x: x.get("seeders") or 0, reverse=True)
        return nzbs[:20] + torrents[:20]
    except Exception:
        return []


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
            guid      = (item.findtext("guid") or "").strip()
            size      = 0
            link      = (item.findtext("link") or "").strip()
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

            # Determine magnet: prefer explicit magneturl attr, else check link
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
    """Search all Prowlarr indexers via their Newznab proxy endpoints."""
    base    = _prowlarr_url()
    api_key = db.get_settings().get("prowlarr_api_key", "")
    if not base or not api_key:
        return []

    indexers = _get_indexers()
    if not indexers:
        return []

    all_results = []
    for indexer in indexers:
        results = _search_indexer(base, api_key, indexer, query)
        all_results.extend(results)

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
                "thumb":    ((s.get("posters") or [{}])[0]).get("url"),
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
                "thumb":    ((s.get("posters") or [{}])[0]).get("url"),
            } for s in scenes]
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(next: str = "/"):
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


@app.get("/api/auth/status")
async def auth_status(request: Request):
    return {
        "password_set":   db.get_password_hash() is not None,
        "session_hours":  db.get_session_hours(),
        "authenticated":  _is_authenticated(request),
    }


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
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


@app.get("/movies", response_class=HTMLResponse)
async def movies_page():
    with open("static/movies.html") as f:
        return f.read()


@app.get("/api/movies/search")
async def movies_search(q: str, year: str = None):
    if not q.strip():
        return JSONResponse({"error": "Query required"}, status_code=400)
    try:
        results = search_tmdb(q.strip(), year=year or None)
        return {"results": results}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/movies/detail/{tmdb_id}")
async def movies_detail(tmdb_id: str):
    try:
        movie = get_tmdb_movie(tmdb_id)
        return {"movie": movie}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/movies/queue")
async def movies_queue():
    settings = db.get_settings()
    source_dir = Path(settings.get("movies_source_dir", "") or settings.get("source_dir", ""))
    if not source_dir.exists():
        return {"files": [], "error": f"Source dir not found: {source_dir}"}
    filed = {r["filename"] for r in db.get_movie_history() if r["status"] == "filed"}
    files = []
    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
            files.append({
                "filename":        f.name,
                "size_mb":         round(f.stat().st_size / 1024 / 1024, 1),
                "previously_filed": f.name in filed,
            })
    return {"files": files}


@app.get("/api/movies/history")
async def movies_history():
    return {"history": db.get_movie_history()}


@app.get("/api/movies/stats")
async def movies_stats():
    return db.get_movie_stats()


@app.post("/api/movies/file")
async def file_movie_endpoint(payload: dict, background_tasks: BackgroundTasks):
    """File a video as a movie using TMDB metadata."""
    if processing_state["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)

    filename = payload.get("filename", "").strip()
    tmdb_id  = payload.get("tmdb_id", "").strip()

    if not filename or not tmdb_id:
        return JSONResponse({"error": "filename and tmdb_id required"}, status_code=400)

    settings = db.get_settings()
    source_dir = Path(settings.get("movies_source_dir", "") or settings.get("source_dir", ""))
    video = source_dir / filename

    if not video.exists():
        return JSONResponse({"error": f"File not found: {filename}"}, status_code=404)

    def run():
        processing_state["running"] = True
        processing_state["log"] = []
        processing_state["current_file"] = filename
        try:
            emit(f"FILE {filename}")
            emit(f"  Fetching TMDB details for ID {tmdb_id}...")
            movie = get_tmdb_movie(tmdb_id)
            file_movie(video, movie)
        except Exception as e:
            emit(f"  ERROR: {e}")
            db.update_movie(filename, status="error", error=str(e))
        finally:
            processing_state["running"] = False
            processing_state["current_file"] = None
            emit("---")

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
    """Manually trigger processing of all existing entries in the download watch folder."""
    s = db.get_settings()
    dl_dir = Path(s.get("download_watch_dir", ""))
    dest_dir = Path(s.get("source_dir", ""))
    if not dl_dir.exists():
        return JSONResponse({"error": f"Download watch dir not found: {dl_dir}"}, status_code=404)
    if not dest_dir.exists():
        return JSONResponse({"error": f"Source dir not found: {dest_dir}"}, status_code=404)

    entries = [e for e in dl_dir.iterdir() if e.name not in ('.', '..')]
    if not entries:
        return {"started": False, "message": "No entries found"}

    def run():
        for entry in entries:
            try:
                _process_download_entry(entry, dest_dir)
            except Exception as e:
                emit(f"  ERROR processing {entry.name}: {e}")

    background_tasks.add_task(run)
    return {"started": True, "count": len(entries)}


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
    dl_dir = Path(s.get("download_watch_dir", ""))
    return {
        "enabled":             s.get("folder_watch_enabled", "true").lower() == "true",
        "watching":            observer is not None and observer.is_alive() if observer else False,
        "hold_secs":           int(s.get("folder_watch_hold_secs", "60")),
        "pending":             pending,
        "download_enabled":    s.get("download_watch_enabled", "false").lower() == "true",
        "download_watching":   download_observer is not None and download_observer.is_alive() if download_observer else False,
        "download_dir":        str(dl_dir),
        "download_dir_exists": dl_dir.exists(),
        "download_hold_secs":  int(s.get("download_watch_hold_secs", "300")),
        "download_pending":    dl_pending,
    }


@app.get("/metadata", response_class=HTMLResponse)
async def metadata_page():
    with open("static/metadata.html") as f:
        return f.read()


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
async def metadata_search(q: str, type: str = "performer"):
    if not q.strip():
        return JSONResponse({"error": "Query required"}, status_code=400)
    if type == "studio":
        results = search_studios(q.strip())
    else:
        results = search_performers(q.strip())
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
    else:
        data = fetch_studio_detail(source, mid)
        if not data:
            return JSONResponse({"error": "Failed to fetch studio data"}, status_code=500)
        name       = data.get("name") or data.get("title", "Unknown")
        nfo        = build_studio_tvshow_nfo(data)
        poster_url = data.get("logo") or data.get("poster")

    folder = create_tvshow_folder(name, dest_path, nfo, poster_url)
    return {
        "success":    True,
        "name":       name,
        "folder":     str(folder),
        "has_poster": poster_url is not None,
    }


@app.get("/api/scenes/recent")
async def scenes_recent(source: str, id: str, type: str = "performer", slug: str = ""):
    """Get recent scenes for a performer or studio from TPDB."""
    if source != "TPDB":
        return {"scenes": [], "note": "Scene lookup only available for TPDB sources"}

    entity_type = type  # avoid shadowing builtin


    lookups = []
    if slug and slug != id:
        lookups.append(slug)
    lookups.append(id)

    for lookup in lookups:
        if entity_type == "performer":
            url = f"https://api.theporndb.net/performers/{lookup}/scenes"
        else:
            url = f"https://api.theporndb.net/sites/{lookup}/scenes"
        try:
            resp = requests.get(url, params={"page": 1, "per_page": 8},
                                headers=_tpdb_headers(), timeout=15)
            if resp.status_code != 200:
                continue
            payload  = resp.json()
            data_raw = payload.get("data")
            if not isinstance(data_raw, list):
                continue
            out = []
            for s in data_raw:
                # TPDB scenes: thumb is poster_image or image (strings), not posters list
                thumb = s.get("poster_image") or s.get("image") or s.get("poster")
                # posters may be list of strings
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
            return {"scenes": out}
        except Exception:
            pass
    return {"scenes": []}


@app.get("/api/prowlarr/search")
async def prowlarr_search_endpoint(q: str):
    if not q.strip():
        return JSONResponse({"error": "Query required"}, status_code=400)
    # Try Whisparr first (better NZB coverage), fall back to Prowlarr
    results = whisparr_search(q.strip())
    if not results:
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
    base = _prowlarr_url()
    category = s.get("prowlarr_category", "Top-Shelf")

    # ---- NZBs: fetch file, push to NZBGet or SABnzbd ----
    if download_url and not is_torrent:
        try:
            r = requests.get(download_url, timeout=20, allow_redirects=True)
            emit(f"GRAB nzb fetch → {r.status_code}")
            if r.status_code == 200 and base:
                import base64 as b64
                nzb_content  = r.content
                nzb_filename = (download_url.split("file=")[-1] if "file=" in download_url else "release") + ".nzb"
                nzb_pref     = s.get("prowlarr_nzb_client", "")
                cr = requests.get(f"{base}/api/v1/downloadclient", headers=_prowlarr_headers(), timeout=10)
                emit(f"GRAB clients: {[c.get('name') for c in cr.json()]}")
                for c in cr.json():
                    if nzb_pref and c.get("name") != nzb_pref:
                        continue
                    impl   = c.get("implementation", "").lower()
                    # Skip non-NZB clients (qBittorrent, Transmission, etc.)
                    if "nzbget" not in impl and "sabnzbd" not in impl and "nzbvortex" not in impl:
                        continue
                    fields = {f["name"]: f.get("value") for f in c.get("fields", [])}
                    host   = fields.get("host", "localhost")
                    port   = int(fields.get("port") or 6789)
                    emit(f"GRAB trying {impl} at {host}:{port}")
                    if "nzbget" in impl:
                        user = (fields.get("username") or fields.get("Username") or "")
                        pwd  = (fields.get("password") or fields.get("Password") or "")
                        emit(f"GRAB NZBGet user={user!r} pwd={'(set)' if pwd else '(empty)'}")
                        # Always try with credentials first, then without auth as fallback
                        auth_attempts = []
                        if user or pwd:
                            auth_attempts.append({"auth": (user, pwd)})
                        auth_attempts.append({"auth": None})
                        for auth_arg in auth_attempts:
                            gr = requests.post(
                                f"http://{host}:{port}/jsonrpc",
                                json={"method": "append", "params": [
                                    nzb_filename, b64.b64encode(nzb_content).decode(),
                                    category, 0, False, False, "", 0, "SCORE"
                                ]},
                                timeout=15, **auth_arg,
                            )
                            emit(f"GRAB NZBGet → {gr.status_code} {gr.text[:100]}")
                            if gr.status_code == 200:
                                result = gr.json().get("result")
                                if result and result > 0:
                                    return {"ok": True}
                                break
                            if gr.status_code == 401:
                                emit("GRAB NZBGet auth failed, trying next method")
                                continue
                            break
                    elif "sabnzbd" in impl:
                        api_key = fields.get("apiKey", "")
                        gr = requests.post(
                            f"http://{host}:{port}/sabnzbd/api",
                            data={"mode": "addfile", "apikey": api_key, "cat": category, "output": "json"},
                            files={"nzbfile": (nzb_filename, nzb_content)},
                            timeout=15,
                        )
                        emit(f"GRAB SABnzbd → {gr.status_code} {gr.text[:100]}")
                        if gr.status_code == 200:
                            return {"ok": True}
        except Exception as e:
            emit(f"GRAB nzb error: {e}")

    # ---- Torrents: magnet or .torrent file to qBittorrent ----
    if download_url and is_torrent:
        is_magnet = download_url.startswith("magnet:")
        try:
            torrent_pref = s.get("prowlarr_torrent_client", "")
            if base:
                cr = requests.get(f"{base}/api/v1/downloadclient", headers=_prowlarr_headers(), timeout=10)
                for c in cr.json():
                    if torrent_pref and c.get("name") != torrent_pref:
                        continue
                    impl   = c.get("implementation", "").lower()
                    # Skip non-torrent clients
                    if "torrent" not in impl and "qbittorrent" not in impl and "transmission" not in impl and "deluge" not in impl and "rtorrent" not in impl:
                        continue
                    fields = {f["name"]: f.get("value") for f in c.get("fields", [])}
                    host   = fields.get("host", "localhost")
                    port   = int(fields.get("port") or 8080)
                    emit(f"GRAB trying {impl} at {host}:{port}")
                    if "qbittorrent" in impl:
                        user = fields.get("username", "") or ""
                        pwd  = fields.get("password", "") or ""
                        sess = requests.Session()
                        sess.post(f"http://{host}:{port}/api/v2/auth/login",
                                  data={"username": user, "password": pwd}, timeout=10)
                        if is_magnet:
                            # Send magnet link directly
                            gr = sess.post(
                                f"http://{host}:{port}/api/v2/torrents/add",
                                data={"urls": download_url, "category": category},
                                timeout=15,
                            )
                        else:
                            # Fetch torrent file — handle redirects to magnet URIs
                            r = requests.get(download_url, timeout=20, allow_redirects=False)
                            if r.status_code in (301, 302, 303, 307, 308):
                                redirect_url = r.headers.get("Location", "")
                                if redirect_url.startswith("magnet:"):
                                    emit(f"GRAB torrent redirect → magnet, sending directly")
                                    gr = sess.post(
                                        f"http://{host}:{port}/api/v2/torrents/add",
                                        data={"urls": redirect_url, "category": category},
                                        timeout=15,
                                    )
                                else:
                                    r = requests.get(redirect_url, timeout=20, allow_redirects=True)
                                    emit(f"GRAB torrent fetch → {r.status_code}")
                                    gr = sess.post(
                                        f"http://{host}:{port}/api/v2/torrents/add",
                                        data={"category": category},
                                        files={"torrents": ("release.torrent", r.content)},
                                        timeout=15,
                                    )
                            elif r.status_code == 200:
                                emit(f"GRAB torrent fetch → {r.status_code}")
                                gr = sess.post(
                                    f"http://{host}:{port}/api/v2/torrents/add",
                                    data={"category": category},
                                    files={"torrents": ("release.torrent", r.content)},
                                    timeout=15,
                                )
                            else:
                                emit(f"GRAB torrent fetch failed → {r.status_code}")
                                continue
                        emit(f"GRAB qBittorrent → {gr.status_code} {gr.text[:80]}")
                        if gr.status_code == 200:
                            return {"ok": True}
        except Exception as e:
            emit(f"GRAB torrent error: {e}")

    # ---- Whisparr fallback ----
    whisparr_base = s.get("whisparr_url", "").rstrip("/")
    whisparr_key  = s.get("whisparr_api_key", "")
    if whisparr_base and whisparr_key and guid:
        try:
            resp = requests.post(
                f"{whisparr_base}/api/v3/release",
                json={"guid": guid, "indexerId": indexer_id or 0},
                headers={"X-Api-Key": whisparr_key},
                timeout=15,
            )
            if resp.status_code in (200, 201, 202, 204):
                return {"ok": True}
        except Exception:
            pass

    return {"error": "Could not send to download client - check NZBGet/qBittorrent credentials in Prowlarr"}


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
    """Fetch preview data (image, bio, meta) without creating any files."""
    if type == "performer":
        data = fetch_performer_detail(source, id)
        if not data:
            return {"image": None, "bio": "", "meta": ""}
        extras  = data.get("extras") or {}
        posters = data.get("posters") or []
        image   = posters[0].get("url") if posters and isinstance(posters[0], dict) else (posters[0] if posters else None)
        bio     = data.get("bio") or ""
        aliases = data.get("aliases") or []
        meta_parts = [f"<span>{source}</span>"]
        if extras.get("birthday"):       meta_parts.append(f"Born: <span>{extras['birthday']}</span>")
        if extras.get("birthplace"):     meta_parts.append(f"From: <span>{extras['birthplace']}</span>")
        if extras.get("career_start_year"): meta_parts.append(f"Active: <span>{extras['career_start_year']}</span>")
        if extras.get("ethnicity"):      meta_parts.append(f"Ethnicity: <span>{extras['ethnicity']}</span>")
        if extras.get("measurements"):   meta_parts.append(f"Stats: <span>{extras['measurements']}</span>")
        if aliases: meta_parts.append(f"AKA: <span>{', '.join(aliases[:3])}</span>")
        return {"image": image, "bio": bio[:500] if bio else "", "meta": " &middot; ".join(meta_parts),
                "slug": data.get("slug") or ""}
    else:
        data = fetch_studio_detail(source, id)
        if not data:
            return {"image": None, "bio": "", "meta": ""}
        image = data.get("logo") or data.get("poster")
        bio   = data.get("description") or data.get("bio") or ""
        return {"image": image, "bio": bio[:500] if bio else "", "meta": f"<span>{source}</span>"}


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
