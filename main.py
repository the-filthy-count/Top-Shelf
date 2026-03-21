"""
Top-Shelf - FastAPI backend

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8891 --reload
"""

import asyncio
import io
import json
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

import imagehash
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image

import database as db

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

app = FastAPI(title="Top-Shelf")
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
    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
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
# Directory matching (settings-driven)
# ---------------------------------------------------------------------------

def normalise(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9 ]", "", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def find_studio_dir(studio_name: str, settings: dict) -> Path | None:
    series_dir = Path(settings.get("series_dir", ""))
    if not series_dir.exists():
        return None
    norm = normalise(studio_name)
    for folder in (d.name for d in series_dir.iterdir() if d.is_dir()):
        if normalise(folder) == norm:
            return series_dir / folder
    return None


def find_performer_dir(performers: list, performer_dirs: list) -> tuple | None:
    female_names = [
        p["performer"]["name"] for p in performers
        if (p["performer"].get("gender") or "").upper() in ("FEMALE", "TRANSGENDER_FEMALE", "")
    ]
    if not female_names:
        female_names = [p["performer"]["name"] for p in performers]

    for entry in sorted(performer_dirs, key=lambda x: x["rank"]):
        base = Path(entry["path"])
        if not base.exists():
            continue
        norm_folders = {normalise(d.name): d.name for d in base.iterdir() if d.is_dir()}
        for name in female_names:
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
# API routes
# ---------------------------------------------------------------------------

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
    filed = {r["filename"] for r in db.get_history(limit=10000) if r["status"] == "filed"}
    files = []
    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
            files.append({
                "filename": f.name,
                "size_mb": round(f.stat().st_size / 1024 / 1024, 1),
                "has_phash": db.get_phash(f.name) is not None,
                "previously_filed": f.name in filed,
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
