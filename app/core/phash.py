import os
import math
import subprocess
import json
import threading
import imagehash
from pathlib import Path
from datetime import datetime
from io import BytesIO
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

import database as db  # noqa: F401  (kept for downstream signature parity)

SCREENSHOT_SIZE = 160
COLUMNS = 5
ROWS = 5

_PHASH_CONCURRENCY = 2
_phash_semaphore = threading.Semaphore(_PHASH_CONCURRENCY)


def get_video_duration(video_path: Path) -> float:
    """Return the runtime of *video_path* in seconds.

    Tries three strategies in order:

    1. ``format=duration`` — the container's stored duration. Fast and
       reliable for every common container (MP4/MKV/AVI/MOV/WMV).
    2. ``packet=pts_time`` over the full file — for containers that
       don't store duration in the format header.
    3. ``stream=duration`` — last-resort per-stream duration.

    AVI (and some old MP4s) emit ``N/A`` for packet pts_time; we
    skip any ``N/A`` / empty lines and fall through until something
    parses. If every strategy fails we raise ``ValueError`` so the
    caller can decide whether that's fatal (building a sprite can't
    proceed without a duration) or just log-and-continue.
    """
    def _first_float(text: str) -> float | None:
        for line in reversed(text.strip().split("\n")):
            s = line.strip()
            if not s or s.upper() == "N/A":
                continue
            try:
                return float(s)
            except ValueError:
                continue
        return None

    base = ["ffprobe", "-hide_banner", "-loglevel", "error", "-of",
            "compact=p=0:nk=1"]
    #: See ``get_sprite_screenshot`` — ``file:`` prefix forces ffprobe
    #: to treat the path literally so leading-dash, bracketed or
    #: non-ASCII filenames don't get rejected by the protocol layer.
    input_arg = f"file:{video_path}"
    try:
        res = subprocess.run(
            [*base, "-show_entries", "format=duration", input_arg],
            check=True, capture_output=True, text=True,
        )
        v = _first_float(res.stdout)
        if v is not None and v > 0:
            return v
    except subprocess.CalledProcessError:
        pass
    try:
        res = subprocess.run(
            [*base, "-show_entries", "packet=pts_time", input_arg],
            check=True, capture_output=True, text=True,
        )
        v = _first_float(res.stdout)
        if v is not None and v > 0:
            return v
    except subprocess.CalledProcessError:
        pass
    try:
        res = subprocess.run(
            [*base, "-select_streams", "v:0", "-show_entries", "stream=duration",
             input_arg],
            check=True, capture_output=True, text=True,
        )
        v = _first_float(res.stdout)
        if v is not None and v > 0:
            return v
    except subprocess.CalledProcessError:
        pass
    raise ValueError(f"could not determine duration for {video_path.name}")


def probe_video_stream_meta(
    video_path: Path,
) -> tuple[str | None, int | None, int | None]:
    """First video stream: codec name, width, height (via ffprobe)."""
    cmd = [
        "ffprobe", "-hide_banner", "-loglevel", "error",
        "-print_format", "json", "-show_streams", "-select_streams", "v:0",
        f"file:{video_path}",
    ]
    try:
        res = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
        data = json.loads(res.stdout or "{}")
        streams = data.get("streams") or []
        if not streams:
            return None, None, None
        s0 = streams[0]
        codec = (s0.get("codec_name") or "").strip() or None
        wi = s0.get("width")
        hi = s0.get("height")
        w = int(wi) if wi is not None else None
        h = int(hi) if hi is not None else None
        return codec, w, h
    except (subprocess.CalledProcessError, json.JSONDecodeError, ValueError, subprocess.TimeoutExpired):
        return None, None, None


def _file_stat_created_iso(st: os.stat_result) -> str:
    """Best-effort file creation / birth date for library UI (YYYY-MM-DD)."""
    ts = getattr(st, "st_birthtime", None)
    if ts is None:
        ts = st.st_mtime
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return ""


def _library_index_compute_media_probe_fields(
    row: dict, vpath: Path
) -> tuple[float, str | None, str | None, int | None, int | None] | None:
    """Stat + optional ffprobe; returns args for library_file_update_media_probe. None if stat fails."""
    try:
        st = vpath.stat()
    except OSError:
        return None
    mt = float(st.st_mtime)
    created_iso = _file_stat_created_iso(st) or None
    prev_mt = row.get("media_mtime")
    codec = row.get("media_codec")
    w = row.get("media_width")
    h = row.get("media_height")
    need_probe = True
    if prev_mt is not None:
        try:
            need_probe = abs(float(prev_mt) - mt) > 1e-6
        except (TypeError, ValueError):
            need_probe = True
    if need_probe:
        pc, pw, ph = probe_video_stream_meta(vpath)
        codec, w, h = pc, pw, ph
    else:
        if w is not None:
            try:
                w = int(w)
            except (TypeError, ValueError):
                w = None
        if h is not None:
            try:
                h = int(h)
            except (TypeError, ValueError):
                h = None
    return (mt, created_iso, codec, w, h)


def get_sprite_screenshot(video_path: Path, t: float) -> Image.Image:
    #: ``file:`` protocol prefix forces ffmpeg to treat the argument as
    #: a literal filesystem path instead of trying to parse it as an
    #: option or URL. Required for paths that contain a leading dash,
    #: square brackets, em-dashes, or non-ASCII characters (Japanese,
    #: Cyrillic, etc.) — without it ffmpeg returns rc=183 with no
    #: useful stderr because the protocol layer rejects the input
    #: before the demuxer ever opens it.
    input_arg = f"file:{video_path}"
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error",
           "-ss", str(t), "-i", input_arg,
           "-frames:v", "1", "-vf", f"scale={SCREENSHOT_SIZE}:{-2}",
           "-c:v", "bmp", "-f", "image2", "-"]
    try:
        res = subprocess.run(cmd, check=True, capture_output=True, timeout=60)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"ffmpeg rc={exc.returncode} at t={t:.2f}s for {video_path.name}"
            + (f": {stderr[:240]}" if stderr else "")
        ) from None
    if not res.stdout:
        raise RuntimeError(f"ffmpeg returned no frame at t={t:.2f}s for {video_path.name}")
    img = Image.open(BytesIO(res.stdout))
    img.load()
    return img


def build_sprite(video_path: Path, duration: float = None) -> Image.Image:
    """25 ffmpeg invocations with -ss before -i (input seeking) + Pillow montage, run
    concurrently (one thread per frame grab). Much faster on long files than a single
    decode with vf select=, which must scan/decode linearly to match timestamps."""
    if duration is None:
        duration = get_video_duration(video_path)
    offset = 0.05 * duration
    step = (0.9 * duration) / (COLUMNS * ROWS)
    timestamps = [offset + i * step for i in range(COLUMNS * ROWS)]

    def grab(t: float) -> Image.Image:
        return get_sprite_screenshot(video_path, t)

    images: list[Image.Image] | None = None
    try:
        with ThreadPoolExecutor(max_workers=COLUMNS * ROWS) as pool:
            images = list(pool.map(grab, timestamps))
    except RuntimeError as exc:
        # uvicorn reload / Ctrl-C mid-batch: futures pool refuses new tasks
        # for the rest of the process. Fall back to sequential.
        if "cannot schedule new futures" not in str(exc).lower():
            raise
        images = [grab(t) for t in timestamps]
    w, h = images[0].size
    montage = Image.new("RGB", (w * COLUMNS, h * ROWS))
    for i, img in enumerate(images):
        montage.paste(img, (w * (i % COLUMNS), h * math.floor(i / ROWS)))
    return montage


def compute_phash(video_path: Path, duration: float = None) -> str:
    with _phash_semaphore:
        return str(imagehash.phash(build_sprite(video_path, duration=duration)))
