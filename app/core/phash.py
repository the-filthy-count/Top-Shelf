import os
import math
import subprocess
import json
import threading
from pathlib import Path
from datetime import datetime
from io import BytesIO
import numpy as np
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


# ---------------------------------------------------------------------------
# Stash-compatible perceptual hash
#
# Ports the algorithm from stashapp/stash's pkg/hash/videophash + goimagehash
# so hex output matches (or comes within Hamming distance 1-2 of) what Stash
# would compute on the same file. Reference:
#   - github.com/stashapp/stash/blob/develop/pkg/hash/videophash/phash.go
#   - github.com/corona10/goimagehash/blob/master/hashcompute.go
#   - github.com/nfnt/resize (Bilinear)
#
# Pipeline:
#   1. Extract 25 frames via ffmpeg at offset + i*step, matching Stash's
#      ScreenshotTime (scale=W:-1, BMP over rawvideo pipe).
#   2. Tile into a 5×5 sprite (all frames same size, no gaps).
#   3. Bilinear resize the sprite to 64×64, matching nfnt/resize's integer
#      two-pass algorithm bit-for-bit (weights are int16(kernel * 256);
#      the intermediate transposed image is 8-bit RGBA).
#   4. Greyscale to float64 using goimagehash's Rgb2Gray formula
#      (0.299·R + 0.587·G + 0.114·B on 8-bit ints, no rounding).
#   5. 2D DCT-II unscaled via the recursive Lee-1984 algorithm goimagehash
#      uses; take the top-left 8×8 in row-major order.
#   6. Median of those 64 values via goimagehash's quickSelect (for even N
#      it averages sequence[k-1] with sequence[k=32] AFTER partitioning, so
#      sequence[k-1] is some value ≤ the true 32nd smallest, not the true
#      31st smallest — we replicate that exactly).
#   7. Pack 64 bits MSB-first (bit=1 if value > median) into a hex string.
# ---------------------------------------------------------------------------


def get_sprite_screenshot(video_path: Path, t: float) -> Image.Image:
    #: ``file:`` protocol prefix forces ffmpeg to treat the argument as
    #: a literal filesystem path instead of trying to parse it as an
    #: option or URL. Required for paths that contain a leading dash,
    #: square brackets, em-dashes, or non-ASCII characters — without
    #: it ffmpeg returns rc=183 with no useful stderr because the
    #: protocol layer rejects the input before the demuxer opens it.
    #:
    #: Args deliberately mirror stash's ScreenshotTime: ``-ss t`` before
    #: ``-i`` (fast/input seek to nearest keyframe), ``scale=W:-1`` (auto
    #: height keeping exact aspect — Stash uses -1, not -2), BMP codec
    #: piped over rawvideo. See ``pkg/ffmpeg/transcoder/screenshot.go``.
    input_arg = f"file:{video_path}"
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
           "-ss", str(t), "-i", input_arg,
           "-frames:v", "1", "-vf", f"scale={SCREENSHOT_SIZE}:-1",
           "-c:v", "bmp", "-f", "rawvideo", "-"]
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
    if img.mode != "RGB":
        img = img.convert("RGB")
    return img


def build_sprite(video_path: Path, duration: float = None) -> Image.Image:
    """25 ffmpeg invocations with -ss before -i (input seeking) + Pillow
    montage, run concurrently. Timestamps match Stash's ``generateSprite``:
    offset = 5% of duration, step = 90% / 25."""
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


# --- Bilinear resize matching nfnt/resize ----------------------------------
# nfnt uses integer weights (int16, kernel*256) and processes in two passes
# (horizontal then vertical); the intermediate is 8-bit clamped so we must
# preserve that quantisation to match its final output.


def _bilinear_weights(target_len: int, scale: float) -> tuple[np.ndarray, np.ndarray, int]:
    """
    Port of nfnt/resize's ``createWeights8`` for the bilinear kernel.
    Returns (coeffs, starts, filter_length):
      - coeffs: (target_len, filter_length) int16
      - starts: (target_len,) int32 — leftmost source index for each output pixel
      - filter_length: sample count per output pixel
    """
    blur = 1.0
    filter_length = 2 * max(int(math.ceil(blur * scale)), 1)
    filter_factor = min(1.0 / (blur * scale), 1.0)

    coeffs = np.zeros((target_len, filter_length), dtype=np.int16)
    starts = np.zeros(target_len, dtype=np.int32)
    for y in range(target_len):
        interp_x = scale * (y + 0.5) - 0.5
        # Go's `int(interp_x)` truncates toward zero. interp_x is always
        # >= -0.5 here (scale>0, y>=0), so we can end up at -0. For any
        # interp_x >= 0, int() == floor(). For interp_x in (-0.5, 0),
        # int() == 0 while floor() == -1 — matches Go.
        start = int(interp_x) - filter_length // 2 + 1
        starts[y] = start
        interp_x -= start
        for i in range(filter_length):
            x = (interp_x - i) * filter_factor
            # linear kernel: max(0, 1 - |x|)
            ax = abs(x)
            kv = 1.0 - ax if ax <= 1.0 else 0.0
            # int16 truncation toward zero matches Go's int16() conversion
            coeffs[y, i] = int(kv * 256)
    return coeffs, starts, filter_length


def _resize_axis_uint8(img: np.ndarray, target_len: int, scale: float, axis: int) -> np.ndarray:
    """
    Apply nfnt's 1-D bilinear filter along ``axis`` of an uint8 image with
    shape (..., src_len, ..., 3). Returns uint8. Edge clamp: samples past
    the source's last valid index are pinned to it (matching nfnt's
    ``case xi >= maxX`` branch).
    """
    src_len = img.shape[axis]
    max_x = src_len - 1
    coeffs, starts, filter_length = _bilinear_weights(target_len, scale)

    # Precompute gather indices for every (out_y, i) sample: clamp to [0, max_x].
    # Shape: (target_len, filter_length)
    idx = np.clip(starts[:, None] + np.arange(filter_length)[None, :], 0, max_x)

    # Gather along the specified axis. np.take(img, idx, axis=axis) gives
    # shape (..., target_len, filter_length, ...) — indices broadcast into
    # the axis position.
    gathered = np.take(img, idx.reshape(-1), axis=axis).astype(np.int64)
    # Reshape the flattened axis back to (target_len, filter_length)
    new_shape = list(gathered.shape)
    new_shape[axis:axis+1] = [target_len, filter_length]
    gathered = gathered.reshape(new_shape)  # (..., target_len, filter_length, ...)

    # Broadcast coeffs over the (target_len, filter_length) axes.
    # Build a broadcast shape: ones everywhere except the (target_len, filter_length) dims.
    c_shape = [1] * gathered.ndim
    c_shape[axis] = target_len
    c_shape[axis + 1] = filter_length
    c = coeffs.reshape(c_shape).astype(np.int64)

    weighted = gathered * c
    # Sum along the filter_length axis
    pixel_sum = weighted.sum(axis=axis + 1)  # (..., target_len, ..., 3)
    coeff_sum = c.sum(axis=axis + 1)         # (..., target_len, ..., 3) — broadcast

    # Integer divide then clamp to [0, 255]. Go's clampUint8 uses signed int
    # then unsigned check; for our non-negative sums the result is just clamp.
    # Guard against zero coeff_sum (can happen if all weights were zero for
    # a row — extremely unlikely with taps>=2 and non-zero scale).
    out = np.where(coeff_sum > 0, pixel_sum // coeff_sum, 0)
    out = np.clip(out, 0, 255).astype(np.uint8)
    return out


def _bilinear_resize_nfnt(src_rgb: np.ndarray, target_w: int, target_h: int) -> np.ndarray:
    """Bilinear resize matching nfnt/resize (Bilinear). Input/output are
    uint8 (H, W, 3) fully-opaque RGB arrays. Horizontal pass first (with
    8-bit intermediate quantisation), then vertical."""
    src_h, src_w = src_rgb.shape[:2]
    scale_x = src_w / target_w
    scale_y = src_h / target_h
    temp = _resize_axis_uint8(src_rgb, target_w, scale_x, axis=1)  # (src_h, target_w, 3)
    out = _resize_axis_uint8(temp, target_h, scale_y, axis=0)      # (target_h, target_w, 3)
    return out


# --- 1-D DCT-II (Lee 1984, recursive, unscaled) ----------------------------
# Port of goimagehash's forwardDCT64 (transforms/static.go). The cosine
# tables are precomputed once at module import. Scale factor: the output is
# half of scipy's dct(type=2, norm=None) but that constant cancels out in
# the median comparison downstream, so it doesn't affect the hash bits.

def _lee_dct_table(n: int) -> np.ndarray:
    """cos((i + 0.5) * pi / n) * 2 for i in [0, n/2). Matches goimagehash's
    dct{N} tables."""
    return np.array(
        [math.cos((i + 0.5) * math.pi / n) * 2.0 for i in range(n // 2)],
        dtype=np.float64,
    )


_LEE_DCT_TABLES = {n: _lee_dct_table(n) for n in (2, 4, 8, 16, 32, 64)}


def _lee_forward_dct(x: np.ndarray) -> None:
    """In-place DCT-II via Lee's recursive butterfly. x is a 1-D float64
    array whose length is a power of two ≥ 2. Directly ports
    ``forwardDCT{N}`` from goimagehash transforms/static.go."""
    n = x.shape[0]
    if n == 1:
        return
    half = n // 2
    table = _LEE_DCT_TABLES[n]
    temp = np.empty(n, dtype=np.float64)
    # Even/odd split with cosine scaling for the odd half.
    for i in range(half):
        a = x[i]
        b = x[n - 1 - i]
        temp[i] = a + b
        temp[i + half] = (a - b) / table[i]
    _lee_forward_dct(temp[:half])
    _lee_forward_dct(temp[half:])
    # Interleave, adding neighbour odd-half samples on the odd outputs.
    for i in range(half - 1):
        x[i * 2] = temp[i]
        x[i * 2 + 1] = temp[i + half] + temp[i + half + 1]
    x[n - 2] = temp[half - 1]
    x[n - 1] = temp[n - 1]


def _dct_2d_top_left_8x8(gs: np.ndarray) -> np.ndarray:
    """2-D DCT-II of a 64×64 float64 grid, returning the top-left 8×8 block
    flattened in row-major order (64 float64 values). Mirrors goimagehash's
    ``DCT2DFast64``: row-wise pass on all 64 rows, then column-wise pass on
    only the first 8 columns."""
    if gs.shape != (64, 64):
        raise ValueError(f"gs must be 64x64, got {gs.shape}")
    buf = gs.astype(np.float64, copy=True)
    # Row-wise DCT (in-place)
    for row in range(64):
        _lee_forward_dct(buf[row])
    # Column-wise DCT on only the first 8 columns; keep the first 8 rows.
    flat = np.empty(64, dtype=np.float64)
    col = np.empty(64, dtype=np.float64)
    for i in range(8):
        col[:] = buf[:, i]
        _lee_forward_dct(col)
        for j in range(8):
            flat[j * 8 + i] = col[j]
    return flat


# --- Median: port of goimagehash's quickSelectMedian for length 64 ---------
# For even N the algorithm returns (sequence[k-1] + sequence[k]) / 2 where
# k = N/2 and the array has been partitioned around k. sequence[k-1] is NOT
# guaranteed to be the true (k-1)th smallest — it's whatever ended up there
# after partitioning. Replicating this exactly keeps our bits aligned with
# Stash even when the naive median would drift by 1 bit.


def _go_quickselect_median(seq: np.ndarray) -> float:
    """Port of ``etcs/utils.go``'s quickSelectMedian for an even-length
    array. Mutates the input array. Returns the "median" the same way Go
    would: half the true k-th smallest plus half of whatever element was
    left in sequence[k-1] after partitioning."""
    tmp = seq.astype(np.float64, copy=True)
    n = tmp.shape[0]
    k = n // 2
    low = 0
    hi = n - 1
    if low == hi:
        return float(tmp[k])
    while low < hi:
        pivot = low // 2 + hi // 2
        pivot_value = tmp[pivot]
        tmp[pivot], tmp[hi] = tmp[hi], tmp[pivot]
        store_idx = low
        for i in range(low, hi):
            if tmp[i] < pivot_value:
                tmp[store_idx], tmp[i] = tmp[i], tmp[store_idx]
                store_idx += 1
        tmp[hi], tmp[store_idx] = tmp[store_idx], tmp[hi]
        if k <= store_idx:
            hi = store_idx
        else:
            low = store_idx + 1
    if n % 2 == 0:
        return float(tmp[k - 1]) / 2 + float(tmp[k]) / 2
    return float(tmp[k])


def compute_phash(video_path: Path, duration: float = None) -> str:
    """Compute a 64-bit perceptual hash of a video, returning 16 hex chars.

    Algorithm ports Stash's ``videophash.Generate`` end-to-end so the hex
    output matches (or is within 1–2 Hamming bits of) what Stash would
    compute on the same file. See the module docstring above for the full
    pipeline reference.
    """
    with _phash_semaphore:
        sprite = build_sprite(video_path, duration=duration)
        # PIL RGB image → uint8 numpy (H, W, 3)
        sprite_arr = np.asarray(sprite, dtype=np.uint8)
        if sprite_arr.ndim != 3 or sprite_arr.shape[2] != 3:
            raise RuntimeError(
                f"sprite must be H×W×3 RGB, got shape {sprite_arr.shape}"
            )
        # Step 3: bilinear resize to 64×64 (nfnt-compatible)
        resized = _bilinear_resize_nfnt(sprite_arr, 64, 64)
        # Step 4: BT.601 luma on 8-bit ints, kept as float64 (no rounding)
        r = resized[:, :, 0].astype(np.float64)
        g = resized[:, :, 1].astype(np.float64)
        b = resized[:, :, 2].astype(np.float64)
        gs = 0.299 * r + 0.587 * g + 0.114 * b
        # Step 5: 2-D DCT-II, take top-left 8×8 (row-major flat)
        flat = _dct_2d_top_left_8x8(gs)
        # Step 6: median via Go's quickSelect
        median = _go_quickselect_median(flat)
        # Step 7: pack 64 bits, MSB first — bit set when value > median
        hash_val = 0
        for idx in range(64):
            if flat[idx] > median:
                hash_val |= 1 << (63 - idx)
        return f"{hash_val:016x}"
