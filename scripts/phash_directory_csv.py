#!/usr/bin/env python3
"""
Walk a directory tree and compute Stash-compatible perceptual hashes for every video file:
25 frames in a 5×5 sprite, then ``imagehash.phash`` — same as Top-Shelf ``main.build_sprite``.

**Default sprite mode (recommended):** 25 separate ``ffmpeg`` runs with ``-ss`` *before* ``-i``
(input seeking near each timestamp). This is **much faster on long videos** than a single
decode with ``vf select=...``, because ``select`` must walk the file sequentially to match
timestamps (often ~decoding the whole file).

Optional ``--one-ffmpeg`` uses one process with ``select`` + ``tile`` (fewer process starts,
but often slower on long content). Use ``--hwaccel cuda`` only with ``--one-ffmpeg`` (NVIDIA).

Also records file size, duration, codec / width / height, and file dates (one ``ffprobe`` call).

Dependencies::

    pip install Pillow imagehash

System: ``ffmpeg`` and ``ffprobe`` on your PATH.

Examples::

    python scripts/phash_directory_csv.py ./Videos -o phash_report.csv
    python scripts/phash_directory_csv.py ./Videos -o out.csv -j 4
    python scripts/phash_directory_csv.py ./Videos -o out.csv --resume
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import imagehash
from PIL import Image

VIDEO_EXTENSIONS = frozenset(
    {
        ".mp4",
        ".mkv",
        ".avi",
        ".wmv",
        ".mov",
        ".m4v",
        ".flv",
        ".webm",
        ".ts",
        ".m2ts",
        ".mpg",
        ".mpeg",
    }
)

SCREENSHOT_SIZE = 160
COLUMNS = 5
ROWS = 5

SKIP_DIR_NAMES = frozenset(
    {
        "#recycle",
        "@recycle",
        "$recycle.bin",
        "system volume information",
        "@eadir",
        ".snapshot",
        ".git",
    }
)

SPRITE_SEEKS = "seeks"
SPRITE_ONE_FFMPEG = "one_ffmpeg"


def get_video_duration_packet_fallback(video_path: Path) -> float:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-loglevel",
        "error",
        "-of",
        "compact=p=0:nk=1",
        "-show_entries",
        "packet=pts_time",
    ]
    try:
        res = subprocess.run(
            [*cmd, "-read_intervals", "9999999%+#1000", str(video_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        return float(res.stdout.strip().split("\n")[-1])
    except (subprocess.CalledProcessError, ValueError):
        res = subprocess.run(
            [*cmd, str(video_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        return float(res.stdout.strip().split("\n")[-1])


def ffprobe_format_and_video0(
    video_path: Path,
) -> tuple[float | None, str | None, int | None, int | None]:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-loglevel",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(video_path),
    ]
    try:
        res = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        data = json.loads(res.stdout or "{}")
    except (
        subprocess.CalledProcessError,
        json.JSONDecodeError,
        subprocess.TimeoutExpired,
    ):
        return None, None, None, None

    dur_raw = (data.get("format") or {}).get("duration")
    duration: float | None = None
    if dur_raw is not None and str(dur_raw).strip():
        try:
            duration = float(dur_raw)
        except ValueError:
            duration = None

    codec: str | None = None
    w: int | None = None
    h: int | None = None
    for s in data.get("streams") or []:
        if (s.get("codec_type") or "").lower() == "video":
            codec = ((s.get("codec_name") or "").strip()) or None
            wi = s.get("width")
            hi = s.get("height")
            try:
                w = int(wi) if wi is not None else None
            except (TypeError, ValueError):
                w = None
            try:
                h = int(hi) if hi is not None else None
            except (TypeError, ValueError):
                h = None
            break

    return duration, codec, w, h


def get_sprite_screenshot(video_path: Path, t: float) -> Image.Image:
    """One frame: ``-ss`` before ``-i`` for fast seeking (same as Top-Shelf ``main.py``)."""
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        str(t),
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-vf",
        f"scale={SCREENSHOT_SIZE}:{-2}",
        "-c:v",
        "bmp",
        "-f",
        "image2",
        "-",
    ]
    res = subprocess.run(cmd, check=True, capture_output=True, timeout=60)
    return Image.open(BytesIO(res.stdout))


def build_sprite_seeks(video_path: Path, duration: float) -> Image.Image:
    """25 ffmpeg invocations (concurrent) + Pillow montage — fast on long files."""
    offset = 0.05 * duration
    step = (0.9 * duration) / (COLUMNS * ROWS)
    timestamps = [offset + i * step for i in range(COLUMNS * ROWS)]

    def grab(t: float) -> Image.Image:
        return get_sprite_screenshot(video_path, t)

    with ThreadPoolExecutor(max_workers=COLUMNS * ROWS) as pool:
        images = list(pool.map(grab, timestamps))
    w, h = images[0].size
    montage = Image.new("RGB", (w * COLUMNS, h * ROWS))
    for i, img in enumerate(images):
        montage.paste(img, (w * (i % COLUMNS), h * math.floor(i / ROWS)))
    return montage


def build_sprite_single_ffmpeg(
    video_path: Path,
    duration: float,
    hwaccel: str | None,
) -> Image.Image:
    """
    One ffmpeg: select + scale + tile. Often slower on long files (linear scan).
    Optional CUDA hwaccel.
    """
    offset = 0.05 * duration
    step = (0.9 * duration) / (COLUMNS * ROWS)
    dt = max(0.02, min(0.08, step * 0.5))
    between_parts = [
        f"between(t\\,{offset + i * step:.6f}\\,{offset + i * step + dt:.6f})"
        for i in range(COLUMNS * ROWS)
    ]
    select_expr = "+".join(between_parts)
    vf_core = f"select='{select_expr}',scale={SCREENSHOT_SIZE}:-2,tile={COLUMNS}x{ROWS}"

    cmd: list[str] = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if hwaccel == "cuda":
        cmd += ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]
    cmd += ["-i", str(video_path), "-vsync", "0"]
    vf = f"hwdownload,format=yuv420p,{vf_core}" if hwaccel == "cuda" else vf_core
    cmd += [
        "-vf",
        vf,
        "-frames:v",
        "1",
        "-c:v",
        "bmp",
        "-f",
        "image2",
        "-",
    ]
    res = subprocess.run(cmd, check=True, capture_output=True)
    return Image.open(BytesIO(res.stdout))


def build_sprite(
    video_path: Path,
    duration: float,
    sprite_mode: str,
    hwaccel: str | None,
) -> Image.Image:
    if sprite_mode == SPRITE_ONE_FFMPEG:
        return build_sprite_single_ffmpeg(video_path, duration, hwaccel)
    return build_sprite_seeks(video_path, duration)


def compute_phash_from_image(montage: Image.Image) -> str:
    return str(imagehash.phash(montage))


def file_stat_created_iso(st: os.stat_result) -> str:
    ts = getattr(st, "st_birthtime", None)
    if ts is None:
        ts = st.st_mtime
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return ""


def iter_video_files(root: Path) -> list[Path]:
    out: list[Path] = []
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d
            for d in dirnames
            if d.lower() not in SKIP_DIR_NAMES and not d.startswith(".")
        ]
        for fn in filenames:
            if Path(fn).suffix.lower() in VIDEO_EXTENSIONS:
                out.append(Path(dirpath) / fn)
    out.sort(key=lambda p: str(p).lower())
    return out


FIELDNAMES = [
    "path",
    "filename",
    "size_bytes",
    "duration_sec",
    "phash",
    "codec",
    "width",
    "height",
    "file_created_iso",
    "mtime_iso",
    "compute_seconds",
    "error",
]


def process_one(
    video_path: Path,
    *,
    sprite_mode: str = SPRITE_SEEKS,
    hwaccel: str | None = None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    row: dict[str, Any] = {k: "" for k in FIELDNAMES}
    row["path"] = str(video_path.resolve())
    row["filename"] = video_path.name

    try:
        st = video_path.stat()
    except OSError as e:
        row["error"] = f"stat: {e}"
        row["compute_seconds"] = f"{time.perf_counter() - t0:.3f}"
        return row

    row["size_bytes"] = st.st_size
    row["file_created_iso"] = file_stat_created_iso(st)
    try:
        row["mtime_iso"] = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        row["mtime_iso"] = ""

    duration, codec, w, h = ffprobe_format_and_video0(video_path)
    if duration is None or duration <= 0:
        try:
            duration = get_video_duration_packet_fallback(video_path)
        except Exception as e:
            row["error"] = f"duration: {e}"
            row["compute_seconds"] = f"{time.perf_counter() - t0:.3f}"
            return row

    row["duration_sec"] = f"{duration:.6f}"
    row["codec"] = codec or ""
    row["width"] = w if w is not None else ""
    row["height"] = h if h is not None else ""

    try:
        montage = build_sprite(video_path, duration, sprite_mode, hwaccel)
        row["phash"] = compute_phash_from_image(montage)
    except Exception as e:
        row["error"] = f"phash: {e}"
    row["compute_seconds"] = f"{time.perf_counter() - t0:.3f}"
    return row


def process_one_packed(t: tuple[str, str, str]) -> dict[str, Any]:
    """Picklable worker for ProcessPoolExecutor: (path_str, sprite_mode, hwaccel or '')."""
    path_s, mode, hw_s = t
    hw = hw_s if hw_s else None
    return process_one(Path(path_s), sprite_mode=mode, hwaccel=hw)


def load_existing_csv(output: Path) -> tuple[set[str], list[dict[str, Any]]]:
    if not output.exists():
        return set(), []
    with open(output, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "path" not in reader.fieldnames:
            return set(), []
        rows = [dict(r) for r in reader]
    seen = {r["path"].strip() for r in rows if r.get("path")}
    return seen, rows


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compute Top-Shelf-style phashes for all videos under a directory; write CSV.",
    )
    ap.add_argument(
        "directory",
        type=Path,
        help="Root directory to walk (recursive)",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: stdout)",
    )
    ap.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        metavar="N",
        help="Parallel workers (default 1). Uses threads unless --processes.",
    )
    ap.add_argument(
        "--processes",
        action="store_true",
        help="Use processes instead of threads for -j (heavier; can help CPU-bound hashing).",
    )
    ap.add_argument(
        "--one-ffmpeg",
        action="store_true",
        help="One ffmpeg with select+tile (often SLOW on long files). Default: 25 seeks (fast).",
    )
    ap.add_argument(
        "--hwaccel",
        choices=("none", "cuda"),
        default="none",
        help="Only with --one-ffmpeg: NVIDIA hw decode. Default: none.",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="With -o: skip paths already in the CSV; merge new rows.",
    )
    args = ap.parse_args()

    sprite_mode = SPRITE_ONE_FFMPEG if args.one_ffmpeg else SPRITE_SEEKS
    hwaccel = None if args.hwaccel == "none" else args.hwaccel

    if args.hwaccel != "none" and not args.one_ffmpeg:
        print("Ignoring --hwaccel (only applies with --one-ffmpeg).", file=sys.stderr)

    if args.resume and not args.output:
        print("--resume requires -o /path/to.csv", file=sys.stderr)
        raise SystemExit(2)

    try:
        paths = iter_video_files(args.directory)
    except SystemExit as e:
        print(e, file=sys.stderr)
        raise SystemExit(1) from e

    existing_rows: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    if args.resume and args.output:
        seen_paths, existing_rows = load_existing_csv(args.output)
        before = len(paths)
        paths = [p for p in paths if str(p.resolve()) not in seen_paths]
        print(
            f"Resume: skipping {before - len(paths)} already in CSV; {len(paths)} to process.",
            file=sys.stderr,
        )

    if not paths:
        if not existing_rows:
            print("No video files found.", file=sys.stderr)
            raise SystemExit(2)
        print("Resume: nothing new to process; rewriting CSV.", file=sys.stderr)
        out_fp = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout
        try:
            writer = csv.DictWriter(out_fp, fieldnames=FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(existing_rows)
        finally:
            if args.output:
                out_fp.close()
        raise SystemExit(0)

    jobs = max(1, args.jobs)
    new_rows: list[dict[str, Any]] = []
    hw_s = hwaccel or ""

    if jobs == 1:
        for p in paths:
            new_rows.append(
                process_one(p, sprite_mode=sprite_mode, hwaccel=hwaccel if args.one_ffmpeg else None)
            )
    elif args.processes:
        packed = [(str(p.resolve()), sprite_mode, hw_s) for p in paths]
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = {ex.submit(process_one_packed, t): t for t in packed}
            for fut in as_completed(futs):
                new_rows.append(fut.result())
        new_rows.sort(key=lambda r: r["path"].lower())
    else:
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            futs = {
                ex.submit(
                    process_one,
                    p,
                    sprite_mode=sprite_mode,
                    hwaccel=hwaccel if args.one_ffmpeg else None,
                ): p
                for p in paths
            }
            for fut in as_completed(futs):
                new_rows.append(fut.result())
        new_rows.sort(key=lambda r: r["path"].lower())

    all_rows = existing_rows + new_rows
    all_rows.sort(key=lambda r: (r.get("path") or "").lower())

    out_fp = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout
    try:
        writer = csv.DictWriter(out_fp, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    finally:
        if args.output:
            out_fp.close()

    if args.output:
        print(
            f"Wrote {len(all_rows)} row(s) ({len(new_rows)} new) to {args.output}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
