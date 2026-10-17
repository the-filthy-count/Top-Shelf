#!/usr/bin/env python3
"""Prune a studio-logo archive down to only the logos that match studios
actually referenced in a media library's NFO sidecars.

Standalone script — no Top-Shelf imports. Run directly:

    python3 prune_studio_logos.py \
        --archive /path/to/static/logos/studio_logos \
        --library /home/me/Top-Shelf/Series \
        --library /home/me/Top-Shelf/Features \
        --library /home/me/Top-Shelf/Stars \
        --library /home/me/Top-Shelf/Erotica \
        --dry-run

Add --execute to actually delete. Use --cache-file /tmp/wanted_studios.json
to speed up a re-run (the NFO walk can take several minutes on a large
library — the cache file lets subsequent runs skip it).

Pipeline:
  1. Walk every --library root for .nfo files.
  2. For each NFO, read the first 4KB and regex-extract <studio>…</studio>.
  3. Normalise studio names → slugs (lowercase, punctuation stripped).
  4. Match each archive PNG against the wanted set using four strategies:
       exact        — archive slug == wanted slug
       spaceless    — archive slug with spaces removed == wanted slug ditto
       wanted⊂arch  — wanted slug appears as whole-word substring of archive
       arch⊂wanted  — archive slug appears as whole-word substring of wanted
  5. Print a report and either dry-run or delete non-matches.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

# ── Normalisation (mirror of main.normalise()) ──────────────────────────
def normalise(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("-", " ").replace("_", " ").replace(".", " ")
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


STUDIO_TAG_BIN = re.compile(rb"<studio[^>]*>([^<]+)</studio>", re.I)


def scan_nfos(roots: list[Path], budget_s: float = 1800.0) -> set[str]:
    """Walk library roots, extract <studio> tag values from NFOs."""
    found: set[str] = set()
    # Every top-level subfolder of the first root is treated as a studio too
    # (matches Top-Shelf's Series directory convention).
    if roots and roots[0].is_dir():
        for d in roots[0].iterdir():
            if d.is_dir():
                found.add(d.name)
    nfo_count = 0
    start = time.time()
    for root in roots:
        if not root.is_dir():
            print(f"  WARN: {root} does not exist, skipping", file=sys.stderr)
            continue
        for dirpath, _dirs, filenames in os.walk(root):
            if (time.time() - start) > budget_s:
                print(f"\n  WARN: NFO scan budget exhausted at {nfo_count} files")
                return found
            for fn in filenames:
                if not fn.lower().endswith(".nfo"):
                    continue
                nfo_count += 1
                if nfo_count % 1000 == 0:
                    sys.stdout.write(
                        f"\r  NFO scan: {nfo_count} files, {len(found)} studios "
                        f"({time.time()-start:.0f}s)"
                    )
                    sys.stdout.flush()
                try:
                    with open(os.path.join(dirpath, fn), "rb") as fh:
                        head = fh.read(4096)
                    for m in STUDIO_TAG_BIN.finditer(head):
                        txt = m.group(1).decode("utf-8", errors="ignore").strip()
                        if txt:
                            found.add(txt)
                except OSError:
                    pass
    sys.stdout.write("\r" + " " * 80 + "\r")
    sys.stdout.flush()
    print(f"[wanted] scanned {nfo_count} NFOs in {time.time()-start:.0f}s → {len(found)} raw names")
    return found


def build_match_index(archive: Path) -> tuple[list[tuple[str, Path]], dict[str, Path]]:
    """Returns (length-desc list, exact slug → path map) for archive PNGs."""
    by_length: list[tuple[str, Path]] = []
    by_slug: dict[str, Path] = {}
    for p in archive.iterdir():
        if not p.is_file() or p.suffix.lower() != ".png":
            continue
        slug = normalise(p.stem)
        if not slug:
            continue
        by_length.append((slug, p))
        by_slug.setdefault(slug, p)
    by_length.sort(key=lambda kv: -len(kv[0]))
    return by_length, by_slug


def classify_archive(
    archive_files: list[tuple[str, Path]],
    by_slug: dict[str, Path],
    wanted_norm: dict[str, str],
) -> dict[Path, tuple[str, str]]:
    """Return {archive_path: (wanted_display_name, match_strategy)} for every
    archive file that matches at least one wanted studio."""
    wanted_spaceless = {k.replace(" ", ""): v for k, v in wanted_norm.items()}

    # Index wanted by first token for the substring passes — keeps them cheap.
    SKIP_FIRST = {"the", "a", "an", "my", "her", "his", "and", "of", "in",
                  "for", "it", "to", "on", "at", "is"}
    first_word_idx: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for wn, wr in wanted_norm.items():
        words = wn.split()
        if not words:
            continue
        key = words[0]
        if key in SKIP_FIRST and len(words) > 1:
            key = words[1]
        if len(key) < 3:
            continue
        first_word_idx[key].append((wn, wr))
    for k in first_word_idx:
        first_word_idx[k].sort(key=lambda kv: -len(kv[0]))

    matched: dict[Path, tuple[str, str]] = {}
    for stem_n, p in archive_files:
        # 1. exact
        if stem_n in wanted_norm:
            matched[p] = (wanted_norm[stem_n], "exact"); continue
        # 2. spaceless
        sns = stem_n.replace(" ", "")
        if sns in wanted_spaceless:
            matched[p] = (wanted_spaceless[sns], "spaceless"); continue
        # 3. wanted ⊂ archive (whole-word)
        stem_words = stem_n.split()
        stem_padded = f" {stem_n} "
        hit = None
        seen_buckets: set[str] = set()
        for w in stem_words:
            if w in seen_buckets or w not in first_word_idx:
                continue
            seen_buckets.add(w)
            for wn, wr in first_word_idx[w]:
                if len(wn) < 5:
                    continue
                if f" {wn} " in stem_padded:
                    hit = (wr, "wanted⊂archive"); break
            if hit:
                break
        if hit:
            matched[p] = hit; continue
        # 4. archive ⊂ wanted
        if len(stem_n) >= 5:
            for w in stem_words:
                if w not in first_word_idx:
                    continue
                for wn, wr in first_word_idx[w]:
                    if f" {stem_n} " in f" {wn} ":
                        hit = (wr, "archive⊂wanted"); break
                if hit:
                    break
        if hit:
            matched[p] = hit
    return matched


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive", required=True, type=Path,
                        help="Directory containing the PNG logos to prune.")
    parser.add_argument("--library", type=Path, action="append", default=[],
                        help="Library root to scan for NFOs (repeatable).")
    parser.add_argument("--cache-file", type=Path,
                        help="JSON file to cache the wanted-studio set. "
                             "First run writes it, subsequent runs read it "
                             "to skip the NFO walk.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete non-matching files "
                             "(default is dry-run).")
    parser.add_argument("--budget", type=float, default=1800.0,
                        help="Max wall-clock seconds for the NFO scan (default 30 min).")
    args = parser.parse_args()

    if not args.archive.is_dir():
        print(f"ERROR: archive dir not found: {args.archive}", file=sys.stderr)
        return 2

    # ── 1. Load / build the wanted studio set ────────────────────────
    wanted_raw: set[str] = set()
    if args.cache_file and args.cache_file.exists():
        try:
            wanted_raw = set(json.loads(args.cache_file.read_text()))
            print(f"[wanted] loaded {len(wanted_raw)} names from cache {args.cache_file}")
        except Exception as e:
            print(f"  WARN: could not read cache file: {e}", file=sys.stderr)
    if not wanted_raw:
        if not args.library:
            print("ERROR: no --library roots and no cache — nothing to match against.",
                  file=sys.stderr)
            return 2
        wanted_raw = scan_nfos(args.library, budget_s=args.budget)
        if args.cache_file:
            args.cache_file.write_text(json.dumps(sorted(wanted_raw)))
            print(f"[wanted] cached to {args.cache_file}")

    wanted_norm: dict[str, str] = {}
    for raw in wanted_raw:
        n = normalise(raw)
        if n:
            wanted_norm.setdefault(n, raw)
    print(f"[wanted] {len(wanted_norm)} unique normalised studio names")

    # ── 2. Build the archive index ───────────────────────────────────
    archive_by_length, by_slug = build_match_index(args.archive)
    total_files = len(archive_by_length)
    total_bytes = sum(p.stat().st_size for _n, p in archive_by_length)
    print(f"[archive] {total_files} png files, {human(total_bytes)}")
    if not total_files:
        print("Nothing to prune.")
        return 0

    # ── 3. Match ─────────────────────────────────────────────────────
    start = time.time()
    matched = classify_archive(archive_by_length, by_slug, wanted_norm)
    print(f"[match] {time.time()-start:.1f}s")

    kept_bytes = sum(p.stat().st_size for p in matched)
    reasons = Counter(m[1] for m in matched.values())

    print()
    print(f"  keep:   {len(matched):>6}  ({human(kept_bytes)})")
    for r, c in reasons.most_common():
        print(f"    {r:<20} {c}")
    print(f"  delete: {total_files - len(matched):>6}  ({human(total_bytes - kept_bytes)})")

    wanted_hit = {m[0] for m in matched.values()}
    missing = sorted(v for v in wanted_norm.values() if v not in wanted_hit)
    print()
    print(f"[wanted without logo] {len(missing)} of {len(wanted_norm)}")
    for m in missing[:10]:
        print(f"  ✗ {m}")
    if len(missing) > 10:
        print(f"  … +{len(missing)-10} more")

    # ── 4. Delete ────────────────────────────────────────────────────
    if not args.execute:
        print()
        print("Dry run — no files deleted. Re-run with --execute to delete.")
        return 0

    print()
    print("Deleting non-matching files…")
    to_delete = [p for _n, p in archive_by_length if p not in matched]
    freed = 0
    errs = 0
    for p in to_delete:
        try:
            sz = p.stat().st_size
            p.unlink()
            freed += sz
        except OSError as e:
            errs += 1
            print(f"  err: {p.name}: {e}", file=sys.stderr)
    print(f"[done] removed {len(to_delete)-errs} files, freed {human(freed)} ({errs} errors)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
