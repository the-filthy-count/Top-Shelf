import re
import json
from pathlib import Path
from datetime import datetime

import database as db

_NAME_SEARCH_NOISE_TOKENS = {
    "xxx", "1080p", "720p", "2160p", "4k", "uhd", "hdr",
    "web", "web-dl", "webdl", "webrip", "hdrip", "bdrip", "brrip",
    "dvdrip", "dvdr", "hd", "sd",
    "x264", "x265", "h264", "h265", "hevc", "avc", "av1",
    "aac", "ac3", "dd5", "ddp", "mp3", "flac",
    "mkv", "mp4", "avi", "wmv", "mov",
    "internal", "repack", "proper", "limited", "extended",
    # Release-name glue words — otherwise "WITH" in a scene title inflates
    # token-overlap scores for unrelated suggestions.
    "with", "and", "for", "of", "vs", "feat", "featuring",
}

#: Minimum name-match score (0–1) before a candidate is shown in the
#: queue Suggestions modal.
_SUGGESTION_MIN_SCORE = 0.50


_SCENE_NUMBER_RE = re.compile(
    r"\b(?:scene|sc)\.?\s*(\d{1,3})\b",
    re.IGNORECASE,
)

# Trailing standalone integer (no `Scene` keyword) — covers compilation releases
# like "Squirt And Fuck 1.avi". 1–99 only to avoid absorbing years/resolutions.
_TRAILING_INDEX_RE = re.compile(
    r"(?<![\d])(\d{1,2})\s*$",
)

# `[ts-XXXXXXXX]` job-id tag the download-folder pipeline stamps onto staged files.
TS_UID_RE = re.compile(
    r"\s*[\[\(\.\_\-]*ts[-_]([a-f0-9]{8})[\]\)\.\_\-]*\s*",
    re.IGNORECASE,
)


def _strip_ts_uid(name: str) -> tuple[str, str]:
    """Remove the `[ts-XXXXXXXX]` tag from a job/file name. Returns
    (cleaned_name, uid_or_empty). Idempotent."""
    if not name:
        return "", ""
    m = TS_UID_RE.search(name)
    if not m:
        return name, ""
    cleaned = TS_UID_RE.sub(" ", name).strip()
    return cleaned, m.group(1).lower()


def _extract_scene_number_from_filename(filename: str) -> int | None:
    """Pull a scene number out of a filename ('Movie Scene 5.mp4' → 5)."""
    if not filename:
        return None
    stem = Path(filename).stem
    m = _SCENE_NUMBER_RE.search(stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _extract_trailing_index_from_filename(filename: str) -> int | None:
    """Pull a trailing standalone integer from a filename (1–99 only).
    Skips when an explicit `Scene N` form is already present."""
    if not filename:
        return None
    stem = Path(filename).stem
    if _SCENE_NUMBER_RE.search(stem):
        return None
    norm = re.sub(r"[._\-]+", " ", stem).strip()
    if not norm:
        return None
    m = _TRAILING_INDEX_RE.search(norm)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except (TypeError, ValueError):
        return None
    return n if 1 <= n <= 99 else None


def _clean_filename_for_search(filename: str) -> str:
    """Strip extension, ts-uid tag, and release-noise tokens from a filename."""
    if not filename:
        return ""
    stem = Path(filename).stem
    cleaned, _ = _strip_ts_uid(stem)
    cleaned = re.sub(r"\[[^\]]*\]", " ", cleaned)
    cleaned = re.sub(r"\([^)]*\)", " ", cleaned)
    cleaned = re.sub(r"\{[^}]*\}", " ", cleaned)
    cleaned = _SCENE_NUMBER_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[._\-]+", " ", cleaned)
    parts = []
    for token in cleaned.split():
        t = token.strip().lower()
        if not t:
            continue
        if t in _NAME_SEARCH_NOISE_TOKENS:
            continue
        parts.append(token)
    out = " ".join(parts).strip()
    out = re.sub(r"\s+", " ", out)
    return out


def _name_match_tokenize(s: str) -> set[str]:
    """Lowercased non-noise word set for scoring overlap."""
    if not s:
        return set()
    words = re.findall(r"[A-Za-z0-9]+", s.lower())
    return {w for w in words if w and w not in _NAME_SEARCH_NOISE_TOKENS and len(w) > 1}


def _split_query_for_search(filename: str) -> tuple[str, str, str, str, list[str]]:
    """Split a cleaned filename into (full_query, title_only, performer, studio, performers).

    Consults the library's performer-folder index for cast names and the
    studio-logos table (with aliases) so the title-only query strips
    studio + performer tokens out before going to IAFD/Prowlarr search.
    Returns ("", "", "", "", []) when cleaning yields nothing usable.
    """
    full = _clean_filename_for_search(filename)
    if not full:
        return ("", "", "", "", [])
    performer = ""
    performers: list[str] = []
    studio = ""
    try:
        guess = db._library_guess_from_release_name(Path(filename).stem)
        performers = [str(p).strip() for p in (guess.get("performers") or []) if str(p).strip()]
        performer = performers[0] if performers else ""
        studio = (guess.get("studio") or "").strip()
    except Exception:
        pass
    if not studio:
        try:
            release_key = re.sub(r"[^a-z0-9]+", "", Path(filename).stem.lower())
            best_len = 0
            for row in db.studio_logo_list_all():
                slug_key = re.sub(r"[^a-z0-9]+", "", (row.get("slug") or "").lower())
                display = row.get("name") or row.get("slug") or ""
                if not slug_key or not display or len(slug_key) < 4:
                    continue
                if slug_key in release_key and len(slug_key) > best_len:
                    studio = display
                    best_len = len(slug_key)
                aliases_raw = row.get("aliases_json") or ""
                if aliases_raw:
                    try:
                        for alias in (json.loads(aliases_raw) or []):
                            alias_key = re.sub(r"[^a-z0-9]+", "", (alias or "").lower())
                            if len(alias_key) < 4:
                                continue
                            if alias_key in release_key and len(alias_key) > best_len:
                                studio = display
                                best_len = len(alias_key)
                    except Exception:
                        pass
        except Exception:
            studio = ""
    perf_tokens: set[str] = set()
    for p in performers:
        perf_tokens |= {t for t in re.findall(r"[a-z0-9]+", p.lower()) if len(t) > 1}
    studio_tokens = {t for t in re.findall(r"[a-z0-9]+", studio.lower()) if len(t) > 1}
    drop_tokens = perf_tokens | studio_tokens
    if not drop_tokens:
        return (full, full, performer, studio, performers)
    title_only_tokens = []
    for tok in full.split():
        norm = re.sub(r"[^a-z0-9]+", "", tok.lower())
        if norm and norm in drop_tokens:
            continue
        title_only_tokens.append(tok)
    title_only = " ".join(title_only_tokens).strip() or full
    return (full, title_only, performer, studio, performers)


def _candidate_performer_names(candidate: dict) -> list[str]:
    names: list[str] = []
    for p in (candidate.get("performers") or []):
        if isinstance(p, dict):
            inner = p.get("performer") if isinstance(p.get("performer"), dict) else None
            n = (inner or p).get("name")
            if n:
                names.append(str(n).strip())
        elif isinstance(p, str) and p.strip():
            names.append(p.strip())
    return names


def _performer_name_tokens_match(query_name: str, cast_name: str) -> bool:
    """True when every significant token in ``query_name`` appears in ``cast_name``."""
    qt = _name_match_tokenize(query_name)
    if not qt:
        return False
    ct = set(_name_match_tokenize(cast_name))
    return bool(ct) and qt <= ct


def _required_performers_in_cast(query_performers: list[str], candidate: dict) -> bool:
    """Every library-detected performer from the filename must map to cast."""
    needed = [p.strip() for p in (query_performers or []) if p and p.strip()]
    if not needed:
        return True
    cast = _candidate_performer_names(candidate)
    if not cast:
        return False
    used: set[int] = set()
    for qp in needed:
        hit = False
        for i, cn in enumerate(cast):
            if i in used:
                continue
            if _performer_name_tokens_match(qp, cn):
                used.add(i)
                hit = True
                break
        if not hit:
            return False
    return True


def _suggestion_performer_names(performers: list[str], performer: str) -> list[str]:
    """Normalized performer list from library guess / split query."""
    names = [p.strip() for p in (performers or []) if p and p.strip()]
    if not names and (performer or "").strip():
        names = [performer.strip()]
    return names


def _build_file_meta(filename: str) -> dict:
    """One-time per-file metadata bundle threaded into ``_score_name_match``:
    duration_s, year, cast_hint, scene_index. All fields optional — a missing
    value just skips that signal instead of zeroing the score.
    """
    from app.core.phash import get_video_duration

    meta: dict = {"duration_s": 0, "year": 0, "cast_hint": 0, "scene_index": None}
    try:
        cached_dur = db.get_media_duration(filename)
        if cached_dur is not None:
            meta["duration_s"] = int(cached_dur)
        else:
            settings = db.get_settings()
            src_root = (settings.get("source_dir") or "").strip()
            if src_root:
                path = Path(src_root) / filename
                if path.is_file():
                    dur = get_video_duration(path) or 0
                    meta["duration_s"] = int(dur)
                    db.update_file(filename, status="processing", media_duration=meta["duration_s"])
    except Exception:
        pass
    try:
        stem = Path(filename).stem
        next_year = datetime.now().year + 1
        for m in re.finditer(r"(?<!\d)(19[7-9]\d|20\d{2})(?!\d)", stem):
            y = int(m.group(1))
            if 1970 <= y <= next_year:
                meta["year"] = y
                break
        if not meta["year"]:
            settings = settings if 'settings' in dir() else db.get_settings()
            src_root = (settings.get("source_dir") or "").strip()
            if src_root:
                path = Path(src_root) / filename
                if path.is_file():
                    mt = datetime.fromtimestamp(path.stat().st_mtime).year
                    if 1970 <= mt <= next_year:
                        meta["year"] = mt
    except Exception:
        pass
    try:
        _full, _title_only, _perf, _studio, performers = _split_query_for_search(filename)
        meta["cast_hint"] = len([p for p in (performers or []) if str(p).strip()])
    except Exception:
        pass
    try:
        sn = _extract_scene_number_from_filename(filename)
        if sn is None:
            sn = _extract_trailing_index_from_filename(filename)
        meta["scene_index"] = sn
    except Exception:
        pass
    return meta


def _score_name_match(query: str, candidate: dict, *,
                       query_performer: str = "",
                       query_performers: list[str] | None = None,
                       require_performers_in_cast: bool = True,
                       file_meta: dict | None = None,
                       reasons: list[dict] | None = None) -> float:
    """Fraction of query tokens present in the candidate's title + studio + performer string.
    Range 0–1.

    Performer-only overlap is rejected: if the query has at least one title-bearing
    token and the candidate's title + studio share zero tokens with those, the score
    is 0 regardless of how many performer-name tokens overlap.

    When ``require_performers_in_cast`` is true and the library index names one or
    more performers in the release, every one must appear in the candidate cast.

    When ``file_meta`` is provided (see ``_build_file_meta``), three additional
    multipliers shape the final score:

    * **Duration delta** — ±60s free; linear scale down to 600s for a ≤40% cut.
    * **Year proximity** — ±1y free; 5%/year out to 5y (25% cap).
    * **Cast-size delta** — 5%/extra-performer (20% cap).

    If ``reasons`` is a list, this function appends structured per-signal labels
    so the UI can show *why* a score landed where it did.
    """
    performers = _suggestion_performer_names(
        list(query_performers or []), query_performer or ""
    )
    if require_performers_in_cast and performers and not _required_performers_in_cast(performers, candidate):
        return 0.0

    q = _name_match_tokenize(query)
    if not q:
        return 0.0
    perf_q_tokens: set[str] = set()
    for p in performers:
        perf_q_tokens |= _name_match_tokenize(p)
    if not perf_q_tokens:
        perf_q_tokens = _name_match_tokenize(query_performer or "")
    title_q_tokens = q - perf_q_tokens

    perf_names = _candidate_performer_names(candidate)
    studio = (candidate.get("studio") or {})
    studio_name = studio.get("name") if isinstance(studio, dict) else (studio or "")
    title_tokens_cand = _name_match_tokenize(
        " ".join([candidate.get("title") or "", studio_name or ""])
    )
    perf_tokens_cand = _name_match_tokenize(" ".join(perf_names))

    if title_q_tokens:
        shared_title = title_q_tokens & title_tokens_cand
        if not shared_title:
            return 0.0
        if len(shared_title) / max(1, len(title_q_tokens)) < 0.34:
            return 0.0

    blob_tokens = title_tokens_cand | perf_tokens_cand
    if not blob_tokens:
        return 0.0
    overlap = len(q & blob_tokens)
    base = overlap / max(1, len(q))

    if reasons is not None:
        shared_count = len(title_q_tokens & title_tokens_cand) if title_q_tokens else 0
        total_count = len(title_q_tokens) if title_q_tokens else 0
        reasons.append({
            "kind": "title",
            "label": (f"Title {shared_count}/{total_count}"
                       if total_count else "Title — no tokens"),
            "ok": (shared_count / total_count) >= 0.5 if total_count else False,
        })

    if reasons is not None and performers:
        for p in performers:
            hit = any(_performer_name_tokens_match(p, c) for c in perf_names)
            reasons.append({
                "kind": "performer",
                "label": p,
                "ok": hit,
            })

    cand_dur = candidate.get("duration") if isinstance(candidate, dict) else None
    try:
        cand_dur_s = int(cand_dur or 0)
    except (TypeError, ValueError):
        cand_dur_s = 0
    file_dur_s = int((file_meta or {}).get("duration_s") or 0)
    if cand_dur_s > 0 and file_dur_s > 0:
        delta = abs(cand_dur_s - file_dur_s)
        free = 60
        cap = 600
        if delta <= free:
            dur_mult = 1.0
            dur_ok = True
        else:
            cut = min(0.40, ((delta - free) / cap) * 0.40)
            dur_mult = 1.0 - cut
            dur_ok = delta <= 120
        base *= dur_mult
        if reasons is not None:
            sign = "−" if cand_dur_s < file_dur_s else "+"
            mins = abs(cand_dur_s - file_dur_s) // 60
            secs = abs(cand_dur_s - file_dur_s) % 60
            if mins:
                label = f"Duration {sign}{mins}m"
            else:
                label = f"Duration {sign}{secs}s"
            reasons.append({"kind": "duration", "label": label, "ok": dur_ok})
    elif reasons is not None and (file_dur_s > 0) != (cand_dur_s > 0):
        side = "file" if file_dur_s else "scene"
        reasons.append({
            "kind": "duration",
            "label": f"No {side} duration",
            "ok": False,
        })

    cand_date = (candidate.get("release_date") or "").strip()
    cand_year = 0
    if len(cand_date) >= 4:
        try:
            cand_year = int(cand_date[:4])
        except ValueError:
            cand_year = 0
    file_year = int((file_meta or {}).get("year") or 0)
    if cand_year and file_year:
        diff = abs(cand_year - file_year)
        if diff <= 1:
            year_mult = 1.0
            year_ok = True
        else:
            cut = min(0.25, (diff - 1) * 0.05)
            year_mult = 1.0 - cut
            year_ok = diff <= 2
        base *= year_mult
        if reasons is not None:
            sign = "+" if cand_year > file_year else "−"
            if diff <= 1:
                reasons.append({"kind": "year", "label": f"Year {cand_year}", "ok": True})
            else:
                reasons.append({"kind": "year", "label": f"Year {sign}{diff}", "ok": year_ok})

    cast_hint = int((file_meta or {}).get("cast_hint") or 0)
    cand_cast = len(perf_names)
    if cast_hint > 0 and cand_cast > cast_hint:
        extra = cand_cast - cast_hint
        cut = min(0.20, extra * 0.05)
        base *= (1.0 - cut)
        if reasons is not None:
            reasons.append({
                "kind": "cast",
                "label": f"Cast {cand_cast} (+{extra})",
                "ok": False,
            })
    elif reasons is not None and cast_hint > 0:
        reasons.append({
            "kind": "cast",
            "label": f"Cast {cand_cast}",
            "ok": True,
        })

    scene_idx = (file_meta or {}).get("scene_index")
    if isinstance(scene_idx, int) and scene_idx > 0:
        cand_title = (candidate.get("title") or "")
        idx_re = re.compile(rf"(?:scene|sc|#)\.?\s*0*{scene_idx}\b|\b0*{scene_idx}\s*$", re.IGNORECASE)
        if idx_re.search(cand_title):
            base = min(1.0, base + 0.03)
            if reasons is not None:
                reasons.append({"kind": "scene_idx", "label": f"Scene {scene_idx}", "ok": True})

    if candidate.get("release_date") and base < 1.0:
        base = min(1.0, base + 0.01)
    return min(1.0, base)
