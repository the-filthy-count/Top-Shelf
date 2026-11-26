import re
import json
from pathlib import Path
from datetime import datetime

import database as db
from app.core.filename_dates import extract_filename_date

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


def _release_name_with_folders(filename: str) -> str:
    """Include parent names, keeping boundaries so names cannot span folders."""
    if not filename:
        return ""
    path = Path(filename.replace("\\", "/"))
    return "/".join((*path.parts[:-1], path.stem))




def studio_search_name(value):
    return ' '.join((value or '').replace('_', ' ').split())


def _suggestion_identity_matches(performers: list[str], studio: str, scene: dict) -> bool:
    """Require a cast match or an exact normalized studio/alias match."""
    if performers and _required_performers_in_cast(performers, scene):
        return True
    normalize = lambda value: re.sub(r"[\W_]", "", str(value).casefold())
    wanted = normalize(studio)
    if not wanted:
        return False
    source = scene.get("studio") or {}
    proof = scene.get('_search_studio_id') or {}
    if (proof.get('source') == scene.get('_source') and normalize(proof.get('name', '')) == wanted
            and isinstance(source, dict) and str(source.get('id') or '') == proof.get('id')):
        return True
    if isinstance(source, str):
        names = [source]
    else:
        names = [source.get("name", ""), *(source.get("aliases") or [])]
    return any(wanted == normalize(name) for name in names if isinstance(name, str))


def _strip_release_noise(text: str) -> str:
    """Remove technical release metadata without discarding names or title numbers."""
    text = re.sub(r"(?i)\b(?:https?://)?(?:www\.)?[a-z0-9-]+\.(?:com|net|org|xxx)\b", " ", text)
    text = re.sub(r"(?i)\b(?:web[ ._-]*dl|web[ ._-]*rip|h[ ._-]*26[45]|x26[45]|hevc|avc|aac(?:[ ._-]*2[ .]0)?|ac3|2160p|1080p|720p|480p|4k|8k|uhd|hdr10?|10bit|8bit|\d{2,3}fps|\d{3,4}x\d{3,4}|\d+(?:\.\d+)?[ ._-]*(?:mb|gb))\b", " ", text)
    text = re.sub(r"(?i)\b[a-f0-9]{16,64}\b", " ", text)
    return re.sub(r"\s+", " ", text).strip(" ._-[]()")

def _clean_filename_for_search(filename: str) -> str:
    """Strip extension, ts-uid tag, and release-noise tokens from a
    filename. Includes any parent folder(s) so a performer / studio
    that only appears in the directory name still contributes tokens.
    """
    if not filename:
        return ""
    base = _release_name_with_folders(filename)
    cleaned, _ = _strip_ts_uid(base)
    _, date_text = extract_filename_date(cleaned)
    if date_text:
        cleaned = cleaned.replace(date_text, " ", 1)
    cleaned = _strip_release_noise(cleaned)
    cleaned = re.sub(r"[\[\](){}]", " ", cleaned)
    cleaned = _SCENE_NUMBER_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[./_\\-]+", " ", cleaned)
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


def _split_query_from_library(filename: str, *, library_indexes=None, studio_logos=None) -> tuple[str, str, str, str, list[str]]:
    """Split a cleaned filename into (full_query, title_only, performer, studio, performers).

    Consults the library's performer-folder index for cast names and the
    studio-logos table (with aliases) so the title-only query strips
    studio + performer tokens out before going to IAFD/Prowlarr search.
    Returns ("", "", "", "", []) when cleaning yields nothing usable.
    """
    full = _clean_filename_for_search(filename)
    if not full:
        return ("", "", "", "", [])
    #: Use the folder-joined release name so the library guess can
    #: pick up performers / studios that only appear in the parent
    #: directory (e.g. ``Megan Mistakes/scene 18.mp4``). Without this
    #: the guess ran against just the basename and dropped every
    #: signal the folder carried.
    release_name = _release_name_with_folders(filename)
    performer = ""
    performers: list[str] = []
    studio = ""
    try:
        guess = db._library_guess_from_release_name(release_name, *library_indexes) if library_indexes is not None else db._library_guess_from_release_name(release_name)
        performers = [str(p).strip() for p in (guess.get("performers") or []) if str(p).strip()]
        performer = performers[0] if performers else ""
        studio = (guess.get("studio") or "").strip()
    except Exception:
        pass
    # Performer folder tags also occur in the studio-logo index (creator sites).
    # Do not let that duplicate identity hide a real studio in the filename.
    compact = lambda value: re.sub(r"[^a-z0-9]+", "", value.casefold())
    performer_keys = {compact(name) for name in performers}
    if compact(studio) in performer_keys:
        studio = ""
    studio_spelling = ""
    if not studio:
        try:
            release_key = re.sub(r"[^a-z0-9]+", "", release_name.lower())
            best_len = 0
            for row in (db.studio_logo_list_all() if studio_logos is None else studio_logos):
                slug_key = re.sub(r"[^a-z0-9]+", "", (row.get("slug") or "").lower())
                display = row.get("name") or row.get("slug") or ""
                if compact(display) in performer_keys or slug_key in performer_keys:
                    continue
                if not slug_key or not display or len(slug_key) < 4:
                    continue
                if slug_key in release_key and len(slug_key) > best_len:
                    studio = display
                    studio_spelling = row.get("slug") or display
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
                                studio_spelling = alias
                                best_len = len(alias_key)
                    except Exception:
                        pass
        except Exception:
            studio = ""
    # Remove complete detected names within each path component. Preserve
    # individual title words and never match a name across folder boundaries.
    title_parts = release_name.split("/")
    names = sorted([*performers, studio, studio_spelling], key=len, reverse=True)
    for name in names:
        letters = [c for c in name.casefold() if c.isalnum()]
        if not letters:
            continue
        pattern = r"(?<!\w)" + r"[\W_]*".join(re.escape(c) for c in letters) + r"(?!\w)"
        title_parts = [re.sub(pattern, " ", part, flags=re.IGNORECASE) for part in title_parts]
    # The cleaner expects a filename; attach a dummy extension to retain the
    # final title token even when the original name contained dots.
    from app.core.scene_relevance import split_title_words
    title_only = split_title_words(_clean_filename_for_search("/".join(title_parts) + ".mp4"))
    return (full, title_only, performer, studio, performers)


def _split_query_for_search(filename: str):
    from app.core.filename_parser import parse_filename
    settings = db.get_settings() if Path(db.DB_PATH).is_file() else db.DEFAULTS
    parsed = parse_filename(filename, settings)
    cast = parsed['performer_names']
    return (parsed['full_query'], parsed['title'], cast[0] if cast else '', parsed['site'], cast)


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
    """Exact ordered name, allowing punctuation, spacing and compact spelling."""
    compact = lambda value: "".join(c for c in str(value).casefold() if c.isalnum())
    wanted = compact(query_name)
    return bool(wanted) and wanted == compact(cast_name)


def _required_performers_in_cast(query_performers: list[str], candidate: dict) -> bool:
    """Every library-detected performer from the filename must map to cast."""
    needed = [p.strip() for p in (query_performers or []) if p and p.strip()]
    if not needed:
        return True
    proof = candidate.get('_search_performer_ids') or {}
    linked = proof.get('ids') or {}
    if proof.get('source') == candidate.get('_source') and all(name in linked for name in needed):
        actual = {str((entry.get('performer') or entry).get('id') or '')
                  for entry in candidate.get('performers') or [] if isinstance(entry, dict)}
        wanted = {str(linked[name]) for name in needed}
        return len(wanted) == len(needed) and wanted.issubset(actual)
    cast = _candidate_performer_names(candidate)
    if not cast:
        return False
    used: set[int] = set()
    for qp in needed:
        hit = False
        for i, raw in enumerate(candidate.get('performers') or []):
            entry = (raw.get('performer') or raw) if isinstance(raw, dict) else {}
            cn = entry.get('name') or (raw if isinstance(raw, str) else '')
            if i in used:
                continue
            if proof.get('source') == candidate.get('_source') and qp in linked:
                same = str(entry.get('id') or '') == str(linked[qp])
            else:
                same = _performer_name_tokens_match(qp, cn)
            if same:
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
        from app.core.queue_media import read
        settings = db.get_settings()
        vice = (settings.get("vice_download_watch_dir") or "").strip()
        path = db.resolve_source_video_path(Path(settings.get("source_dir") or ""), filename,
            vice_dl_root=Path(vice).expanduser() if vice else None)
        if path:
            meta["duration_s"] = int(read(filename, path, duration_probe=get_video_duration).get("media_duration") or 0)
    except Exception:
        pass
    try:
        stem = Path(filename).stem
        parsed_date, _ = extract_filename_date(stem)
        if parsed_date:
            meta["date"] = parsed_date
            meta["year"] = int(parsed_date[:4])
        next_year = datetime.now().year + 1
        for m in re.finditer(r"(?<!\d)(19[7-9]\d|20\d{2})(?!\d)", stem):
            y = int(m.group(1))
            if 1970 <= y <= next_year:
                if not meta["year"]:
                    meta["year"] = y
                break
    except Exception:
        pass
    try:
        _full, _title_only, _perf, _studio, performers = _split_query_for_search(filename)
        meta["cast_hint"] = len([p for p in (performers or []) if str(p).strip()])
    except Exception:
        pass
    try:
        sn = _extract_scene_number_from_filename(filename)
        meta["scene_index"] = sn
    except Exception:
        pass
    return meta


def _score_name_match(query, candidate, **kwargs):
    from app.core.video_matching.decisions import score_suggestion
    return score_suggestion(query,candidate,**kwargs)
