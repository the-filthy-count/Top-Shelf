"""Shared filename interpretation for automatic and manual scene searches."""
import re
from pathlib import Path
from datetime import datetime
from filename_dates import extract_filename_date
from app.core.matching import _strip_release_noise


def cache_signature(settings):
    """Fingerprint parser inputs, including same-second library edits."""
    import hashlib
    import json
    import database as db
    inputs = {
        'settings': {key: settings.get(key, '') for key in (
            'filename_patterns', 'filename_strip_words',
            'site_abbreviations', 'site_rename_map')},
        'indexes': db._build_library_match_indexes(),
        'logos': sorted((row.get('name') or '', row.get('slug') or '')
                        for row in db.studio_logo_list_all()),
    }
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()

_TITLE_NOISE_RE = re.compile(
    r"""
    (?:^|[\s._\-–—]+)          # separator before the token
    (?:
        scene \s* [#]? \s* \d+           # Scene 2, Scene #2
      | part  \s*       \d+              # Part 3
      | pt    \.? \s*   \d+              # Pt 3, Pt. 3
      | episode \s*     \d+              # Episode 4
      | ep    \s*       \d+              # Ep 4
      | cd              \s* \d+          # CD1, CD 1
      | disc  \s*       \d+              # Disc 1
    )
    \s* $                                # anchored to end
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _strip_title_noise(title: str) -> str:
    if not title:
        return title
    out = _strip_release_noise(title)
    #: Loop because multiple noise tokens can stack ("Title Scene 2 Part 1").
    for _ in range(3):
        new = _TITLE_NOISE_RE.sub("", out).strip()
        new = re.sub(r"[\s._\-–—]+$", "", new).strip()
        if new == out:
            break
        out = new
    return out


def _parse_filename_parts(filename: str, settings: dict) -> dict:
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
                result["title"]      = _strip_title_noise(parts[3].strip())
                return result

        # site.YYYY.MM.DD.title (dot separated, full year)
        elif pattern == "dot_date":
            m = re.match(r'^(.+?)\.(20\d{2}|19\d{2})\.(\d{2})\.(\d{2})\.(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(clean(m.group(1)))
                result["date"]  = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
                result["title"] = _strip_title_noise(clean(m.group(5)))
                return result

        # site.YY.MM.DD.title (dot separated, short year)
        elif pattern == "dot_date_short":
            m = re.match(r'^(.+?)\.(\d{2})\.(\d{2})\.(\d{2})\.(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(clean(m.group(1)))
                result["date"]  = f"20{m.group(2)}-{m.group(3)}-{m.group(4)}"
                result["title"] = _strip_title_noise(clean(m.group(5)))
                return result

        # [Site] Title - Performer (YYYY)
        elif pattern == "bracket_site":
            m = re.match(r'^\[(.+?)\]\s*(.+?)(?:\s*-\s*(.+?))?\s*(?:\((\d{4})\))?$', stem)
            if m:
                result["site"]       = expand_abbrev(m.group(1).strip())
                result["title"]      = _strip_title_noise((m.group(2) or "").strip())
                result["performers"] = (m.group(3) or "").strip()
                result["date"]       = m.group(4) or ""
                return result

        # site - YYYY-MM-DD - title
        elif pattern == "dash_date":
            m = re.match(r'^(.+?)\s*[-–]\s*((?:20\d{2}|19\d{2})-\d{2}-\d{2})\s*[-–]\s*(.+)$', stem)
            if m:
                result["site"]  = expand_abbrev(m.group(1).strip())
                result["date"]  = m.group(2).strip()
                result["title"] = _strip_title_noise(m.group(3).strip())
                return result

    # Fallback: return cleaned stem as title
    result["title"] = _strip_title_noise(clean(stem))
    return result



def parse_filename(filename, settings):
    from app.core.matching import _split_query_from_library, _clean_filename_for_search
    from scene_relevance import split_title_words
    path = Path(filename)
    tag = re.match(r"^\[([^\]]+)\]\s*", path.name)
    folder_tag = bool(tag and tag.group(1).casefold() == path.parent.name.casefold())
    clean_input = str(path.with_name(path.name[tag.end():])) if folder_tag else filename
    # Apply user-configured noise stripping to both search paths.
    for word in settings.get('filename_strip_words', '').split(','):
        if word.strip():
            clean_input = re.sub(re.escape(word.strip()), '', clean_input, flags=re.IGNORECASE)
    parsed = _parse_filename_parts(clean_input, settings)
    full, library_title, _, studio, cast = _split_query_from_library(clean_input)
    structured = bool(parsed['site'] or parsed['performers']) and not folder_tag
    title = parsed['title'] if structured else library_title
    studio = studio or (parsed['site'] if structured else '')
    performers = cast or ([n.strip() for n in parsed['performers'].split(',') if n.strip()] if structured else [])
    for name in (sorted([*performers, studio, parsed['site']], key=len, reverse=True) if structured else []):
        letters = [c for c in name.casefold() if c.isalnum()]
        if letters:
            pattern = r"(?<!\w)" + r"[\W_]*".join(re.escape(c) for c in letters) + r"(?!\w)"
            title = re.sub(pattern, ' ', title, flags=re.IGNORECASE)
    title = split_title_words(_clean_filename_for_search(title + '.mp4'))
    date, _ = extract_filename_date(path.stem)
    if not date and parsed['date']:
        try:
            datetime.strptime(parsed['date'], '%Y' if len(parsed['date']) == 4 else '%Y-%m-%d')
            date = parsed['date']
        except ValueError:
            pass
    from app.core.matching import studio_search_name
    result = dict(site=studio_search_name(studio), date=date, performers=', '.join(performers), title=title,
                  full_query=full, performer_names=performers, parser_version=3)
    if folder_tag:
        result['folder_tag'] = True
    return result
