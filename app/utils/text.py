import re

UPPERCASE_KEYWORDS = {
    'tv','vip','mylf','la','als','bts','vk','ftv','omg','yngr','nvg','bbg',
    'bgg','bff','ddf','xxx','pov','dilf','bbc','dp','bj','bwc','pmv','atm',
    'cei','dap','pawg','cim','milf','milfs','bbw','enf','cbt','dt','bdsm',
    'jav','joi','cof','ffm','mmf','owo','bbbj','povd','brcc','vr','4k','hd',
    'atk','aj',
}

# ext4 / most Linux filesystems limit a single filename component to 255
# bytes (not codepoints). One Japanese character is 3 bytes in UTF-8, so
# an 85-kanji JAV title alone is enough to blow the limit. Cap each
# sanitised component well under that so render_pattern's separators +
# performer + studio + extension can still slot in around it.
_FS_COMPONENT_MAX_BYTES = 200
#: Cap for an entire assembled filename ("<base><ext>"). Slightly under
#: the kernel's 255 so renamer suffixes and .part stagings still fit.
FS_FILENAME_MAX_BYTES = 240


def _truncate_utf8(s: str, max_bytes: int) -> str:
    """Truncate *s* so its UTF-8 encoding is <= ``max_bytes``, dropping any
    partial trailing multi-byte sequence cleanly. Returns *s* unchanged
    when already within the limit."""
    if not s:
        return s
    enc = s.encode("utf-8")
    if len(enc) <= max_bytes:
        return s
    return enc[:max_bytes].decode("utf-8", errors="ignore").rstrip()


def apply_caps(text: str) -> str:
    return " ".join(w.upper() if w.lower() in UPPERCASE_KEYWORDS else w for w in text.split())


def render_pattern(pattern: str, fields: dict) -> str:
    result = pattern
    for key, value in fields.items():
        result = result.replace("{" + key + "}", str(value))
    return result


def _sanitize_fs_component(name: str) -> str:
    if not name or not str(name).strip():
        return "Unknown"
    t = str(name).strip()
    for ch in '<>:"/\\|?*':
        t = t.replace(ch, " ")
    t = re.sub(r"[\x00-\x1f]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = t.rstrip(". ")
    t = _truncate_utf8(t, _FS_COMPONENT_MAX_BYTES)
    return t or "Unknown"


def shorten_filename(name: str, ext: str = "", max_bytes: int = FS_FILENAME_MAX_BYTES) -> str:
    """Cap a full filename (without extension) so ``name + ext`` fits in
    ``max_bytes`` bytes on disk. Truncates ``name`` from the right at a
    UTF-8 character boundary; never touches the extension."""
    ext = ext or ""
    ext_bytes = len(ext.encode("utf-8"))
    if ext_bytes >= max_bytes:
        return _truncate_utf8(ext, max_bytes)
    budget = max_bytes - ext_bytes
    return _truncate_utf8(name, budget).rstrip()


# Recognised "special-feature" markers we lift out of source filenames
# so the filer can preserve them in the destination name + NFO. Order
# matters — the most specific pattern wins (e.g. "Behind The Scenes"
# outranks the bare "BTS" abbreviation so a filename containing both
# still resolves to a single canonical label). Each tuple is
# (compiled regex, canonical label).
EXTRA_KIND_PATTERNS = [
    (re.compile(r"\bbehind[\s._-]+the[\s._-]+scenes?\b", re.IGNORECASE), "BTS"),
    (re.compile(r"\bb\.?t\.?s\.?\b", re.IGNORECASE),                      "BTS"),
    (re.compile(r"\bbloopers?\b", re.IGNORECASE),                         "Bloopers"),
    (re.compile(r"\bouttakes?\b", re.IGNORECASE),                         "Outtakes"),
    (re.compile(r"\bmaking[\s._-]+of\b", re.IGNORECASE),                  "Making Of"),
    (re.compile(r"\binterviews?\b", re.IGNORECASE),                       "Interview"),
    (re.compile(r"\btrailers?\b", re.IGNORECASE),                         "Trailer"),
    (re.compile(r"\bdeleted[\s._-]+scenes?\b", re.IGNORECASE),            "Deleted Scenes"),
    (re.compile(r"\bphotoshoots?\b", re.IGNORECASE),                      "Photoshoot"),
    (re.compile(r"\bcommentary\b", re.IGNORECASE),                        "Commentary"),
    (re.compile(r"\b(?:bonus|extras?)\b", re.IGNORECASE),                 "Extras"),
]

#: Canonical label list, used by the manual override dropdown on the
#: Scene Search modal. Declaration order matches the regex priority
#: above so the picker reads top-to-bottom most-common first.
EXTRA_KIND_LABELS = [
    "BTS", "Bloopers", "Outtakes", "Making Of", "Interview",
    "Trailer", "Deleted Scenes", "Photoshoot", "Commentary", "Extras",
]


def detect_extra_kind(text: str) -> str:
    """Detect a BTS / Bloopers / Outtakes (etc.) marker in a filename or
    title. Returns the canonical label or "" when nothing matches.

    Patterns are word-boundary anchored so a studio name like
    "Bloopers Inc" doesn't false-positive on every file it produces."""
    if not text:
        return ""
    src = str(text)
    for rx, label in EXTRA_KIND_PATTERNS:
        if rx.search(src):
            return label
    return ""


def apply_extra_kind(base_name: str, nfo_title: str, tag_names, extra_kind: str):
    """Suffix a special-feature label onto a filed scene's filename + NFO
    title and ensure it appears in the tag list. Returns
    ``(base_name, nfo_title, tag_names)``. Idempotent: skips the suffix
    if the label is already present so a filename that already reads
    "Foo - BTS" doesn't become "Foo - BTS - BTS"."""
    out_tags = [t for t in (tag_names or [])]
    if not extra_kind:
        return base_name, nfo_title, out_tags
    label = str(extra_kind).strip()
    if not label:
        return base_name, nfo_title, out_tags
    low = label.lower()
    bracketed = f"[{label}]"
    new_base = base_name
    if low not in (base_name or "").lower():
        candidate = f"{base_name} {bracketed}" if base_name else bracketed
        new_base = shorten_filename(candidate)
    new_title = nfo_title
    if low not in (nfo_title or "").lower():
        new_title = f"{nfo_title} {bracketed}" if nfo_title else bracketed
    if not any((t or "").strip().lower() == low for t in out_tags):
        out_tags.append(label)
    return new_base, new_title, out_tags
