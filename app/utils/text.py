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
