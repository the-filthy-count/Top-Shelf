import re

UPPERCASE_KEYWORDS = {
    'tv','vip','mylf','la','als','bts','vk','ftv','omg','yngr','nvg','bbg',
    'bgg','bff','ddf','xxx','pov','dilf','bbc','dp','bj','bwc','pmv','atm',
    'cei','dap','pawg','cim','milf','milfs','bbw','enf','cbt','dt','bdsm',
    'jav','joi','cof','ffm','mmf','owo','bbbj','povd','brcc','vr','4k','hd',
    'atk','aj',
}


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
    return t or "Unknown"
