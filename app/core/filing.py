import re
import unicodedata


def _stashbox_normalize_match_source(ms: str) -> str:
    x = (ms or "").strip().lower()
    if x == "stashdb":
        return "StashDB"
    if x in ("tpdb", "theporndb"):
        return "TPDB"
    if x == "fansdb":
        return "FansDB"
    if x == "javstash":
        return "JAVStash"
    return (ms or "").strip() or "Manual"


def normalise(text: str) -> str:
    """Lowercase compare key for folder names vs API names
    (hyphens, dots, underscores → spaces; accents stripped)."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("-", " ").replace("_", " ").replace(".", " ")
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _performer_weight(fav_row: dict | None) -> int:
    """Scoring weight for the scene-filing router's performer pick.

    Resolution order:
      1. Manual override `weight` on the favourite row (1..10) if set,
      2. 2 when is_favourite is truthy (starred in /library),
      3. 1 otherwise (or when no DB row exists — e.g. folder-name-only match).
    """
    if not fav_row:
        return 1
    raw = fav_row.get("weight")
    try:
        if raw is not None and raw != "":
            w = int(raw)
            if 1 <= w <= 10:
                return w
    except (TypeError, ValueError):
        pass
    return 2 if int(fav_row.get("is_favourite") or 0) else 1


def _studio_crosswalk_ids(studio_obj: dict, source: str) -> tuple[str, str, str]:
    sid = str((studio_obj or {}).get("id") or "").strip()
    if not sid:
        return "", "", ""
    s = (source or "").upper()
    if "MANUAL" in s:
        return sid, sid, sid
    if "STASH" in s:
        return "", sid, ""
    if "FANS" in s:
        return "", "", sid
    return sid, "", ""


def _library_index_queue_matching_enabled(settings: dict) -> bool:
    """Prefer saved folder links from the library entity index when filing the queue."""
    v = settings.get("library_index_queue_matching_enabled")
    if v is None:
        v = settings.get("favourites_crosswalk_queue_enabled", "true")
    return str(v).lower() == "true"
