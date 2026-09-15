"""Conservative, offline matching for IAFD bulk scans."""
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import PurePosixPath


def normal(text):
    text = unicodedata.normalize("NFKD", str(text or "")).casefold()
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace("&", " and ").replace("'", "").replace("’", "")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def clean_query(text, performer="", studio=""):
    text = normal(text)
    for label in (performer, studio):
        token = normal(label)
        if token:
            text = re.sub(r"(?<!\w)" + re.escape(token) + r"(?!\w)", " ", text)
    text = re.sub(r"\b(?:scene|sc|cd|disc|disk|part|pt)\s*\d+\b", " ", text)
    text = re.sub(r"\b(?:19|20)\d{2}\b", " ", text)
    text = re.sub(r"\b(?:2160p|1080p|1080i|720p|480p|4k|xxx|mp4|mkv|avi|wmv|x264|x265|h264|h265|hevc|dvdrip|bdrip|bluray|webrip|web dl)\b", " ", text)
    return re.sub(r"^(?:the|a|an) ", "", " ".join(text.split()))


def choose_film(films, parsed, filename, performer):
    """Return a unique strong candidate; reject sequel conflicts and ties."""
    stem = PurePosixPath(str(filename).replace("\\", "/")).stem
    queries = {clean_query(q, performer, parsed.get("site", ""))
               for q in (parsed.get("title", ""), stem) if q}
    queries.discard("")
    year_match = re.search(r"\b(?:19|20)\d{2}\b", str(parsed.get("date") or "") or normal(stem))
    year = year_match.group() if year_match else ""
    ranked = {}
    for film in films:
        title = re.sub(r"^(?:the|a|an) ", "", normal(film.get("title")))
        if not title or not film.get("url"):
            continue
        scores = []
        for query in queries:
            # Different volume numbers are different films, even if all words match.
            if re.findall(r"\b\d+\b", title) != re.findall(r"\b\d+\b", query):
                continue
            if query == title:
                scores.append(0.0)
            elif len(title.split()) >= 2 and (" " + title + " ") in (" " + query + " "):
                scores.append(1.0 + (len(query.split()) - len(title.split())) / max(len(query.split()), 1))
            elif min(len(query.split()), len(title.split())) >= 3:
                similarity = SequenceMatcher(None, title, query).ratio()
                if similarity >= .94:
                    scores.append(3.0 + 1 - similarity)
        if not scores:
            continue
        score = (min(scores), 0 if year and str(film.get("year")) == year else 1,
                 0 if normal(parsed.get("site")) and normal(parsed.get("site")) == normal(film.get("studio")) else 1)
        old = ranked.get(film["url"])
        if old is None or score < old[0]:
            ranked[film["url"]] = (score, film)
    candidates = sorted(ranked.values(), key=lambda item: item[0])
    if not candidates:
        return None
    if len(candidates) > 1:
        first, second = candidates[0][0], candidates[1][0]
        if first == second or (first[0] >= 3 and second[0] >= 3 and abs(first[0] - second[0]) < .03):
            return None
    return candidates[0][1]


def choose_scene(scenes, performer, number=None):
    if number is not None:
        matches = [i for i, scene in enumerate(scenes) if str(scene.get("number")) == str(number)]
    else:
        matches = [i for i, scene in enumerate(scenes)
                   if any(normal(name) == normal(performer) for name in scene.get("cast", []))]
    return matches[0] if len(matches) == 1 else -1


def film_suggestions(films, parsed, filename, performer, limit=5):
    """Review-only shortlist with evidence; never changes automatic selection."""
    stem = PurePosixPath(str(filename).replace("\\", "/")).stem
    queries = [clean_query(q, performer, parsed.get("site", ""))
               for q in (parsed.get("title", ""), stem) if q]
    queries = [q for q in queries if q]
    year_hit = re.search(r"\b(?:19|20)\d{2}\b", str(parsed.get("date") or "") or normal(stem))
    year = year_hit.group() if year_hit else ""
    suggestions = {}
    for film in films:
        title = re.sub(r"^(?:the|a|an) ", "", normal(film.get("title")))
        if not title or not film.get("url") or not queries:
            continue
        query = max(queries, key=lambda q: SequenceMatcher(None, title, q).ratio())
        similarity = SequenceMatcher(None, title, query).ratio()
        if similarity < .45:
            continue
        reasons = ["Exact cleaned title" if title == query else f"Title similarity {round(similarity * 100)}%"]
        numbers_match = re.findall(r"\b\d+\b", title) == re.findall(r"\b\d+\b", query)
        if not numbers_match:
            reasons.append("Volume numbers differ — check manually")
        year_match = bool(year and str(film.get("year")) == year)
        if year:
            reasons.append("Year matches" if year_match else "Year differs or is unknown")
        studio_match = bool(normal(parsed.get("site")) and normal(parsed.get("site")) == normal(film.get("studio")))
        if studio_match:
            reasons.append("Studio matches")
        item = {k: film.get(k, "") for k in ("url", "title", "year", "studio")}
        item["reasons"] = reasons
        score = (numbers_match, similarity, year_match, studio_match)
        if film["url"] not in suggestions or score > suggestions[film["url"]][0]:
            suggestions[film["url"]] = (score, item)
    return [item for _, item in sorted(suggestions.values(), key=lambda row: row[0], reverse=True)[:limit]]
