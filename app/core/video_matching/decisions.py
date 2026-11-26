"""Pure matching policy. Retrieval order is never identity evidence."""
from dataclasses import dataclass
import re
import math
import unicodedata
from difflib import SequenceMatcher


@dataclass(frozen=True)
class Decision:
    state: str
    candidate: dict | None = None
    reason: str = ''


def normal(value):
    text = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', str(value or ''))
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = unicodedata.normalize('NFKD', text).casefold()
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = text.replace("'", '').replace('’', '').replace('&', ' and ')
    return ' '.join(re.findall(r'[^\W_]+', text))


def distance(scene, phash):
    value = str(phash or '')
    if not re.fullmatch(r'[a-fA-F0-9]{16}', value):
        return None
    hashes = scene.get('fingerprints') or scene.get('hashes') or []
    if isinstance(hashes, dict):
        hashes = [{'algorithm': k, 'hash': v} for k,v in hashes.items()]
    found = []
    for item in hashes:
        if not isinstance(item, dict):
            continue
        hash_value = str(item.get('hash') or item.get('value') or '')
        if str(item.get('algorithm') or item.get('type')).upper() == 'PHASH' and re.fullmatch(r'[a-fA-F0-9]{16}', hash_value):
            found.append((int(value,16) ^ int(hash_value,16)).bit_count())
    return min(found) if found else None


def fingerprint(rows, phash):
    by_id = {}
    for row in rows or []:
        if not isinstance(row, dict) or not row.get('id'):
            continue
        key = str(row['id'])
        rank = distance(row, phash)
        old = by_id.get(key)
        if old is None or (rank is not None and (old[0] is None or rank < old[0])):
            by_id[key] = (rank, row)
    if not by_id:
        return Decision('miss', reason='No fingerprint candidates')
    if len(by_id) == 1:
        return Decision('matched', next(iter(by_id.values()))[1], 'Unique provider fingerprint result')
    if any(rank is None for rank,_ in by_id.values()):
        return Decision('review', reason='Competing results with incomplete fingerprint evidence')
    best = min(rank for rank,_ in by_id.values())
    winners = [row for rank,row in by_id.values() if rank == best]
    return (Decision('matched', winners[0], 'Unique closest fingerprint') if len(winners) == 1
            else Decision('review', reason='Equally close fingerprints identify different scenes'))


def movie(rows, title, year=None):
    wanted = normal(title)
    candidates = {}
    for row in rows or []:
        if not isinstance(row, dict) or not wanted:
            continue
        if wanted not in {normal(row.get(field)) for field in ('title','original_title','name')}:
            continue
        actual_year = str(row.get('year') or row.get('release_date') or row.get('date') or '')[:4]
        if year and actual_year != str(year):
            continue
        identity = row.get('id') or row.get('_id')
        if identity:
            candidates[str(identity)] = row
    if len(candidates) == 1:
        return Decision('matched', next(iter(candidates.values())), 'Unique full title and release year')
    return Decision('review' if candidates else 'miss', reason='Ambiguous title' if candidates else 'No full title/year agreement')


def title_score(title, query):
    title, query = normal(title), normal(query)
    if not title or not query:
        return None
    if title == query:
        return 1.0
    if set(re.findall(r'\d+',title)) != set(re.findall(r'\d+',query)):
        return None
    if title.replace(' ','') == query.replace(' ',''):
        return 1.0
    stop = {'the','a','an','to','of','and','in','on','for','with'}
    wanted, actual = set(query.split())-stop, set(title.split())-stop
    if not wanted or not actual:
        return None
    if len(actual) >= 2 and f' {title} ' in f' {query} ':
        return .96
    overlap = len(wanted & actual)
    coverage, precision = overlap/len(wanted), overlap/len(actual)
    if coverage >= .8 and precision >= .65:
        return .7 + .2 * coverage * precision
    similarity = SequenceMatcher(None,title,query).ratio()
    if min(len(wanted),len(actual)) >= 2 and similarity >= .9:
        return similarity * .9
    return None


def number(value):
    try:
        result = float(value or 0)
        return result if math.isfinite(result) and result > 0 else 0
    except (TypeError, ValueError, OverflowError):
        return 0


def score_suggestion(query, candidate, *, query_performer='', query_title=None,
                     query_performers=None, require_performers_in_cast=True,
                     allow_joint_cast=False, file_meta=None, reasons=None):
    from app.core.matching import _required_performers_in_cast, _candidate_performer_names
    from app.core.scene_relevance import title_query
    names = list(query_performers or ([query_performer] if query_performer else []))
    has_cast = bool(names and _required_performers_in_cast(names,candidate))
    if require_performers_in_cast and names and not has_cast:
        return 0.0
    query = title_query(query if query_title is None else query_title, ', '.join(names))
    query = re.sub(r'\b(?:scene|sc)\.?\s*\d+\b',' ',query,flags=re.I).strip()
    titles = [candidate.get('title')]
    for item in candidate.get('movies') or []:
        if isinstance(item,dict):
            item = item.get('movie') or item
            titles.append(item.get('title') or item.get('name'))
    score = max((title_score(title,query) or 0 for title in titles), default=0)
    explanation = []
    if not score:
        if allow_joint_cast and len(names)>1 and has_cast:
            explanation.append({'kind':'cast','label':'All detected performers present; title unconfirmed','ok':True})
            score = .5
        else:
            return 0.0
    else:
        explanation.append({'kind':'title','label':f'Title relevance {round(score*100)}%','ok':True})
        meta = file_meta or {}
        duration, actual = number(meta.get('duration_s')), number(candidate.get('duration'))
        if duration and actual:
            delta = abs(duration-actual)
            score *= 1-min(.4,max(0,delta-60)/600*.4)
            explanation.append({'kind':'duration','label':f'Duration difference {round(delta)}s','ok':delta<=120})
        year = str(candidate.get('release_date') or '')[:4]
        if year.isdigit() and meta.get('year'):
            difference = abs(int(year)-int(meta['year']))
            score *= 1-min(.25,max(0,difference-1)*.05)
            explanation.append({'kind':'year','label':f'Year {year}','ok':difference<=1})
        cast_hint = int(meta.get('cast_hint') or 0)
        if cast_hint:
            score *= 1-min(.2,max(0,len(_candidate_performer_names(candidate))-cast_hint)*.05)
    explanation.extend({'kind':'performer','label':name,'ok':_required_performers_in_cast([name],candidate)} for name in names)
    if reasons is not None:
        reasons.extend(explanation)
    return min(1.0,score)
