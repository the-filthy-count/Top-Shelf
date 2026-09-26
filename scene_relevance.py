"""Title relevance for the manual scene search, independent of source ranking."""
import re
import unicodedata
from difflib import SequenceMatcher

_STOP = {'the', 'a', 'an', 'to', 'of', 'and', 'in', 'on', 'for', 'with'}

def normal(value):
    text = unicodedata.normalize('NFKD', str(value or '')).casefold()
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = text.replace("'", '').replace('’', '').replace('&', ' and ')
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', text).split())

def title_query(title, performers=''):
    query = normal(title)
    for name in re.split(r'[,|]', performers or ''):
        name = normal(name)
        if not name:
            continue
        if query == name:
            return ''
        if query.startswith(name + ' '):
            query = query[len(name):].strip()
        elif query.endswith(' ' + name):
            query = query[:-len(name)].strip()
    return query

def _score(title, query):
    title = normal(title)
    if not title or not query:
        return None
    if title == query:
        return 1.0
    qt, tt = query.split(), title.split()
    qn, tn = set(re.findall(r'\b\d+\b', query)), set(re.findall(r'\b\d+\b', title))
    if qn != tn and (qn or tn):
        return None
    significant_q = set(qt) - _STOP
    significant_t = set(tt) - _STOP
    if not significant_q or not significant_t:
        return None
    # A complete multiword title inside a release string may have a performer prefix.
    if len(significant_t) >= 2 and (' '+title+' ') in (' '+query+' '):
        return .96
    coverage = len(significant_q & significant_t) / len(significant_q)
    precision = len(significant_q & significant_t) / len(significant_t)
    if coverage >= .8 and precision >= .65:
        return .7 + .2 * coverage * precision
    # Tolerate small spelling differences, not a lone shared word.
    ratio = SequenceMatcher(None, title, query).ratio()
    if min(len(significant_q), len(significant_t)) >= 2 and ratio >= .9:
        return ratio * .9
    return None

def rank_manual_results(rows, query, performers=''):
    query = title_query(query, performers)
    if not query:
        return list(rows)
    ranked = []
    for index, row in enumerate(rows):
        titles = [row.get('title')]
        for item in row.get('movies') or []:
            if isinstance(item, dict):
                movie = item.get('movie') or item
                if isinstance(movie, dict):
                    titles.append(movie.get('title') or movie.get('name'))
        scores = [_score(title, query) for title in titles if title]
        scores = [score for score in scores if score is not None]
        if scores:
            ranked.append((max(scores), index, row))
    return [row for _, _, row in sorted(ranked, key=lambda item: (-item[0], item[1]))]
