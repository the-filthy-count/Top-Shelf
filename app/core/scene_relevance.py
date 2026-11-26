"""Title relevance for the manual scene search, independent of source ranking."""
import re
import unicodedata
from difflib import SequenceMatcher

_STOP = {'the', 'a', 'an', 'to', 'of', 'and', 'in', 'on', 'for', 'with'}

def split_title_words(value):
    """Recover word boundaries from CamelCase without guessing lowercase words."""
    text = str(value or '')
    text = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', text)
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', text)


def normal(value):
    text = unicodedata.normalize('NFKD', split_title_words(value)).casefold()
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
    from app.core.video_matching.decisions import title_score
    return title_score(title,query)

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
