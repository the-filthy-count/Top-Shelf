"""Bounded search alternatives for lowercase, space-free release titles."""
import re
from functools import lru_cache


@lru_cache(maxsize=1024)
def title_search_queries(title: str) -> tuple[str, ...]:
    import wordninja

    def separate(match):
        token = match.group()
        words = wordninja.split(token)
        # Never discard characters or invent a query from single-letter fragments.
        if len(words) < 2 or ''.join(words) != token:
            return token
        if any(len(word) == 1 and word not in {'a', 'i'} for word in words):
            return token
        return ' '.join(words)

    # Keep original search as well: word boundaries can be ambiguous. Numbers,
    # punctuation and names outside the extracted title are not rewritten.
    if len(title) > 512:
        return (title,)
    spaced = re.sub(r'(?<![\w])[a-z]{6,128}(?![a-zA-Z_])', separate, title)
    if spaced != title:
        spaced = re.sub(r"(?<=[a-z])(?=[0-9])", " ", spaced)
    return (title, spaced) if spaced != title else (title,)
