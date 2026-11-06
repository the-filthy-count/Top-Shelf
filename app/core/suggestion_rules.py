"""Increment when candidate discovery or filename interpretation changes.

Old empty results become eligible for the existing bounded background scanner.
Saved suggestions and explicit rejections are retained.
"""
RULES_VERSION = 4


def date_proximity(scene_date, file_date):
    """Tie-break equally relevant scenes; missing/invalid dates carry no boost."""
    from datetime import date
    try:
        return -abs((date.fromisoformat(scene_date) - date.fromisoformat(file_date)).days)
    except (TypeError, ValueError):
        return float('-inf')
