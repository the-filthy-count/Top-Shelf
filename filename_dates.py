"""Calendar-validated dates explicitly present in release filenames."""
import re
from datetime import date, datetime


def extract_filename_date(text):
    """Return (ISO date, matched text); leave ambiguous day/month dates alone."""
    patterns = [
        (r'(?<!\d)((?:19|20)\d{2})(\d{2})(\d{2})(?!\d)', 'ymd'),
        (r'(?<!\d)((?:19|20)\d{2})[. _-]+(\d{1,2})[. _-]+(\d{1,2})(?!\d)', 'ymd'),
        (r'(?<!\d)(\d{1,2})[. _-]+(\d{1,2})[. _-]+((?:19|20)\d{2})(?!\d)', 'endyear'),
        (r'(?<!\d)(\d{2})[._-](\d{2})[._-](\d{2})(?!\d)', 'short'),
    ]
    for pattern, order in patterns:
        for match in re.finditer(pattern, text):
            a,b,c = map(int, match.groups())
            if order == 'ymd': y,m,d = a,b,c
            elif order == 'short': y,m,d = 2000+a,b,c
            elif a > 12: y,m,d = c,b,a
            elif b > 12: y,m,d = c,a,b
            elif a == b: y,m,d = c,b,a
            else: continue
            try: return date(y,m,d).isoformat(), match.group()
            except ValueError: continue
    return '', ''
