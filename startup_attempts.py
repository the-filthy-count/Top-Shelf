"""Persistent successful attempt signatures used only to suppress boot replays."""
from pathlib import Path


def signature(path):
    st = Path(path).stat()
    return (st.st_size, st.st_mtime_ns)


def _table(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS startup_file_attempts (path TEXT PRIMARY KEY, size INTEGER, mtime_ns INTEGER)')


def unchanged(db, path):
    sig = signature(path)
    with db.get_conn() as conn:
        _table(conn)
        row = conn.execute('SELECT size,mtime_ns FROM startup_file_attempts WHERE path=?', (str(Path(path).absolute()),)).fetchone()
    return row is not None and tuple(row) == sig


def remember(db, path, before, result):
    if not isinstance(result, dict) or result.get('status') not in ('unmatched', 'filed'):
        return
    try:
        if signature(path) != before:
            return
    except FileNotFoundError:
        return  # Filed/moved files won't be replayed from this inbox.
    with db.get_conn() as conn:
        _table(conn)
        conn.execute('INSERT OR REPLACE INTO startup_file_attempts VALUES (?,?,?)', (str(Path(path).absolute()), *before))
