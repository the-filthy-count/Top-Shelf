"""Persistent, indexed IAFD performer snapshot with atomic background refresh."""
from contextlib import closing
import json
import logging
import lzma
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unicodedata
import requests
import database as db

URL = 'https://github.com/stash-of-awesomeness/performer-datasets/raw/refs/heads/main/iafd-performers.jsonl.xz'
INDEX_VERSION = 2

def compact_performer(item):
    # These are the only fields read by performer search and cast rendering.
    return {k: item[k] for k in ('name', 'aliases', 'urls', 'image_url') if item.get(k)}

def encode(item):
    return json.dumps(item, ensure_ascii=False, separators=(',', ':'))

def current_index():
    try:
        with closing(sqlite3.connect(f'{path().resolve().as_uri()}?mode=ro', uri=True)) as conn:
            return conn.execute('PRAGMA user_version').fetchone()[0] == INDEX_VERSION
    except sqlite3.Error:
        return False

_lock = threading.Lock()
_thread = None

def path():
    return Path(db.DB_PATH).parent / 'iafd-performer-dataset.sqlite'

def key(value):
    return ''.join(c for c in unicodedata.normalize('NFKC', value).casefold() if c.isalnum())

def import_file(archive, destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=destination.parent, suffix='.sqlite')
    os.close(fd)
    count = 0
    try:
        with closing(sqlite3.connect(temporary)) as conn:
            conn.executescript('CREATE TABLE performers(id TEXT PRIMARY KEY, data TEXT NOT NULL); CREATE TABLE names(name TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(name,id));')
            with lzma.open(archive, 'rt', encoding='utf-8') as stream:
                for line in stream:
                    item = json.loads(line)
                    identity = str(item.get('source_reference') or '')
                    name = item.get('name')
                    if not identity or not isinstance(name, str) or not name.strip(): continue
                    aliases = [a for a in item.get('aliases', []) if isinstance(a, str)]
                    conn.execute('INSERT OR REPLACE INTO performers VALUES(?,?)', (identity,encode(compact_performer(item))))
                    conn.executemany('INSERT OR IGNORE INTO names VALUES(?,?)', [(key(n),identity) for n in [name]+aliases if key(n)])
                    count += 1
            if not count: raise ValueError('Empty IAFD dataset')
            conn.execute(f'PRAGMA user_version={INDEX_VERSION}')
            conn.commit()
        os.replace(temporary, destination)
        return count
    finally:
        if os.path.exists(temporary): os.unlink(temporary)

def search(name):
    normalized = key(name or '')
    target = path()
    if not normalized or not target.exists(): return []
    try:
        with closing(sqlite3.connect(f'{target.resolve().as_uri()}?mode=ro', uri=True)) as conn:
            rows = conn.execute('SELECT p.id,p.data FROM names n JOIN performers p ON p.id=n.id WHERE n.name=? ORDER BY p.id', (normalized,)).fetchall()
        results = []
        for identity, data in rows:
            item = json.loads(data)
            urls = [u['url'] for u in item.get('urls', []) if isinstance(u,dict) and u.get('site','').upper() == 'IAFD' and str(u.get('url','')).startswith('https://www.iafd.com/')]
            if not urls: continue
            results.append({'id':identity, 'name':item['name'], 'aliases':item.get('aliases') or [],
                            'urls':urls, 'thumb_url':item.get('image_url') or '', 'dataset':True})
        return results
    except (sqlite3.Error, ValueError, OSError):
        logging.exception('IAFD dataset search failed; using live fallback')
        return []

def search_with_fallback(name, fallback):
    return search(name) or fallback(name)

def refresh():
    with _lock:
        target = path()
        if target.exists() and current_index() and time.time() - target.stat().st_mtime < 7*86400: return
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target.parent) as directory:
            archive = Path(directory)/'performers.xz'
            with requests.get(URL, stream=True, timeout=(10,60)) as response:
                response.raise_for_status()
                with archive.open('wb') as output:
                    for chunk in response.iter_content(1024*1024): output.write(chunk)
            import_file(archive, target)

def retain_catalogue_performers():
    """Apply the catalogue's frozen eligibility set to the headshot/search index."""
    from . import iafd_catalogue
    if not iafd_catalogue.current_index() or not path().exists(): return
    with _lock, closing(sqlite3.connect(iafd_catalogue.path())) as catalogue:
        eligible = [(r[0],) for r in catalogue.execute('SELECT id FROM people')]
        target = path(); previous = target.stat()
        fd, temporary = tempfile.mkstemp(dir=target.parent, suffix='.sqlite'); os.close(fd)
        try:
            with closing(sqlite3.connect(target)) as original, closing(sqlite3.connect(temporary)) as conn:
                original.backup(conn)
                conn.execute('CREATE TEMP TABLE eligible(id TEXT PRIMARY KEY)')
                conn.executemany('INSERT INTO eligible VALUES(?)',eligible)
                removed = conn.execute('DELETE FROM performers WHERE id NOT IN (SELECT id FROM eligible)').rowcount
                conn.execute('DELETE FROM names WHERE id NOT IN (SELECT id FROM eligible)')
                conn.commit()
                if not removed: return
                conn.execute('VACUUM')
            os.replace(temporary,target)
            # Pruning must not postpone the next scheduled source refresh.
            os.utime(target,(previous.st_atime,previous.st_mtime))
        finally:
            if os.path.exists(temporary): os.unlink(temporary)

def start():
    global _thread
    if _thread and _thread.is_alive(): return
    def run():
        while True:
            try: refresh()
            except Exception: logging.exception('IAFD dataset refresh failed; retaining previous snapshot')
            try:
                from . import iafd_catalogue
                iafd_catalogue.refresh()
                retain_catalogue_performers()
            except Exception: logging.exception('IAFD catalogue refresh failed; retaining previous snapshot')
            time.sleep(86400)
    _thread = threading.Thread(target=run, name='iafd-dataset', daemon=True)
    _thread.start()
