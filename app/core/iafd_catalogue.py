"""IAFD title catalogue: source identities, cast and studios joined locally."""
from contextlib import closing
import json
import lzma
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time
from . import iafd_dataset as people

MIN_TITLE_YEAR = 2000
INDEX_VERSION = 5

def keep_title(item):
    # Unknown dates are retained; release date takes precedence over production.
    for field in ('release_date', 'production_date'):
        match = re.match(r'^(\d{4})(?:-|$)', str(item.get(field) or '').strip())
        if match and int(match.group(1)) > 0:
            return int(match.group(1)) >= MIN_TITLE_YEAR
    return True

def current_index():
    try:
        with closing(sqlite3.connect(f'{path().resolve().as_uri()}?mode=ro', uri=True)) as conn:
            return conn.execute('PRAGMA user_version').fetchone()[0] == INDEX_VERSION
    except sqlite3.Error:
        return False

def path():
    return people.path().with_name('iafd-title-dataset.sqlite')

def records(archive):
    with lzma.open(archive, 'rt', encoding='utf-8') as f:
        for line in f: yield json.loads(line)

def prune_single_credit_titles(c):
    # Freeze the exclusions before deleting: do not cascade removals as
    # retained performers lose credits from another performer's titles.
    c.execute('''CREATE TEMP TABLE excluded_people AS SELECT rid FROM people
        WHERE rid NOT IN (SELECT person FROM cast GROUP BY person HAVING count(*) >= 2)''')
    c.execute('''CREATE TEMP TABLE excluded_titles AS
        SELECT DISTINCT title FROM cast WHERE person IN (SELECT rid FROM excluded_people)''')
    c.execute('''INSERT INTO title_search(title_search,rowid,title)
        SELECT 'delete',rid,title FROM titles WHERE rid IN (SELECT title FROM excluded_titles)''')
    c.execute('DELETE FROM cast WHERE title IN (SELECT title FROM excluded_titles)')
    c.execute('DELETE FROM titles WHERE rid IN (SELECT title FROM excluded_titles)')
    c.execute('DELETE FROM names WHERE person IN (SELECT rid FROM excluded_people)')
    c.execute('DELETE FROM people WHERE rid IN (SELECT rid FROM excluded_people)')
    return c.execute('SELECT count(*) FROM titles').fetchone()[0]

def import_files(performers, studios, titles, target):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=target.parent, suffix='.sqlite'); os.close(fd)
    count = 0
    try:
        with closing(sqlite3.connect(temporary)) as c:
            c.execute('PRAGMA cache_size=-32768')
            c.executescript("""CREATE TABLE people(rid INTEGER PRIMARY KEY,id TEXT UNIQUE,data TEXT);
            CREATE TABLE names(name TEXT,person INTEGER,PRIMARY KEY(name,person)) WITHOUT ROWID;
            CREATE TABLE studios(id TEXT PRIMARY KEY,name TEXT) WITHOUT ROWID;
            CREATE TABLE titles(rid INTEGER PRIMARY KEY,id TEXT UNIQUE,title TEXT,studio TEXT,data TEXT);
            CREATE TABLE cast(person INTEGER,title INTEGER,PRIMARY KEY(person,title)) WITHOUT ROWID;
            CREATE VIRTUAL TABLE title_search USING fts5(title,content='titles',content_rowid='rid');""")
            identities = {}
            for p in records(performers):
                identity = str(p['source_reference'])
                c.execute('INSERT OR IGNORE INTO people(id,data) VALUES(?,?)',(identity,people.encode({k:v for k,v in people.compact_performer(p).items() if k != 'image_url'})))
                rid = c.execute('SELECT rid FROM people WHERE id=?',(identity,)).fetchone()[0]
                identities[identity] = rid
                c.executemany('INSERT OR IGNORE INTO names VALUES(?,?)',[(people.key(n),rid) for n in [p.get('name','')]+(p.get('aliases') or []) if isinstance(n,str) and people.key(n)])
            for s in records(studios):
                c.execute('INSERT OR REPLACE INTO studios VALUES(?,?)',(str(s['source_reference']),s.get('name') or ''))
            for t in records(titles):
                if not keep_title(t): continue
                identity = str(t['source_reference'])
                # Cast and studio relationships live once, outside the JSON.
                data = {k:t[k] for k in ('urls','details','release_date','duration','cover_image_url','studio_code','director','tags') if t.get(k)}
                c.execute('INSERT INTO titles(id,title,studio,data) VALUES(?,?,?,?)',
                    (identity,t.get('title') or '',str((t.get('studio') or {}).get('source_reference','')),people.encode(data)))
                rid = c.execute('SELECT last_insert_rowid()').fetchone()[0]
                c.execute('INSERT INTO title_search(rowid,title) VALUES(?,?)',(rid,t.get('title') or ''))
                c.executemany('INSERT OR IGNORE INTO cast VALUES(?,?)',[(identities[str(p['source_reference'])],rid) for p in t.get('performers',[]) if str(p.get('source_reference','')) in identities])
                count += 1
            if not count: raise ValueError('Empty title catalogue')
            count = prune_single_credit_titles(c)
            c.execute('CREATE INDEX cast_title ON cast(title,person)')
            c.execute(f'PRAGMA user_version={INDEX_VERSION}')
            c.commit()
            c.execute('VACUUM')
        os.replace(temporary,target)
        return count
    finally:
        if os.path.exists(temporary): os.unlink(temporary)

def source_urls(item):
    return [u['url'] for u in item.get('urls',[]) if isinstance(u,dict) and str(u.get('url','')).startswith('https://www.iafd.com/')]

def connect():
    conn=sqlite3.connect(f'{path().resolve().as_uri()}?mode=ro',uri=True)
    conn.row_factory=sqlite3.Row
    return conn

def shape(c, row):
    t=json.loads(row['data']);urls=source_urls(t)
    if not urls: return None
    cast=[]
    for p_row in c.execute('SELECT p.id,p.data FROM cast x JOIN people p ON p.rid=x.person WHERE x.title=? ORDER BY p.id',(row['rid'],)):
        p=json.loads(p_row['data'])
        cast.append({'performer':{'id':p_row['id'],'name':p['name'],'aliases':p.get('aliases') or [],'urls':[{'url':u} for u in source_urls(p)]}})
    studio=c.execute('SELECT name FROM studios WHERE id=?',(row['studio'],)).fetchone()
    image=t.get('cover_image_url') or ''
    return {'id':'iafd:'+urls[0], '_iafd_url':urls[0], '_iafd_dataset':True,
            'title':row['title'], 'details':t.get('details') or '',
            'release_date':t.get('release_date') or '', 'duration':t.get('duration') or 0,
            'studio':{'id':row['studio'],'name':studio[0] if studio else ''}, 'performers':cast,
            'images':[{'url':image}] if image else [], 'urls':[{'url':u} for u in urls],
            'code':t.get('studio_code') or '', 'director':t.get('director') or '', 'tags':t.get('tags') or []}

def search(query='', performer='', studio='', date_from='', date_to='', limit=20, offset=0, performer_id=''):
    if not path().exists() or not any((query,performer,studio,performer_id)): return []
    with closing(connect()) as c:
        clauses=[];params=[]
        words=re.findall(r'\w+',query or '',re.UNICODE)
        if words:
            clauses.append('t.rid IN (SELECT rowid FROM title_search WHERE title_search MATCH ?)')
            params.append(' AND '.join('"'+word+'"' for word in words))
        if performer_id:
            clauses.append('t.rid IN (SELECT title FROM cast WHERE person IN (SELECT rid FROM people WHERE id=?))');params.append(performer_id)
        elif performer:
            clauses.append('t.rid IN (SELECT title FROM cast WHERE person IN (SELECT person FROM names WHERE name=?))');params.append(people.key(performer))
        if studio:
            clauses.append('t.studio IN (SELECT id FROM studios WHERE lower(name)=lower(?))');params.append(studio)
        if date_from: clauses.append("json_extract(t.data,'$.release_date')>=?");params.append(date_from)
        if date_to: clauses.append("json_extract(t.data,'$.release_date')<=?");params.append(date_to)
        sql='SELECT t.* FROM titles t'+(' WHERE '+' AND '.join(clauses) if clauses else '')+" ORDER BY json_extract(t.data,'$.release_date') DESC,t.id LIMIT ? OFFSET ?"
        rows=c.execute(sql,params+[max(1,min(limit,100000)),max(0,offset)]).fetchall()
        return [result for row in rows if (result:=shape(c,row))]

def detail(url):
    if not path().exists(): return None
    identity = url.split('/id=')[-1].split('?')[0].rstrip('/')
    with closing(connect()) as c:
        row=c.execute('SELECT * FROM titles WHERE id=?',(identity,)).fetchone()
        return shape(c,row) if row else None

def refresh():
    if path().exists() and current_index() and time.time()-path().stat().st_mtime < 7*86400: return
    path().parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=path().parent) as directory:
        files=[]
        for kind in ('performers','studios','scenes'):
            file=Path(directory)/(kind+'.xz'); files.append(file)
            with people.requests.get(people.URL.replace('iafd-performers','iafd-'+kind),stream=True,timeout=(10,60)) as response:
                response.raise_for_status()
                with file.open('wb') as out:
                    for chunk in response.iter_content(1024*1024): out.write(chunk)
        import_files(*files,path())

def filmography(performer_id):
    """Return title summaries without expanding every cast member again."""
    if not path().exists(): return []
    with closing(connect()) as c:
        rows=c.execute("""SELECT t.title,t.data,s.name FROM cast x JOIN titles t ON t.rid=x.title
            LEFT JOIN studios s ON s.id=t.studio
            WHERE x.person IN (SELECT rid FROM people WHERE id=?)
            ORDER BY json_extract(t.data,'$.release_date') DESC,t.id""", (performer_id,)).fetchall()
    out=[]
    for title,data,studio in rows:
        item=json.loads(data);urls=source_urls(item)
        if urls:
            out.append({'title':title, 'url':urls[0], 'year':(item.get('release_date') or '')[:4], 'studio':studio or ''})
    return out
