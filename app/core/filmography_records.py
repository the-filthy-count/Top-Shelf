"""Relational filmography storage and exact library ownership joins."""
import re
import time
import database as db


def create_schema(conn):
    schema = '''
    CREATE TABLE IF NOT EXISTS filmography_scenes (
      source TEXT NOT NULL, scene_id TEXT NOT NULL, title TEXT NOT NULL,
      release_date TEXT NOT NULL, studio TEXT NOT NULL, url TEXT NOT NULL,
      PRIMARY KEY(source, scene_id));
    CREATE TABLE IF NOT EXISTS filmography_credits (
      source TEXT NOT NULL, performer_id TEXT NOT NULL, scene_id TEXT NOT NULL,
      PRIMARY KEY(source, performer_id, scene_id),
      FOREIGN KEY(source,scene_id) REFERENCES filmography_scenes(source,scene_id));
    CREATE TABLE IF NOT EXISTS filmography_fingerprints (
      source TEXT NOT NULL, scene_id TEXT NOT NULL, algorithm TEXT NOT NULL,
      hash TEXT NOT NULL, duration REAL,
      PRIMARY KEY(source,scene_id,algorithm,hash),
      FOREIGN KEY(source,scene_id) REFERENCES filmography_scenes(source,scene_id));
    CREATE INDEX IF NOT EXISTS filmography_fingerprint_lookup ON filmography_fingerprints(algorithm,hash);
    CREATE TABLE IF NOT EXISTS filmography_sync (
      source TEXT NOT NULL, performer_id TEXT NOT NULL, updated_at REAL NOT NULL,
      state TEXT NOT NULL, PRIMARY KEY(source,performer_id));
    '''
    for statement in schema.split(';'):
        if statement.strip():
            conn.execute(statement)


def fingerprints(items):
    result = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        algorithm = str(item.get('algorithm') or item.get('type') or '').upper()
        value = str(item.get('hash') or item.get('value') or '').lower().strip()
        if not algorithm or not value:
            continue
        if algorithm == 'PHASH' and not re.fullmatch(r'[0-9a-f]{16}', value):
            continue
        duration = item.get('duration')
        try:
            duration = float(duration) if duration is not None else None
        except (ValueError, TypeError):
            duration = None
        result.append({'algorithm': algorithm, 'hash': value, 'duration': duration})
    return result


def save(source, performer_id, scenes, updated_at=None):
    source = source.lower()
    with db.get_conn() as conn:
        for scene in scenes:
            sid = str(scene['id'])
            conn.execute('''INSERT INTO filmography_scenes VALUES(?,?,?,?,?,?)
              ON CONFLICT(source,scene_id) DO UPDATE SET title=excluded.title,
              release_date=excluded.release_date,studio=excluded.studio,url=excluded.url''',
              (source,sid,scene.get('title') or '',scene.get('date') or '',scene.get('studio') or '',scene.get('url') or ''))
            for fp in fingerprints(scene.get('fingerprints')):
                conn.execute('INSERT OR REPLACE INTO filmography_fingerprints VALUES(?,?,?,?,?)',
                             (source,sid,fp['algorithm'],fp['hash'],fp['duration']))
                # Keep existing matching/search consumers supplied with PHASH data.
                if fp['algorithm'] == 'PHASH':
                    conn.execute('''INSERT OR REPLACE INTO scene_fingerprints
                        (source,scene_id,phash,algorithm,duration,fetched_at)
                        VALUES(?,?,?,?,?,datetime('now'))''',
                        (source,sid,fp['hash'],fp['algorithm'],fp['duration']))
        conn.execute('DELETE FROM filmography_credits WHERE source=? AND performer_id=?',(source,performer_id))
        conn.executemany('INSERT OR IGNORE INTO filmography_credits VALUES(?,?,?)',
                         [(source,performer_id,str(scene['id'])) for scene in scenes])
        conn.execute('INSERT OR REPLACE INTO filmography_sync VALUES(?,?,?,?)',
                     (source,performer_id,time.time() if updated_at is None else updated_at,'Complete'))


def failed(source, performer_id):
    with db.get_conn() as conn:
        conn.execute('''INSERT INTO filmography_sync VALUES(?,?,0,'Refresh failed')
          ON CONFLICT(source,performer_id) DO UPDATE SET state='Refresh failed' ''',(source,performer_id))


def read(ids, labels):
    scenes, statuses, times = {}, [], []
    with db.get_conn() as conn:
        for source, performers in ids.items():
            for performer in performers:
                sync = conn.execute('SELECT * FROM filmography_sync WHERE source=? AND performer_id=?',(source,performer)).fetchone()
                if sync:
                    statuses.append({'source':labels[source][0], 'state':sync['state']})
                    times.append(sync['updated_at'])
                else:
                    times.append(0)
                for row in conn.execute('''SELECT s.* FROM filmography_scenes s JOIN filmography_credits c
                    ON s.source=c.source AND s.scene_id=c.scene_id WHERE c.source=? AND c.performer_id=?''',(source,performer)):
                    scenes[(source,row['scene_id'])] = dict(id=row['scene_id'],source=labels[source][0],
                        title=row['title'],date=row['release_date'],year=row['release_date'][:4],studio=row['studio'],url=row['url'])
        # Batch library lookup, never title-based and never filesystem probes.
        owned_ids = {(r['source'],r['scene_id']) for r in conn.execute('''
          SELECT lower(p.match_source) source,p.match_external_id scene_id FROM processed_files p
          JOIN library_files l ON l.destination=p.destination
          WHERE l.is_removed=0 AND p.status IN ('success','filed')''')}
        hashes = {str(r['hash']).lower() for r in conn.execute('''
          SELECT phash_1 hash FROM library_files WHERE is_removed=0 AND phash_1 IS NOT NULL
          UNION SELECT phash_2 FROM library_files WHERE is_removed=0 AND phash_2 IS NOT NULL
          UNION SELECT phash_3 FROM library_files WHERE is_removed=0 AND phash_3 IS NOT NULL''')}
        # Only read fingerprints for this filmography, in bounded SQL batches.
        keys = list(scenes)
        for offset in range(0,len(keys),200):
            chunk = keys[offset:offset+200]
            where = ' OR '.join('(source=? AND scene_id=?)' for _ in chunk)
            for fp in conn.execute('SELECT * FROM filmography_fingerprints WHERE '+where,[v for pair in chunk for v in pair]):
                scene = scenes[(fp['source'],fp['scene_id'])]
                scene.setdefault('fingerprints',[]).append(dict(algorithm=fp['algorithm'],hash=fp['hash'],duration=fp['duration']))
                if fp['algorithm']=='PHASH' and fp['hash'] in hashes:
                    scene['in_library']=True
        for key, scene in scenes.items():
            scene['in_library']=bool(scene.get('in_library') or key in owned_ids)
    return {'films':list(scenes.values()),'sources':statuses,'updated_at':min(times) if times else 0}
