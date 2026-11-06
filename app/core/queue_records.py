"""Authoritative search details attached to stable queue file identities.

Technical observations remain in processed_files; this record stores parsed
search fields separately from explicit user corrections, never filenames.
"""
import json
from datetime import date
import database as db
from app.core.matching import studio_search_name

FIELDS = ('title', 'performers', 'studio', 'date', 'scene_number', 'term', 'description', 'tags', 'extra_kind')
SOURCE_FIELDS = {'TPDB':'match_tpdb_id','StashDB':'match_stashdb_id',
                 'FansDB':'match_fansdb_id','JAVStash':'match_javstash_id'}


def install(conn):
    for table in ('processed_files', 'processed_movies'):
        columns = {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
        for column, kind in [('media_duration','INTEGER'),('media_size_bytes','INTEGER'),('media_codec','TEXT'),('media_width','INTEGER'),('media_height','INTEGER')]:
            if column not in columns:
                conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {kind}')
    conn.execute("""CREATE TABLE IF NOT EXISTS queue_search_details (
        file_id TEXT PRIMARY KEY, parsed_json TEXT NOT NULL DEFAULT '{}',
        overrides_json TEXT NOT NULL DEFAULT '{}', signature TEXT NOT NULL DEFAULT '',
        revision INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")


def _asset(filename, file_id=None):
    from app.core.queue_recovery import resolve_target
    filename, kind = resolve_target(file_id, filename)
    if kind != 'scene':
        raise ValueError('Search details require a scene file')
    table = 'processed_movies' if filename.startswith('JAV/') else 'processed_files'
    key = 'jav://' + filename[4:] if table == 'processed_movies' else filename
    asset = db.file_identity(key, table)
    if not asset:
        from pathlib import Path
        settings = db.get_settings()
        root = Path(settings.get('source_dir') or '')
        video = db.resolve_source_video_path(root, filename)
        if not video or not video.is_file():
            raise ValueError('File is not registered in the queue; refresh the queue')
        (db.upsert_movie if table == 'processed_movies' else db.upsert_file)(key)
        asset = db.file_identity(key, table)
    with db.get_conn() as conn:
        row = conn.execute(f'SELECT * FROM {table} WHERE id=?', (asset['queue_id'],)).fetchone()
    if not row or row['status'] == 'removed':
        raise ValueError('File is no longer in the queue')
    return filename, asset, dict(row)


def _links(names, kind):
    compact = lambda text: ''.join(c for c in text.casefold() if c.isalnum())
    with db.get_conn() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM favourite_entities WHERE kind=? AND COALESCE(is_group,0)=0", (kind,))]
    result = []
    for name in names:
        matches = [r for r in rows if compact(r['folder_name'] or '') == compact(name)]
        link = {'name': name, 'library_id': None, 'source_ids': {}}
        if len(matches) == 1:
            row = matches[0]
            link.update(library_id=row['id'], source_ids={src: str(row[field]) for src, field in SOURCE_FIELDS.items() if row.get(field)})
        result.append(link)
    return result


def read(filename='', file_id=None, *, settings=None, parser_signature=None):
    from app.core.filename_parser import parse_filename, cache_signature
    from app.core.matching import _extract_scene_number_from_filename
    filename, asset, technical = _asset(filename, file_id)
    settings = db.get_settings() if settings is None else settings
    signature = (cache_signature(settings) if parser_signature is None else parser_signature) + ':' + filename
    with db.get_conn() as conn:
        stored = conn.execute('SELECT * FROM queue_search_details WHERE file_id=?', (asset['id'],)).fetchone()
    if not stored or stored['signature'] != signature:
        raw = parse_filename(filename, settings)
        parsed = dict(title=raw['title'], performers=raw['performer_names'], studio=raw['site'],
                      date=raw['date'], scene_number=_extract_scene_number_from_filename(filename), term='', description='', tags=[], extra_kind='__auto__')
        with db.get_conn() as conn:
            conn.execute("""INSERT INTO queue_search_details(file_id,parsed_json,signature)
                VALUES(?,?,?) ON CONFLICT(file_id) DO UPDATE SET
                parsed_json=excluded.parsed_json,signature=excluded.signature,revision=revision+1""",
                         (asset['id'], json.dumps(parsed), signature))
            stored = conn.execute('SELECT * FROM queue_search_details WHERE file_id=?', (asset['id'],)).fetchone()
    parsed = json.loads(stored['parsed_json'])
    overrides = json.loads(stored['overrides_json'])
    effective = dict(description='', tags=[], extra_kind='__auto__')
    effective.update(parsed)
    effective.update(overrides)
    effective["studio"] = studio_search_name(effective.get("studio"))
    # Resolve against the library now, retaining links saved with manual edits.
    links = effective.get('performer_links') or _links(effective.get('performers', []), 'performer')
    studio_links = effective.get('studio_link') or (_links([effective['studio']], 'studio')[0] if effective.get('studio') else None)
    return dict(file_id=asset['id'], filename=filename, revision=stored['revision'],
                fields={key:effective.get(key) for key in FIELDS},
                performer_links=links, studio_link=studio_links,
                edited=bool(overrides), technical=technical,
                identity={key:asset[key] for key in ('original_path','original_known','current_path','first_seen')})


def save(filename, file_id, fields, revision, reset=False):
    record = read(filename, file_id)
    if not isinstance(fields, dict):
        raise ValueError('Search fields must be an object')
    values = {}
    if not reset:
        for key in FIELDS:
            value = fields.get(key, [] if key in ('performers', 'tags') else '')
            if key in ('performers', 'tags'):
                if not isinstance(value, list) or len(value) > 100 or any(not isinstance(n, str) or len(n) > 200 for n in value):
                    raise ValueError('Provide a list of names for ' + key)
                values[key] = list(dict.fromkeys(n.strip() for n in value if n.strip()))
            elif key == 'scene_number':
                if value in ('', None):
                    values[key] = None
                else:
                    try:
                        number = int(value)
                    except (ValueError, TypeError):
                        raise ValueError('Scene number must be a positive whole number')
                    if str(number) != str(value) or not 1 <= number <= 9999:
                        raise ValueError('Scene number must be a positive whole number')
                    values[key] = number
            else:
                if not isinstance(value, str) or len(value) > (20000 if key == 'description' else 1000):
                    raise ValueError('Invalid search field: ' + key)
                values[key] = value.strip()
        if values['date']:
            try:
                if len(values['date']) == 4:
                    date(int(values['date']), 1, 1)
                else:
                    date.fromisoformat(values['date'])
            except ValueError:
                raise ValueError('Use a valid YYYY-MM-DD date or YYYY year')
        values['performer_links'] = _links(values['performers'], 'performer')
        values['studio_link'] = _links([values['studio']], 'studio')[0] if values['studio'] else None
    with db.get_conn() as conn:
        conn.execute('BEGIN IMMEDIATE')
        cur = conn.execute("""UPDATE queue_search_details SET overrides_json=?,revision=revision+1,
            updated_at=CURRENT_TIMESTAMP WHERE file_id=? AND revision=?""",
                           (json.dumps(values), record['file_id'], revision))
        if not cur.rowcount:
            raise ValueError('Search details changed in another window; reopen this file')
        # New corrections become eligible immediately. Explicit rejections survive.
        conn.execute('DELETE FROM queue_suggestion_attempts WHERE filename=?', (record['filename'],))
        conn.execute('DELETE FROM queue_suggestions WHERE filename=?', (record['filename'],))
    return read(file_id=record['file_id'])


def saved_ids(record):
    return {p['name']: {source: {value} for source, value in p.get('source_ids', {}).items()}
            for p in record['performer_links']}


def observe_media(filename, video, codec=None, width=None, height=None, duration=None):
    """Save probe observations beside the fingerprint, without touching corrections."""
    _, asset, stored = _asset(filename)
    size = video.stat().st_size
    values = {'media_size_bytes': size, 'media_codec': codec, 'media_width': width,
              'media_height': height, 'media_duration': duration}
    if all(value is None or stored.get(key) == value for key, value in values.items()):
        return
    with db.get_conn() as conn:
        conn.execute(f"""UPDATE {asset['queue_table']} SET media_size_bytes=?,
            media_codec=COALESCE(?,media_codec),media_width=COALESCE(?,media_width),
            media_height=COALESCE(?,media_height),media_duration=COALESCE(?,media_duration)
            WHERE id=?""", (size, codec, width, height, duration, asset['queue_id']))
