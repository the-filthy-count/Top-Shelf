"""Read-only library statistics. No media/filesystem scans or remote requests."""
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
import re
from statistics import mean, median
from threading import Lock
from time import monotonic


def key(value):
    return ' '.join(str(value or '').split()).casefold()


def source(value):
    value = key(value)
    return {'theporndb': 'tpdb', 'stash': 'stashdb', 'javstash.org': 'javstash'}.get(value, value)


def names(value):
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
        except (ValueError, TypeError):
            pass
    if isinstance(value, list):
        result = []
        for item in value:
            if isinstance(item, dict):
                item = item.get('tag', item.get('performer', item))
                item = item.get('name', '') if isinstance(item, dict) else item
            if isinstance(item, str) and item.strip():
                result.append(item.strip())
        return result
    return [part.strip() for part in str(value or '').split(',') if part.strip()]


def height_cm(value):
    text = str(value or '').strip().lower().replace('′', "'").replace('″', '"')
    feet = re.fullmatch(r"(\d)\s*(?:'|ft|feet)\s*(\d{1,2})?\s*(?:\"|in|inches)?", text)
    number = re.fullmatch(r'(\d+(?:\.\d+)?)\s*(cm|m|in|inches)?', text)
    if feet:
        inches = int(feet[2] or 0)
        if inches >= 12:
            return None
        result = (int(feet[1])*12+inches)*2.54
    elif number:
        result = float(number[1]) * {'m': 100, 'in': 2.54, 'inches': 2.54}.get(number[2], 1)
    else:
        return None
    return result if 100 <= result <= 250 else None


def weight_kg(value):
    text = str(value or '').strip().lower()
    match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*(kg|kgs|kilograms|lb|lbs|pounds)?', text)
    if not match:
        # Sources sometimes include both units; prefer explicit kilograms.
        match = re.search(r'\((\d+(?:\.\d+)?)\s*(kg)\)', text)
    if not match:
        return None
    result = float(match[1]) * (0.45359237 if match[2] in {'lb','lbs','pounds'} else 1)
    return result if 25 <= result <= 300 else None


def measurements(value):
    # Provider convention: 34D-26-36 is band(in), cup, waist(in), hips(in).
    # Metric triplets without a cup are bust/waist/hips; do not call bust a band.
    match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)([A-Za-z]{1,4})?\s*[-/]\s*(\d+(?:\.\d+)?)\s*[-/]\s*(\d+(?:\.\d+)?)\s*(cm|in|inches)?\s*', str(value or ''), re.I)
    if not match:
        return {}
    first, waist, hips = float(match[1]), float(match[3]), float(match[4])
    cup, unit = (match[2] or '').upper(), (match[5] or '').lower()
    if unit == 'cm':
        if cup: return {}  # bra sizing systems cannot be converted as lengths
        scale = 1
    elif unit in {'in','inches'} or (not unit and 20 <= first <= 60 and 15 <= waist <= 60 and 20 <= hips <= 80):
        scale = 2.54
    elif not unit and not cup and 60 <= first <= 180 and 40 <= waist <= 180 and 60 <= hips <= 200:
        scale = 1
    else:
        return {}
    result = {}
    if 40 <= waist*scale <= 200: result['waist'] = waist*scale
    if 50 <= hips*scale <= 230: result['hips'] = hips*scale
    if cup and scale == 2.54:
        result['band'] = first
        result['cup_size'] = cup
    return result


def gender_group(value):
    value = key(value).replace('_',' ').replace('-',' ')
    if value in {'female','woman','f'}: return 'female'
    if value in {'male','man','m'}: return 'male'
    if value in {'transgender','trans','trans female','trans male','transgender female','transgender male',
                 'trans woman','trans man','non binary','nonbinary','intersex','mixed','other'}:
        return 'mix'
    return None


def profiles(rows, today):
    groups = {name:[] for name in ('female','male','mix')}
    for row in rows:
        group = gender_group(row.get('gender'))
        if group: groups[group].append(row)
    return {name:profile(group,today) for name,group in groups.items() if group}


def numeric_mode(values):
    counts = Counter(round(value, 1) for value in values)
    peak = max(counts.values(), default=0)
    modes = sorted(value for value, count in counts.items() if count == peak)
    # No arbitrary representative for ties, including all-distinct samples.
    return {'mode': modes[0] if len(modes) == 1 else None,
            'modes': modes if peak > 1 or len(values) == 1 else []}


def profile(rows, today):
    numeric = {field:[] for field in ('age','height','weight','waist','hips','band','career_start_year')}
    categories = {field: Counter() for field in ('gender', 'hair_color', 'eye_color', 'country', 'ethnicity', 'cup_size')}
    labels = {}
    for row in rows:
        try:
            birthday = date.fromisoformat(row.get('birthdate', row.get('sort_birth_date')) or '')
            age = today.year-birthday.year-((today.month,today.day)<(birthday.month,birthday.day))
            if 18 <= age <= 100:
                numeric['age'].append(age)
        except (ValueError, TypeError):
            pass
        row = dict(row)
        measured = measurements(row.get('measurements'))
        if measured.get('cup_size'): row.setdefault('cup_size',measured['cup_size'])
        for field in ('waist','hips','band'):
            if field in measured: numeric[field].append(measured[field])
        weight = weight_kg(row.get('weight'))
        if weight is not None: numeric['weight'].append(weight)
        height = height_cm(row.get('height'))
        if height is not None:
            numeric['height'].append(height)
        year = str(row.get('career_start_year') or '')
        if re.fullmatch(r'\d{4}',year) and 1900 <= int(year) <= today.year:
            numeric['career_start_year'].append(int(year))
        for field, counts in categories.items():
            raw = str(row.get(field) or '').strip()
            normalized = key(raw)
            if normalized and normalized not in {'unknown', 'n/a', 'none', 'null', 'unspecified'}:
                counts[normalized] += 1
                labels.setdefault((field, normalized), raw.replace('_',' ').title())
    return {
        'population': len(rows),
        'numeric': {field: {'mean': round(mean(values),1) if values else None,
                            'median': round(median(values),1) if values else None,
                            'samples': len(values), **numeric_mode(values)} for field,values in numeric.items()},
        'categorical': {field: {'values': [labels[field,k] for k,v in sorted(counts.items()) if v == max(counts.values())],
                                'count': max(counts.values(),default=0), 'samples': sum(counts.values())}
                        for field,counts in categories.items()},
    }


def build(conn, today=None, folders=(), studio_filters=()):
    today = today or date.today()
    entities = [dict(r) for r in conn.execute('SELECT * FROM favourite_entities')]
    performers = {r['id']:r for r in entities if r['kind']=='performer'}
    aliases = defaultdict(set)
    canonical = {}
    for row in performers.values():
        label = row['folder_name']
        canonical[row['id']] = label
        for name in [label, *names(row.get('aliases_json')), *[row.get(f'match_{s}_name') for s in ('tpdb','stashdb','fansdb','javstash')]]:
            if key(name):
                aliases[key(name)].add(row['id'])
    for row in conn.execute('SELECT row_id, field, value FROM performer_bio_fields'):
        if row['row_id'] in performers:
            performers[row['row_id']][row['field']] = row['value']
    # The durable index includes pre-existing files and survives history pruning.
    # Only strings and database metadata are inspected; never stat media paths.
    excluded_roots=[]
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='settings'").fetchone():
        excluded_roots=[str(row['value']).rstrip('/') for row in conn.execute("SELECT value FROM settings WHERE key IN ('features_dir','jav_dir')") if row['value']]
    indexed = [dict(row) for row in conn.execute('SELECT * FROM library_files')]
    removed = {r['destination'] for r in indexed if r.get('is_removed')}
    history = [dict(row) for row in conn.execute("SELECT * FROM processed_files WHERE status='filed' ORDER BY processed_at DESC,id DESC")]
    by_id = {r['id']:r for r in history}
    by_path = {}
    for row in history:
        by_path.setdefault(row.get('destination'),row)
    entity_paths = {str(r.get('path') or '').rstrip('/'):r for r in entities if r.get('path')}
    def ancestors(path):
        parts=str(path or '').replace('\\','/').split('/')
        return ['/'.join(parts[:i]) for i in range(len(parts)-1,0,-1)]
    def membership(path):
        return [entity_paths[parent] for parent in ancestors(path) if parent in entity_paths]
    candidates=[]
    present=set()
    for item in indexed:
        dest=item['destination']
        if item.get('is_removed'):continue
        present.add(dest)
        row=dict(by_path.get(dest) or by_id.get(item.get('source_record_id')) or {})
        row.update(destination=dest, current_filename=item.get('current_filename') or dest.rsplit('/',1)[-1])
        row['processed_at']=row.get('processed_at') or ''  # Index discovery is not download time.
        candidates.append(row)
    # Newly filed files may not yet have reached the index.
    candidates.extend(r for r in history if r.get('destination') and r['destination'] not in present|removed)
    scenes={}
    folder_options={}
    for row in candidates:
        dest=row['destination']
        if any(dest.startswith(root+'/') for root in excluded_roots):continue
        owners=membership(dest)
        if any(r['kind'] in ('movie','jav') for r in owners):continue
        row=dict(row)
        row['_performers']={r['id'] for r in owners if r['kind']=='performer'}
        row['_folders']=set()
        for ident in row['_performers']:
            performer=performers[ident]
            folder=str(performer.get('root_label') or 'Unlabelled stars folder')
            row['_folders'].add(folder)
            folder_options[folder]=folder
        for name in names(row.get('performers')):
            ids=aliases.get(key(name),set())
            if len(ids)==1:row['_performers'].update(ids)
        studio=str(row.get('match_studio') or '').strip()
        studio_owner=next((r for r in owners if r['kind']=='studio'),None)
        if not studio and studio_owner:studio=studio_owner['folder_name']
        row['match_studio']=studio
        row['_release']=str(row.get('match_date') or '')
        # Our canonical scene filenames encode release date as SyyEmmdd.
        match=re.match(r'^(.+?) - S(\d{2})E(\d{2})(\d{2}) - ',row.get('current_filename') or dest.rsplit('/',1)[-1],re.I)
        if match:
            try:
                release=date(2000+int(match[2]),int(match[3]),int(match[4])).isoformat()
                row['_release']=row['_release'] or release
                if not studio and key(match[1]) not in aliases:row['match_studio']=match[1].strip()
            except ValueError:pass
        identity=(source(row.get('match_source')),str(row.get('match_external_id') or '').strip())
        if not all(identity):identity=('local',dest)
        if identity in scenes:
            scenes[identity]['_performers'].update(row['_performers'])
            scenes[identity]['_folders'].update(row['_folders'])
        else:scenes[identity]=row
    studio_options={key(r['match_studio']):r['match_studio'] for r in scenes.values() if r['match_studio']}
    selected_folders=set(folders)
    selected_studios={key(v) for v in studio_filters}
    scenes={identity:row for identity,row in scenes.items()
            if (not selected_folders or row['_folders'] & selected_folders)
            and (not selected_studios or key(row['match_studio']) in selected_studios)}
    if selected_folders or selected_studios:
        selected_people=set().union(*(row['_performers'] for row in scenes.values())) if scenes else set()
        performers={ident:row for ident,row in performers.items() if ident in selected_people}
    cast, studios, months = Counter(), Counter(), Counter()
    cast_labels, studio_labels = {}, {}
    matched = 0
    for row in scenes.values():
        seen = {('library',ident) for ident in row['_performers']}
        cast_labels.update({ident:canonical[ident[1]] for ident in seen})
        for name in names(row.get('performers')):
            ids = aliases.get(key(name),set())
            if len(ids) != 1:
                continue  # Rankings only include unambiguously saved library performers.
            ident = ('library', next(iter(ids)))
            seen.add(ident)
            cast_labels[ident] = canonical[ident[1]]
        cast.update(seen)
        if seen:
            matched += 1
        studio = str(row['match_studio'] or '').strip()
        if studio:
            studios[key(studio)] += 1
            studio_labels.setdefault(key(studio),studio)
        month = str(row['processed_at'] or '')[:7]
        if re.fullmatch(r'\d{4}-\d{2}',month):
            months[month] += 1
    # Only metadata linked to an actually filed scene contributes tags.
    scene_tags = defaultdict(dict)
    def collect(src, ident, tags, release=None):
        identity = (source(src), str(ident or '').strip())
        if identity not in scenes:
            return
        if release and not scenes[identity]['_release']:scenes[identity]['_release']=str(release)
        for tag in names(tags):
            scene_tags[identity].setdefault(key(tag), tag)
    for row in conn.execute("SELECT * FROM wanted_items WHERE kind='scene'"):
        collect(row['source'],row['external_id'],row['tags_json'],dict(row).get('release_date'))
    for row in conn.execute("SELECT payload_json FROM feed_display_pools WHERE kind='scenes'"):
        try:
            payload = json.loads(row['payload_json'])
        except (ValueError,TypeError):
            continue
        if not isinstance(payload,list):
            continue
        for scene in payload:
            if isinstance(scene,dict):
                collect(scene.get('source') or scene.get('_source'),scene.get('id'),scene.get('tags') or [],scene.get('date') or scene.get('release_date'))
    release_months=Counter()
    for row in scenes.values():
        value=row['_release']
        try:
            month=date.fromisoformat(value[:10] if len(value)>=10 else value+'-01').strftime('%Y-%m')
            release_months[month]+=1
        except (ValueError,TypeError):pass
    tags,tag_labels=Counter(),{}
    for values in scene_tags.values():
        tags.update(values.keys())
        tag_labels.update(values)
    def ranked(counts,labels):
        return [{'name':labels[k], 'count':v} for k,v in sorted(counts.items(),key=lambda item:(-item[1],key(labels[item[0]])))[:15]]
    # Every library performer contributes once, independent of scene count.
    return {
        'generated_at':datetime.now(timezone.utc).isoformat(),
        'totals': {'scenes':len(scenes),'performers':len(performers),'studios':len(studios),'tags':len(tags), 'library_performers':len(performers)},
        'coverage':{'cast':matched,'tags':sum(bool(v) for v in scene_tags.values()),'release_dates':sum(release_months.values())},
        'performers':ranked(cast,cast_labels),'studios':ranked(studios,studio_labels),'tags':ranked(tags,tag_labels),
        'months':[{'month':month,'count':months[month]} for month in sorted(months)[-12:]],
        'release_months':[{'month':month,'count':release_months[month]} for month in sorted(release_months)],
        'filters':{'folders':sorted(folder_options.values(),key=key),'studios':sorted(studio_options.values(),key=key)},
        'profile':profile(list(performers.values()),today),
        'profiles':profiles(list(performers.values()),today),
        'unknown_gender':sum(gender_group(row.get('gender')) is None for row in performers.values()),
    }

_lock=Lock()
_cached=None
_built=0.0
_selection=None

def snapshot(db, refresh=False, folders=(), studios=()):
    global _cached,_built,_selection
    selection=(tuple(sorted(set(folders))),tuple(sorted(set(studios))))
    with _lock:
        if selection != _selection or refresh or _cached is None or monotonic()-_built > 60:
            with db.get_conn() as conn:
                result=build(conn, folders=folders, studio_filters=studios)
            _cached=result
            _selection=selection
            _built=monotonic()
        return _cached
