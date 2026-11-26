"""The video workflow. Each transition consumes saved evidence before doing work.

Filesystem side effects remain in the filing service; revision and content
checks guard the boundary between an identity decision and a filing action.
"""
import asyncio
import json
from pathlib import Path
from app.core.queue_changes import assert_current
from app.core import queue_media
from .decisions import fingerprint
from .journal import Journal
from .providers import fingerprints


class MatchingEngine:
    def __init__(self, db, callbacks, *, phase=lambda _:None, emit=lambda _:None,
                 complete=None, duration=None, phash=None):
        self.db,self.cb,self.phase,self.emit = db,callbacks,phase,emit
        self.complete,self.duration,self.phash = complete,duration,phash

    def journal(self, filename, table='processed_files'):
        asset = self.db.file_identity(filename,table)
        if not asset:
            raise RuntimeError('Video has no permanent queue identity')
        return Journal(self.db,asset['id'])

    def validate(self, video, media):
        assert_current()
        queue_media.assert_signature(video,media['media_signature'])

    def analyse(self, key, video, settings):
        self.phase('Reading saved analysis')
        self.complete(video,settings)
        media = queue_media.read(key,video,duration_probe=self.duration,fingerprint_probe=self.phash)
        self.complete(video,settings)
        self.validate(video,media)
        if not media.get('phash'):
            raise RuntimeError('Video fingerprint is not available')
        return media

    def grab(self, filename, video):
        from app.core.matching import _strip_ts_uid
        for part in [video.name,*(p.name for p in list(video.parents)[:3])]:
            _,uid = _strip_ts_uid(part)
            if uid:
                found = self.db.lookup_scene_grab_by_ts_uid(uid)
                if found:
                    return found
        return self.db.lookup_scene_grab_for_filename(filename)

    async def scene(self, video):
        db = self.db
        settings = db.get_settings()
        filename = db.queue_file_key(video,settings)
        db.upsert_file(filename)
        record = db.queue_search_record(filename)
        journal = self.journal(filename)
        media = await asyncio.to_thread(self.analyse,filename,video,settings)
        db.update_file(filename,status='processing',phash=media['phash'])
        journal.write('analysis',media['media_signature'],result={'phash':media['phash'],'duration':media.get('media_duration')})
        self.phase('Checking library')
        duplicate = self.cb.get('queue_resolve_phash')
        if duplicate:
            early = await asyncio.to_thread(duplicate,video,filename,media['phash'])
            if early is not None:
                return early
        saved = json.loads(media.get('pipeline_match_json') or 'null')
        grab = None
        if not saved:
            self.phase('Resolving download identity')
            grab = self.grab(filename,video)
            if grab and (grab.get('kind') or 'scene').lower() == 'scene' and grab.get('source_id'):
                from app.core.filing import _stashbox_normalize_match_source
                source = _stashbox_normalize_match_source(grab.get('source_db') or '')
                try:
                    scene = await asyncio.to_thread(self.cb['fetch_grab_scene'],grab)
                except Exception as exc:
                    journal.write('download_identity',{'source':source,'id':grab['source_id']},error=exc)
                    scene = None
                if scene and str(scene.get('id') or '') != str(grab['source_id']):
                    journal.write('download_identity',{'source':source,'id':grab['source_id']},error='Provider returned a different scene ID')
                    scene = None
                if scene:
                    saved = {'scene':scene,'source':source,'evidence':'download_id'}
        if not saved:
            grab = None
            self.phase('Searching fingerprints')
            keys = {key:settings.get('api_key_'+key,'') for key in ('stashdb','tpdb','fansdb','javstash')}
            matches,source = await fingerprints(media['phash'],keys,self.emit,journal)
            self.validate(video,media)
            decision = fingerprint(matches,media['phash'])
            journal.write('decision',{'phash':media['phash'],'record':record},result={
                'state':decision.state,'reason':decision.reason,'source':source,
                'id':decision.candidate.get('id') if decision.candidate else None})
            if decision.state != 'matched':
                db.update_file(filename,status='unmatched')
                if decision.state == 'review':
                    from app.core.fingerprint_selection import save_review
                    save_review(db,filename,matches,source,media['phash'])
                    return {'status':'unmatched','review_required':True}
                await asyncio.to_thread(self.cb['queue_suggestions'],filename)
                prewarm = self.cb.get('prewarm_queue_thumbs')
                if prewarm:
                    await asyncio.to_thread(prewarm,filename)
                return {'status':'unmatched'}
            saved = {'scene':decision.candidate,'source':source,'evidence':'fingerprint'}
        self.validate(video,media)
        queue_media.save_match(filename,video,saved,expected_signature=media['media_signature'])
        scene = dict(saved['scene'])
        vice = self.cb.get('resolve_import_vice_dir')
        if vice:
            destination = vice(video)
            if destination:
                scene['_force_vice'] = destination
        self.phase('Filing verified identity')
        self.validate(video,media)
        result = await asyncio.to_thread(self.cb['file_scene_from_match'],video,scene,saved['source'])
        if grab and result.get('status') == 'filed':
            db.mark_scene_grab_consumed(grab.get('id'))
        return result

    def feature(self, filename, src=None, dest_kind=None):
        db,cb = self.db,self.cb
        settings = db.get_settings()
        if src is None:
            if not settings.get('movies_source_dir'):
                raise ValueError('Movies source directory is not configured')
            src = Path(settings['movies_source_dir']).expanduser()
        src = Path(src).resolve()
        video = src / filename
        if src not in video.resolve().parents:
            raise ValueError('Movie path escapes inbox')
        jav = settings.get('jav_download_watch_dir')
        is_jav = bool(jav and src == Path(jav).expanduser().resolve())
        key = 'jav://'+filename if is_jav else filename
        cache_key = key if is_jav else 'movie://'+key
        self.complete(video,{'source_dir':str(src)})
        db.upsert_movie(key)
        journal = self.journal(key,'processed_movies')
        media = self.analyse(cache_key,video,{'source_dir':str(src)})
        db.update_movie(key,status='processing')
        early = cb['queue_resolve_phash'](video,key,media['phash'],movie=True)
        if early is not None:
            return early
        saved = json.loads(media.get('pipeline_match_json') or 'null')
        if not saved:
            title,year = cb['movie_query'](filename)
            errors = []
            enabled = False
            for provider in cb['movie_providers']:
                if not settings.get('api_key_'+provider['key']):
                    continue
                enabled = True
                from .journal import digest
                inputs = {'title':title,'year':year,'credential':digest(settings['api_key_'+provider['key']])}
                try:
                    payload = journal.call('movie:'+provider['key'],inputs,lambda:provider['search'](title,year))
                    if payload:
                        saved = payload
                        break
                except Exception as exc:
                    from app.core.queue_changes import SupersededWork
                    if isinstance(exc,SupersededWork):
                        raise
                    errors.append(provider['key']+': '+str(exc))
            self.validate(video,media)
            if not saved:
                if errors or not enabled:
                    raise RuntimeError('Movie lookup incomplete: '+('; '.join(errors) or 'No movie databases configured'))
                db.update_movie(key,status='unmatched',error='No full title/year match')
                return {'status':'unmatched'}
        self.validate(video,media)
        queue_media.save_match(cache_key,video,saved,expected_signature=media['media_signature'])
        self.phase('Filing verified identity')
        self.validate(video,media)
        result = cb['file_movie'](video,saved['movie'],tpdb_id=saved.get('tpdb_id'),
            javstash_id=saved.get('javstash_id'),match_source=saved['source'],
            dest_kind=dest_kind,history_filename=key)
        cb['cleanup'](result,media['phash'])
        return result
