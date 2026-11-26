"""Adapt legacy isolated API tests to the replacement engine's dependencies.

Durable journals have their own real-SQLite integration tests. These fixtures
keep API and filesystem tests isolated without importing the running app.
"""
from unittest.mock import Mock, patch
from test_duplicate_filing_safety import load


def load_movie(env):
    for name in ('guess_movie_query_from_filename','file_movie','_cleanup_superseded_library_copies','_queue_resolve_phash_vs_indexed_library'):
        env.setdefault(name,Mock())
    env.setdefault('get_video_duration',Mock())
    env.setdefault('PartialDownloadPending',__import__('download_safety').PartialDownloadPending)
    env['db'].file_identity=lambda *a:{'id':'fixture'}
    function=load('main.py','_process_movie_file_impl',env)
    def run(*args,**kwargs):
        from download_safety import PartialDownloadPending
        def validate(video,signature):
            if env['db'].media_file_signature(video)!=signature:
                raise PartialDownloadPending('Video changed')
        with patch('app.core.video_matching.engine.Journal') as journal,patch('app.core.queue_media.assert_signature',side_effect=validate):
            journal.return_value.call.side_effect=lambda stage,inputs,callback:callback()
            return function(*args,**kwargs)
    return run


def load_suggestions(env):
    from app.core import matching
    database=env['db']
    original=database.queue_search_record
    def record(filename):
        saved=original(filename)
        if saved:
            return saved
        query,title,performer,studio,names=env['_split_query_for_search'](filename)
        return {'file_id':'fixture','filename':filename,'fields':{
            'title':title,'performers':names or ([performer] if performer else []),
            'studio':studio,'term':'','date':env.get('_build_file_meta',lambda f:{})(filename).get('date',''),'scene_number':None},'performer_links':[]}
    database.queue_search_record=record
    database.get_settings=lambda:{}
    for name in ('search_scenes_with_all_performers','_prefetch_suggestion_breakdowns_async'):
        env.setdefault(name,Mock(return_value=([],True) if name.startswith('search_') else None))
    env.setdefault('_scene_passes_settings_filters',lambda s:True)
    env.setdefault('_normalize_suggestion',lambda s,f:{'source_db':s['_source'],'source_id':s['id'],'payload':s})
    function=load('main.py','_run_name_search_for_file',env)
    def run(*args,**kwargs):
        with patch('app.core.video_matching.suggestions.Journal') as journal,patch.object(matching,'_build_file_meta',env.get('_build_file_meta',lambda f:{})):
            journal.return_value.call.side_effect=lambda stage,inputs,callback:callback()
            return function(*args,**kwargs)
    return run
