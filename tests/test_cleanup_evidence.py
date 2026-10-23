import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class CleanupEvidenceTests(unittest.TestCase):
    def test_fuzzy_and_unknown_filing_cannot_remove_client_job(self):
        sources=['fuzzy','alias',None,'','exact','exact_filename']
        rows=[{'id':f'nzbget-h-{i}','status':'SUCCESS','name':'Release','filing':{'filed':True,'match_source':source}} for i,source in enumerate(sources)]
        remove=Mock(return_value={'ok':True})
        env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{'download_import_remove_client':'true'},download_cleanup_names=lambda:[],was_download_import_done=lambda i:False,mark_download_import_done=Mock()),
             '_cleanup_downloads_status':lambda:{'items':rows},'_download_remove_by_id':remove,
             '_watch_import_name_matches_client_job':lambda *a:False,'emit':Mock(),'_downloads_warm':SimpleNamespace(invalidate=Mock())}
        load('main.py','_retry_download_cleanup',env)()
        self.assertEqual([c.args[0] for c in remove.call_args_list],['nzbget-h-4','nzbget-h-5'])

    def test_filed_names_are_not_excluded_from_queue(self):
        conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row;self.addCleanup(conn.close)
        conn.execute('CREATE TABLE processed_movies(filename TEXT,status TEXT)')
        conn.executemany('INSERT INTO processed_movies VALUES(?,?)',[('new-copy.mp4','filed'),('ignored.mp4','skipped')])
        get=load('database.py','get_movie_terminal_filenames',{'get_conn':lambda:conn})
        self.assertEqual(get(),{'ignored.mp4'})

    def test_present_movie_reaches_fingerprint_despite_filed_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);video=root/'movie.mp4';video.touch()
            compute=Mock(return_value='fresh-hash')
            db=SimpleNamespace(media_file_signature=lambda p:'stable',get_settings=lambda:{},get_movie_by_filename=lambda n:{'status':'filed'},upsert_movie=Mock(),update_movie=Mock())
            env={'Path':Path,'db':db,'emit':Mock(),'require_complete_queue_video':lambda *a:None,
                 'PartialDownloadPending':type('PartialDownloadPending',(Exception,),{}),'compute_phash':compute,
                 '_queue_resolve_phash_vs_indexed_library':lambda *a,**k:{'status':'removed'}}
            result=load('main.py','process_movie_file',env)('movie.mp4',src=root)
            self.assertEqual(result,{'status':'removed'})
            compute.assert_called_once_with(video)
            db.update_movie.assert_called_once_with('movie.mp4',status='processing')

    def test_movie_filing_keeps_nested_history_key(self):
        db=SimpleNamespace(get_settings=lambda:{},upsert_movie=Mock(),update_movie=Mock())
        fn=load('main.py','_file_movie_impl',{'Path':Path,'db':db,'emit':Mock(),'require_complete_queue_video':lambda *a:None})
        for filename in ('first/movie.mp4', 'second/movie.mp4'):
            result=fn(Path('/inbox')/filename,{},history_filename=filename)
            self.assertEqual(result,{'status':'no_dir'})
            db.upsert_movie.assert_called_with(filename)
            self.assertEqual(db.update_movie.call_args.args[0],filename)

    def test_all_movie_filing_callers_pass_history_key(self):
        import ast
        tree=ast.parse(Path('main.py').read_text())
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='file_movie']
        self.assertEqual(len(calls),4)
        for call in calls:
            self.assertTrue(any(k.arg=='history_filename' and isinstance(k.value,ast.Name) and k.value.id in ('filename','history_filename') for k in call.keywords))
