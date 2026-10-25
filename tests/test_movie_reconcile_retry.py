import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class MovieReconcileRetryTests(unittest.TestCase):
    def test_checks_both_roots_and_preserves_offline_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);movies=root/'movies';jav=root/'jav';movies.mkdir();jav.mkdir()
            (movies/'movie.mp4').touch();(jav/'jav.mp4').touch()
            conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row;self.addCleanup(conn.close)
            conn.execute('CREATE TABLE processed_movies(filename TEXT,status TEXT,processed_at TEXT)')
            conn.executemany("INSERT INTO processed_movies VALUES(?,'unmatched',NULL)",[(n,) for n in ('movie.mp4','jav.mp4','gone.mp4')])
            reconcile=load('database.py','reconcile_movies_source',{'get_conn':lambda:conn,'datetime':datetime})
            self.assertEqual(reconcile(movies,root/'offline'),0)
            self.assertEqual(reconcile(movies,jav),1)
            self.assertEqual(dict(conn.execute('SELECT filename,status FROM processed_movies').fetchall()),{'movie.mp4':'unmatched','jav.mp4':'unmatched','gone.mp4':'removed'})

    def test_deferred_retries_after_delay_without_new_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);video=root/'movie.mp4';video.touch();clock=SimpleNamespace(now=100)
            settings={'movies_source_dir':str(root),'folder_watch_hold_secs':'0'}
            process=Mock(side_effect=[{'status':'deferred'},{'status':'filed'}])
            env={'Path':Path,'time':SimpleNamespace(time=lambda:clock.now),'db':SimpleNamespace(get_settings=lambda:settings,get_movie_terminal_filenames=lambda:set(),rename_movie=Mock(),update_movie=Mock()),
                '_movie_pipeline_lock':threading.Lock(),'_pending_lock':threading.Lock(),'_pending_movie_files':{},'_movie_poll_seen':set(),
                'processing_state':{'running':False},'emit':Mock(),'process_movie_file':process,
                '_movie_source_video_relpaths':lambda *a:[('movie.mp4',video)],'_clean_and_rename_on_disk':lambda root,rel,**kw:rel}
            load('main.py','run_movie_pipeline',env)
            poll=load('main.py','_check_pending_movies',env)
            poll();self.assertEqual(process.call_count,1);self.assertEqual(env['_pending_movie_files'],{'movie.mp4':130})
            clock.now=129;poll();self.assertEqual(process.call_count,1)
            clock.now=130;poll();self.assertEqual(process.call_count,2);self.assertEqual(env['_pending_movie_files'],{})

    def test_retry_preserves_later_event_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            env={'Path':Path,'time':SimpleNamespace(time=lambda:100),'db':SimpleNamespace(get_settings=lambda:{'movies_source_dir':tmp}),
                '_movie_pipeline_lock':threading.Lock(),'_pending_lock':threading.Lock(),'_pending_movie_files':{'movie.mp4':500},
                'processing_state':{'running':False},'emit':Mock(),'process_movie_file':lambda *a:{'status':'deferred'}}
            load('main.py','run_movie_pipeline',env)(['movie.mp4'])
            self.assertEqual(env['_pending_movie_files']['movie.mp4'],500)
