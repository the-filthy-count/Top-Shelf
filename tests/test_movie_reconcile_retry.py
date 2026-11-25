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

    def test_movie_admission_uses_shared_dispatcher_and_does_not_reset_watch_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            enqueue=Mock();run=Mock()
            env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{'movies_source_dir':tmp},upsert_movie=Mock(),processing_job_enqueue=enqueue),
                '_pending_movie_files':{'movie.mp4':500},'run_pipeline':run}
            load('main.py','run_movie_pipeline',env)(['movie.mp4'])
            enqueue.assert_called_once_with('movie.mp4','processed_movies')
            run.assert_called_once_with([])
            self.assertEqual(env['_pending_movie_files']['movie.mp4'],500)
