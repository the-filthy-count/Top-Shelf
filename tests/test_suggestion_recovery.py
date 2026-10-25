import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app.core import matching
from test_duplicate_filing_safety import load

class SuggestionRecoveryTests(unittest.TestCase):
    def connection(self):
        conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row
        conn.executescript('CREATE TABLE processed_files(filename TEXT,status TEXT,processed_at TEXT,media_duration INTEGER); CREATE TABLE queue_suggestions(filename TEXT); CREATE TABLE queue_suggestion_attempts(filename TEXT PRIMARY KEY,attempted_at REAL NOT NULL);')
        self.addCleanup(conn.close)
        return conn

    def test_duration_probe_preserves_unmatched_status(self):
        conn=self.connection()
        conn.execute("INSERT INTO processed_files VALUES('clip.mp4','unmatched','original',NULL)")
        cache=load('database.py','cache_media_duration',{'get_conn':lambda:conn})
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp)/'clip.mp4').touch()
            with patch.object(matching.db,'get_media_duration',return_value=None),patch.object(matching.db,'get_settings',return_value={'source_dir':tmp}),patch.object(matching.db,'cache_media_duration',cache),patch('app.core.phash.get_video_duration',return_value=600),patch.object(matching,'_split_query_for_search',return_value=('','','','',[])):
                matching._build_file_meta('clip.mp4')
        row=conn.execute('SELECT * FROM processed_files').fetchone()
        self.assertEqual((row['status'],row['processed_at'],row['media_duration']),('unmatched','original',600))

    def test_zero_result_searches_rotate_and_cool_down(self):
        conn=self.connection();env={'get_conn':lambda:conn}
        pending=load('database.py','queue_suggestion_pending',env)
        mark=load('database.py','queue_suggestion_mark_attempt',env)
        for i in range(8):conn.execute('INSERT INTO processed_files VALUES(?,?,?,NULL)',(str(i),'unmatched',str(i)))
        with patch('time.time',return_value=100000):
            first=pending()
            for name in first:mark(name)
            second=pending()
            self.assertEqual(len(first),5);self.assertEqual(len(second),3)
            self.assertFalse(set(first)&set(second))
            for name in second:mark(name)
            self.assertEqual(pending(),[])
        with patch('time.time',return_value=200000):self.assertEqual(len(pending()),5)

    def test_dates_removed_but_volume_numbers_retained(self):
        scene={'title':'Private Lesson 2','performers':[{'name':'Jane Doe'}],'release_date':'2026-10-14'}
        with patch.object(matching.db,'_library_guess_from_release_name',return_value={'performers':['Jane Doe'],'studio':'Example Studio'}):
            for date in ('20261014','2026-10-14','14.10.2026','26.10.14'):
                full,title,perf,studio,cast=matching._split_query_for_search(f'Example Studio Jane Doe {date} Private Lesson 2.mp4')
                self.assertEqual(title,'Private Lesson 2')
                self.assertEqual(matching._score_name_match(full,scene,query_title=title,query_performers=cast,file_meta={}),1)
                wrong=dict(scene,title='Private Lesson 3')
                self.assertEqual(matching._score_name_match(full,wrong,query_title=title,query_performers=cast,file_meta={}),0)
        self.assertIn('20260230',matching._clean_filename_for_search('20260230 Lesson 2.mp4'))
