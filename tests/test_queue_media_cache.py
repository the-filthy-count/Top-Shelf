import sqlite3
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import database as db
from app.core import queue_media

class QueueMediaCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.video=Path(self.tmp.name)/'video.mp4';self.video.write_bytes(b'video')
        self.c=sqlite3.connect(':memory:');self.c.row_factory=sqlite3.Row;self.addCleanup(self.c.close)
        for table in ('processed_files','processed_movies'):
            self.c.execute(f'CREATE TABLE {table}(filename TEXT UNIQUE,pipeline_match_json TEXT,phash TEXT,media_signature TEXT,media_duration REAL,media_codec TEXT,media_width INTEGER,media_height INTEGER,media_size_bytes INTEGER)')
        def upsert(table,key):self.c.execute(f'INSERT OR IGNORE INTO {table}(filename) VALUES(?)',(key,))
        for name,fn in [('get_conn',lambda:self.c),('upsert_file',lambda key:upsert('processed_files',key)),('upsert_movie',lambda key:upsert('processed_movies',key))]:
            p=patch.object(db,name,fn);p.start();self.addCleanup(p.stop)
    def test_reuses_and_invalidates_observations(self):
        duration=Mock(return_value=100.5);stream=Mock(return_value=('h264',1920,1080));phash=Mock(return_value='0123456789abcdef')
        def read():return queue_media.read('video.mp4',self.video,duration_probe=duration,stream_probe=stream,phash_probe=phash)
        read();read()
        self.assertEqual(duration.call_count,1);self.assertEqual(stream.call_count,1);self.assertEqual(phash.call_count,1)
        moved=self.video.with_name('renamed.mp4');self.video.rename(moved);self.video=moved
        read();self.assertEqual(duration.call_count,1)
        self.video.write_bytes(b'changed bytes');read()
        self.assertEqual(duration.call_count,2);self.assertEqual(phash.call_count,2)
    def test_movies_use_their_own_record(self):
        probe=Mock(return_value=30)
        for key in ('JAV/release.mp4','movie://feature.mp4'):
            queue_media.read(key,self.video,duration_probe=probe)
            queue_media.read(key,self.video,duration_probe=probe)
        self.assertEqual(probe.call_count,2)
        self.assertEqual(self.c.execute('SELECT count(*) FROM processed_files').fetchone()[0],0)
    def test_changed_during_probe_is_not_saved(self):
        def probe(video):video.write_bytes(b'longer changed');return 42
        with self.assertRaisesRegex(RuntimeError,'changed during'):
            queue_media.read('video.mp4',self.video,duration_probe=probe)
        self.assertIsNone(self.c.execute('SELECT media_duration FROM processed_files').fetchone()[0])

    def test_movie_fingerprint_and_match_survive_rename_but_not_content_changes(self):
        probe=Mock(return_value='hash')
        key='movie://feature.mp4'
        queue_media.read(key,self.video,phash_probe=probe)
        queue_media.save_match(key,self.video,{'movie':{'id':'verified'}}, expected_signature=db.media_file_signature(self.video))
        moved=self.video.with_name('moved.mp4');self.video.rename(moved);self.video=moved
        self.assertIsNotNone(queue_media.read(key,self.video,phash_probe=probe)['pipeline_match_json'])
        self.assertEqual(probe.call_count,1)
        self.video.write_bytes(b'new content')
        self.assertIsNone(queue_media.read(key,self.video,phash_probe=probe)['pipeline_match_json'])
        self.assertEqual(probe.call_count,2)

    def test_lookup_checkpoint_rejects_content_change(self):
        queue_media.read('video.mp4',self.video,phash_probe=lambda p:'hash')
        signature=db.media_file_signature(self.video)
        self.video.write_bytes(b'changed after lookup')
        with self.assertRaisesRegex(RuntimeError,'changed during lookup'):
            queue_media.save_match('video.mp4',self.video,{'scene':{'id':'wrong'}}, expected_signature=signature)
        self.assertIsNone(self.c.execute('SELECT pipeline_match_json FROM processed_files').fetchone()[0])

    def test_completed_probes_survive_hash_failure(self):
        for key in ('video.mp4','movie://movie.mp4'):
            duration=Mock(return_value=120);stream=Mock(return_value=('h264',1920,1080))
            fingerprint=Mock(side_effect=RuntimeError('hash failed'))
            for attempt in range(2):
                with self.assertRaisesRegex(RuntimeError,'hash failed'):
                    queue_media.read(key,self.video,duration_probe=duration,stream_probe=stream,fingerprint_probe=fingerprint)
            duration.assert_called_once();stream.assert_called_once()
            self.assertEqual(fingerprint.call_count,2)

    def test_refresh_cannot_validate_a_lookup_for_older_bytes(self):
        for key in ('video.mp4','movie://movie.mp4'):
            old=queue_media.read(key,self.video,phash_probe=lambda p:'old-hash')
            self.video.write_bytes(self.video.read_bytes()+b'new video')
            queue_media.read(key,self.video,duration_probe=lambda p:200)
            with self.assertRaisesRegex(RuntimeError,'changed during lookup'):
                queue_media.save_match(key,self.video,{'scene':{'id':'old-match'}},expected_signature=old['media_signature'])
            self.assertIsNone(queue_media.read(key,self.video)['pipeline_match_json'])
