import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch
from app.core.recovery import RecoveryStore, _signature

class PermanentDeletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.conn=sqlite3.connect(':memory:');self.addCleanup(self.conn.close)
        self.conn.executescript('CREATE TABLE file_assets(id TEXT,current_path TEXT NOT NULL); CREATE TABLE file_events(file_id TEXT,kind TEXT,detail TEXT);')
        self.conn.execute("INSERT INTO file_assets VALUES('a','original')");self.conn.commit()
        @contextmanager
        def connect():
            yield self.conn
        self.store=RecoveryStore(connect,self.root/'recovery',move=Mock(side_effect=AssertionError('Must not move')))
    def test_delete_video_and_sidecar_without_recovery_copy(self):
        paths=[self.root/'video.mp4',self.root/'video.nfo']
        for p in paths:p.write_bytes(b'123')
        result=self.store.delete(paths,'a',{},lambda c,ctx:None)
        self.assertEqual(result['freed_bytes'],6)
        self.assertFalse(result['recoverable'])
        self.assertTrue(all(not p.exists() for p in paths))
        self.assertFalse(self.store.root.exists())
        self.assertEqual(self.conn.execute('select current_path from file_assets').fetchone()[0],'')
    def test_validate_all_files_before_deleting_any(self):
        p=self.root/'video.mp4';p.write_bytes(b'123')
        with self.assertRaises(ValueError):self.store.delete([p,self.root/'missing'],'a',{},Mock())
        self.assertTrue(p.exists())
    def test_changed_identity_blocks_deletion(self):
        p=self.root/'video.mp4';p.write_bytes(b'123');sig=_signature(p);p.write_bytes(b'changed')
        with self.assertRaises(OSError):self.store.delete([p],'a',{'source_stats':{str(p):sig}},Mock())
        self.assertTrue(p.exists())
    def test_partial_failure_is_reported_without_committing_removal(self):
        paths=[self.root/'one',self.root/'two']
        for p in paths:p.touch()
        unlink=Path.unlink
        def fail_second(path,*a,**kw):
            if path==paths[1]:raise OSError('denied')
            return unlink(path,*a,**kw)
        commit=Mock()
        with patch.object(Path,'unlink',fail_second), self.assertRaises(OSError):
            self.store.delete(paths,'a',{},commit)
        commit.assert_not_called()
        self.assertEqual(self.conn.execute('select kind from file_events').fetchone()[0],'file.delete_failed')
        self.assertTrue(paths[1].exists())
