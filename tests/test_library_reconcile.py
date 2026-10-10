import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import library_reconcile as lr

class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.old = self.root / 'video.mp4'
        self.new = self.root / 'video.mkv'
        self.old.write_bytes(b'original')
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        self.conn.executescript('''CREATE TABLE library_files (
          id INTEGER PRIMARY KEY, destination TEXT UNIQUE, library_root TEXT,
          current_filename TEXT, filename_stem TEXT, phash_1 TEXT, phash_2 TEXT,
          phash_2_size INTEGER, phash_2_mtime_ns INTEGER, phash_3 TEXT, phash_3_scanned_at TEXT,
          media_mtime REAL, file_created_iso TEXT, media_codec TEXT, media_width INTEGER,
          media_height INTEGER, is_removed INTEGER, removed_at TEXT, updated_at TEXT);
          CREATE TABLE processed_files (destination TEXT, destination_base TEXT, destination_ext TEXT);
          CREATE TABLE processed_movies (destination TEXT);''')
        self.conn.execute('INSERT INTO library_files(id,destination,library_root,phash_1) VALUES(1,?,?,?)', (str(self.old),str(self.root),'original-hash'))
        self.conn.execute('INSERT INTO processed_files(destination) VALUES(?)',(str(self.old),))
        self.conn.commit()
        self.db=SimpleNamespace(get_conn=lambda:self.conn,_iso_now=lambda:'now',library_file_get_by_destination=lambda p:dict(self.conn.execute('SELECT * FROM library_files WHERE destination=?',(p,)).fetchone()))
        self.probe=lambda row,p:(p.stat().st_mtime,'today','h264',1920,1080)
        lr._observed.clear();lr._pending.clear();lr._errors.clear()
    def row(self):return dict(self.conn.execute('SELECT * FROM library_files WHERE id=1').fetchone())
    def test_changed_file_gets_new_hash_but_keeps_original(self):
        self.assertTrue(lr.refresh(self.db,self.row(),self.old,lambda p:'second',self.probe))
        self.assertFalse(lr.needs_hash(self.row(),self.old))
        self.old.write_bytes(b'new longer contents')
        self.assertTrue(lr.needs_hash(self.row(),self.old))
        self.assertTrue(lr.refresh(self.db,self.row(),self.old,lambda p:'third',self.probe))
        self.assertEqual(self.row()['phash_1'],'original-hash')
        self.assertEqual(self.row()['phash_2'],'third')
    def test_extension_change_keeps_identity_and_pipeline_link(self):
        self.old.rename(self.new)
        self.assertEqual(lr.replacement(self.row(),[self.row()],{'.mp4','.mkv'}),self.new)
        self.assertTrue(lr.refresh(self.db,self.row(),self.new,lambda p:'new',self.probe))
        self.assertEqual(self.row()['id'],1)
        self.assertEqual(self.conn.execute('SELECT destination_ext FROM processed_files').fetchone()[0],'mkv')
    def test_changing_during_hash_is_not_saved(self):
        def compute(p):p.write_bytes(b'changed during read');return 'bad'
        self.assertFalse(lr.refresh(self.db,self.row(),self.old,compute,self.probe))
        self.assertIsNone(self.row()['phash_2'])
    def test_ambiguous_replacement_is_not_guessed(self):
        self.old.unlink();self.new.write_bytes(b'1');self.new.with_suffix('.avi').write_bytes(b'2')
        self.assertIsNone(lr.replacement(self.row(),[self.row()],{'.mkv','.avi'}))
    def test_stability_window_and_skip_unchanged(self):
        calls=[]
        def compute(p):calls.append(p);return 'hash'
        for now in (100,130,161,200):
            with patch.object(lr,'monotonic',return_value=now):
                lr.reconcile(self.db,compute,self.probe,{'.mp4'},lambda _:None)
        self.assertEqual(len(calls),1)
    def test_completion_path_validation(self):
        self.new.write_bytes(b'encoded')
        self.assertEqual(lr.queue_completion(self.db,str(self.old),str(self.new),{'.mkv'}),1)
        with self.assertRaises(ValueError):lr.queue_completion(self.db,'',str(self.new),{'.mkv'})
        with self.assertRaises(ValueError):lr.queue_completion(self.db,str(self.old),'/etc/passwd',{'.mkv'})

if __name__=='__main__':unittest.main()
