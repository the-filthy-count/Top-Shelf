import sqlite3
import unittest
from unittest.mock import patch
from app.core import filmography_records as r
from app.integrations import performer_filmography as f

class RecordsTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:');self.conn.row_factory=sqlite3.Row
        self.conn.executescript('''CREATE TABLE library_files(id INTEGER PRIMARY KEY,destination TEXT,is_removed INT,phash_1 TEXT,phash_2 TEXT,phash_3 TEXT);
        CREATE TABLE processed_files(match_source TEXT,match_external_id TEXT,destination TEXT,status TEXT);
        CREATE TABLE scene_fingerprints(source TEXT,scene_id TEXT,phash TEXT,algorithm TEXT,duration REAL,fetched_at TEXT,PRIMARY KEY(source,scene_id,phash));''')
        r.create_schema(self.conn)
        self.patch=patch.object(r.db,'get_conn',return_value=self.conn);self.patch.start()
        self.ids={source:['p'] if source=='tpdb' else [] for source in f.SOURCES}
    def tearDown(self):
        self.patch.stop();self.conn.close()
    def scene(self, sid, algorithm='PHASH'):
        return dict(id=sid,title='Title',date='2024-01-01',studio='Studio',url='https://example.com',fingerprints=[{'hash':'0123456789abcdef','algorithm':algorithm}])
    def test_exact_hash_only_and_removed_files(self):
        r.save('tpdb','p',[self.scene('1'),self.scene('2','OSHASH')])
        self.conn.execute("INSERT INTO library_files VALUES(1,'/file',0,'0123456789abcdef',NULL,NULL)");self.conn.commit()
        rows=r.read(self.ids,f.SOURCES)['films']
        self.assertTrue(rows[0]['in_library']);self.assertFalse(rows[1]['in_library'])
        self.conn.execute('UPDATE library_files SET is_removed=1');self.conn.commit()
        self.assertFalse(any(x['in_library'] for x in r.read(self.ids,f.SOURCES)['films']))
    def test_source_id_and_failure_preserve_rows(self):
        r.save('tpdb','p',[self.scene('1')])
        self.conn.execute("INSERT INTO library_files VALUES(1,'/file',0,NULL,NULL,NULL)")
        self.conn.execute("INSERT INTO processed_files VALUES('FansDB','1','/file','success')");self.conn.commit()
        self.assertFalse(r.read(self.ids,f.SOURCES)['films'][0]['in_library'])
        self.conn.execute("UPDATE processed_files SET match_source='TPDB'");self.conn.commit()
        r.failed('tpdb','p')
        self.assertTrue(r.read(self.ids,f.SOURCES)['films'][0]['in_library'])
        self.assertEqual(r.read(self.ids,f.SOURCES)['sources'][0]['state'],'Refresh failed')
    def test_replace_credits_keeps_other_performer(self):
        r.save('tpdb','p',[self.scene('1')]);r.save('tpdb','other',[self.scene('1')]);r.save('tpdb','p',[])
        self.assertEqual(r.read(self.ids,f.SOURCES)['films'],[])
        self.assertEqual(len(r.read({'tpdb':['other']},f.SOURCES)['films']),1)
    def test_legacy_import_idempotent(self):
        old={'scenes':[self.scene('1')]}
        with patch.object(f.db,'app_json_cache_get',return_value=old):
            f.migrate(self.ids);f.migrate(self.ids)
        self.assertEqual(len(r.read(self.ids,f.SOURCES)['films']),1)
        self.assertEqual(r.read(self.ids,f.SOURCES)['updated_at'],0)
