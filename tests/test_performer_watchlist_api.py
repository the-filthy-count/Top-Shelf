"""Exercise endpoint operations against a real isolated SQLite store."""
import json
import sqlite3
import unittest
from unittest.mock import patch
from fastapi import HTTPException
from app.api import performer_watchlist as api
from app.core import performer_watchlist as watch

class WatchlistAPITests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=sqlite3.Row
        watch.install(self.conn)
        self.conn.execute("CREATE TABLE favourite_entities(kind TEXT,is_group INTEGER DEFAULT 0,match_tpdb_id TEXT,match_stashdb_id TEXT,match_fansdb_id TEXT,match_javstash_id TEXT)")
        self.patch=patch.object(watch.db,'get_conn',return_value=self.conn);self.patch.start()
    def tearDown(self):
        self.patch.stop();self.conn.close()
    def test_add_edit_confirm_and_remove(self):
        identity=api.add({'name':'Unknown Alias','notes':'Found on a site'})['id']
        with self.assertRaises(HTTPException) as error: api.add({'name':'unknown alias'})
        self.assertEqual(error.exception.status_code,409)
        hit={'id':'source-id','source':'FansDB','name':'Known Name','aliases':['Unknown Alias']}
        self.conn.execute('UPDATE performer_watchlist SET candidates=? WHERE id=?',(json.dumps([hit]),identity));self.conn.commit()
        with self.assertRaises(HTTPException): api.confirm(identity,{**hit,'id':'invented'})
        api.confirm(identity,hit)
        self.assertEqual(api.listing()['items'][0]['confirmed'],hit)
        api.edit(identity,{'name':'Corrected Alias'})
        row=api.listing()['items'][0]
        self.assertIsNone(row['confirmed']);self.assertEqual(row['candidates'],[])
        api.remove(identity)
        self.assertEqual(api.listing()['items'],[])
    def test_check_all_excludes_identified(self):
        self.conn.execute("INSERT INTO performer_watchlist(name,next_check) VALUES('Pending',123)")
        self.conn.execute("INSERT INTO performer_watchlist(name,next_check,confirmed) VALUES('Identified',123,'{}')");self.conn.commit()
        api.check_all()
        self.assertEqual(dict(self.conn.execute('SELECT name,next_check FROM performer_watchlist')),{'Pending':0,'Identified':123})
    def test_rejects_unsafe_source_link(self):
        with self.assertRaises(HTTPException) as error: api.add({'name':'Alias','source_url':'javascript:alert(1)'})
        self.assertEqual(error.exception.status_code,400)

    def test_library_badge_uses_source_and_id_not_name(self):
        identity = api.add({'name': 'Bella'})['id']
        hits = [{'source':'FansDB','id':'123','name':'Bella'},
                {'source':'TPDB','id':'123','name':'Bella'}]
        self.conn.execute("INSERT INTO favourite_entities(kind,match_fansdb_id) VALUES('performer','123')")
        self.conn.execute('UPDATE performer_watchlist SET candidates=? WHERE id=?', (json.dumps(hits),identity))
        row = api.listing()['items'][0]
        self.assertEqual([h['in_library'] for h in row['candidates']], [True, False])
        self.assertFalse(row['in_library'])
        api.confirm(identity, hits[0])
        self.assertTrue(api.listing()['items'][0]['in_library'])

    def test_retired_source_results_are_hidden(self):
        identity = api.add({'name': 'Bella'})['id']
        hit = {'source': 'Babepedia', 'id': 'old', 'name': 'Bella'}
        self.conn.execute('UPDATE performer_watchlist SET candidates=?,checks=? WHERE id=?',
                          (json.dumps([hit]), json.dumps({'Babepedia':'Checked'}), identity))
        row = api.listing()['items'][0]
        self.assertEqual(row['candidates'], [])
        self.assertEqual(row['checks'], {})
