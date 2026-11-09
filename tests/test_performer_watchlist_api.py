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
        self.patch=patch.object(watch.db,'get_conn',return_value=self.conn);self.patch.start()
    def tearDown(self):
        self.patch.stop();self.conn.close()
    def test_add_edit_confirm_and_remove(self):
        identity=api.add({'name':'Unknown Alias','notes':'Found on a site'})['id']
        with self.assertRaises(HTTPException) as error: api.add({'name':'unknown alias'})
        self.assertEqual(error.exception.status_code,409)
        hit={'id':'source-id','source':'FansDB','name':'Known Name'}
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
