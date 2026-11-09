import json
import sqlite3
import unittest
from unittest.mock import patch, Mock
from app.core import performer_watchlist as w

class WatchlistTests(unittest.TestCase):
    def test_validation(self):
        values=w.validate({'name':'  New Alias ','aliases':['Other','Other'],'source_url':'https://example.com/name'})
        self.assertEqual(values[0],'New Alias')
        self.assertEqual(json.loads(values[1]),['Other'])
        with self.assertRaises(ValueError): w.validate({'name':'A','source_url':'javascript:alert(1)'})
        with self.assertRaises(ValueError): w.validate({'name':' '})
    def test_unconfigured_is_not_a_successful_empty_search(self):
        self.assertEqual(w.search_source('FansDB',['Name'],{}),([], 'Not configured'))
    def test_database_failure_is_not_not_found(self):
        with patch.object(w.requests,'post',side_effect=OSError('offline')):
            hits,status=w.search_source('StashDB',['Name'],{'api_key_stashdb':'test'})
        self.assertEqual(hits,[])
        self.assertIn('failed',status)
    def test_source_ids_preserved_and_alias_queries_deduplicated(self):
        response=Mock();response.json.return_value={'data':{'searchPerformer':[{'id':'abc','name':'Name','aliases':['Alias'],'images':[{'url':'https://example.com/a.jpg'}]}]}}
        with patch.object(w.requests,'post',return_value=response):
            hits,status=w.search_source('FansDB',['Name','Alias'],{'api_key_fansdb':'test'})
        self.assertEqual(len(hits),1);self.assertEqual(hits[0]['id'],'abc');self.assertEqual(hits[0]['source'],'FansDB')
        self.assertEqual(status,'Checked')
    def test_stale_check_cannot_overwrite_edited_or_deleted_entry(self):
        c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row;w.install(c)
        c.execute("INSERT INTO performer_watchlist(id,name) VALUES(1,'Old')");c.commit()
        old=dict(c.execute('select * from performer_watchlist').fetchone())
        c.execute("UPDATE performer_watchlist SET name='New',revision=2 WHERE id=1");c.commit()
        with patch.object(w.db,'get_conn',return_value=c),patch.object(w.db,'get_settings',return_value={}),patch.object(w,'search_source',return_value=([{'id':'wrong'}],'Checked')):
            w.check_entry(old)
            self.assertEqual(c.execute('select candidates from performer_watchlist').fetchone()[0],'[]')
            c.execute('delete from performer_watchlist');c.commit();w.check_entry(old)
            self.assertEqual(c.execute('select count(*) from performer_watchlist').fetchone()[0],0)
        c.close()
