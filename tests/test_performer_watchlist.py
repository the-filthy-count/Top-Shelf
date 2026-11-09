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
        with patch.object(w.db,'get_conn',return_value=c),patch.object(w.db,'get_settings',return_value={}),patch.object(w,'search_source',return_value=([{'id':'wrong'}],'Checked')),patch.object(w,'search_external',return_value=([], 'Checked')):
            w.check_entry(old)
            self.assertEqual(c.execute('select candidates from performer_watchlist').fetchone()[0],'[]')
            c.execute('delete from performer_watchlist');c.commit();w.check_entry(old)
            self.assertEqual(c.execute('select count(*) from performer_watchlist').fetchone()[0],0)
        c.close()

    def test_full_name_matching_keeps_order_and_accepts_joined_aliases(self):
        names = ['Valeria Pixie', 'lovelybabylove1']
        for value in ['Valeria Pixie', 'valeria_pixie', 'VALERIAPIXIE']:
            self.assertTrue(w.matches_names({'name':value}, names))
        for value in ['Valeria', 'Pixie', 'Pixie Valeria', 'Valeria Pixie Extra']:
            self.assertFalse(w.matches_names({'name':value}, names))
        self.assertTrue(w.matches_names({'name':'Different Name','aliases':['lovelybabylove1']}, names))
        self.assertEqual(w.search_terms(['Valeria_Pixie']), ['Valeria Pixie','ValeriaPixie'])
    def test_broad_database_results_are_filtered(self):
        response=Mock();response.json.return_value={'data':{'searchPerformer':[
            {'id':'wrong','name':'Valeria'}, {'id':'reverse','name':'Pixie Valeria'},
            {'id':'right','name':'ValeriaPixie'}]}}
        with patch.object(w.requests,'post',return_value=response):
            hits,_=w.search_source('FansDB',['Valeria Pixie'],{'api_key_fansdb':'test'})
        self.assertEqual([h['id'] for h in hits],['right'])

class ExternalLeadTests(unittest.TestCase):
    def test_coomer_exact_alias_and_cached_catalogue(self):
        response = Mock()
        response.json.return_value = [
            {'id':'janedoe','name':'Jane Doe','service':'onlyfans'},
            {'id':'jane','name':'Jane','service':'onlyfans'},
            {'id':'doejane','name':'Doe Jane','service':'fansly'}]
        with patch.object(w, '_creators_cache', (0, [])), patch.object(w.requests, 'get', return_value=response) as get:
            hits, status = w.search_external('Coomer', ['Jane Doe'])
            self.assertEqual(status, 'Checked')
            self.assertEqual(len(hits), 1)
            self.assertEqual(hits[0]['url'], 'https://coomer.st/onlyfans/user/janedoe')
            self.assertTrue(hits[0]['external_only'])
            w.search_external('Coomer', ['Someone Else'])
            self.assertEqual(get.call_count, 1)

    def test_failed_catalogue_is_not_not_found(self):
        with patch.object(w, '_creators_cache', (0, [])), patch.object(w.requests, 'get', side_effect=OSError):
            self.assertIn('failed', w.search_external('Coomer', ['Jane'])[1])
