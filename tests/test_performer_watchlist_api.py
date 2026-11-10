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

    def test_confirmed_entry_skips_direct_check_and_stale_worker(self):
        identity = api.add({'name':'Bella'})['id']
        snapshot = dict(self.conn.execute('SELECT * FROM performer_watchlist WHERE id=?', (identity,)).fetchone())
        self.conn.execute("UPDATE performer_watchlist SET confirmed='{}',next_check=123 WHERE id=?", (identity,))
        self.conn.commit()
        api.check(identity)
        with patch.object(watch.db, 'get_settings') as settings:
            watch.check_entry(snapshot)
            settings.assert_not_called()
        self.assertEqual(self.conn.execute('SELECT next_check FROM performer_watchlist WHERE id=?', (identity,)).fetchone()[0],123)

    def test_details_persist_without_resetting_confirmed_identity(self):
        identity=api.add({'name':'Bella'})['id']
        hit={'source':'FansDB','id':'123','name':'Bella'}
        self.conn.execute('UPDATE performer_watchlist SET candidates=? WHERE id=?',(json.dumps([hit]),identity))
        self.conn.commit()
        api.confirm(identity,hit)
        details={'country':'United States','biography':'Known facts','image_url':'https://example.com/photo.webp','urls':'https://example.com/profile','evidence':'Source checked'}
        api.edit(identity,{'name':'Bella','notes':'Updated notes','details':details})
        row=api.listing()['items'][0]
        self.assertEqual(row['details'],details)
        self.assertEqual(row['confirmed'],hit)
        api.edit(identity,{'name':'Bella','notes':'Another note'})
        self.assertEqual(api.listing()['items'][0]['details'],details)
        watch.install(self.conn)
        self.assertEqual(api.listing()['items'][0]['details'],details)

    def test_invalid_details_do_not_modify_entry(self):
        identity=api.add({'name':'Bella'})['id']
        for details in ({'image_url':'javascript:alert(1)'},{'birthdate':'not a date'},{'unknown':'value'}):
            with self.assertRaises(HTTPException):
                api.edit(identity,{'name':'Different','details':details})
            self.assertEqual(api.listing()['items'][0]['name'],'Bella')

    def test_external_confirmation_and_detail_import_preserves_edits(self):
        identity=api.add({'name':'Bella'})['id']
        hit={'source':'IAFD','id':'iafd-id','name':'Bella','url':'https://www.iafd.com/person.rme/perfid=bella','external_only':True}
        self.conn.execute('UPDATE performer_watchlist SET candidates=?,details=? WHERE id=?',(json.dumps([hit]),json.dumps({'country':'Manual country'}),identity));self.conn.commit()
        api.confirm(identity,hit)
        with patch.object(watch,'_detail_loader',return_value={'country':'Source country','hair_color':'Brown'}):
            self.assertEqual(api.details(identity)['details'],{'country':'Manual country','hair_color':'Brown'})
        self.assertEqual(api.listing()['items'][0]['origin_sources'],['IAFD'])

    def test_submission_blocks_origin_before_library_or_network(self):
        identity=api.add({'name':'Bella'})['id']
        self.conn.execute('UPDATE performer_watchlist SET origin_sources=? WHERE id=?',(json.dumps(['FansDB']),identity));self.conn.commit()
        with patch.object(watch,'_library_creator') as create,patch.object(watch.requests,'post') as post:
            with self.assertRaisesRegex(ValueError,'source database'): watch.submit_draft(identity,'FansDB','/library')
            create.assert_not_called();post.assert_not_called()
        for source in ('TPDB','StashDB','Coomer','IAFD'):
            with self.assertRaises(ValueError): watch.submit_draft(identity,source,'/library')

    def test_submission_receipt_prevents_duplicate_drafts(self):
        identity=api.add({'name':'Bella','source_url':'https://example.com/profile'})['id']
        details={'image_url':'https://example.com/photo.jpg','gender':'Female','evidence':'Verified profile'}
        self.conn.execute('UPDATE performer_watchlist SET details=? WHERE id=?',(json.dumps(details),identity));self.conn.commit()
        with patch.object(watch,'_library_creator',return_value=5),patch.object(watch.db,'get_settings',return_value={'api_key_fansdb':'test'}),patch.object(watch.requests,'post') as post:
            post.return_value.json.return_value={'data':{'submitPerformerDraft':{'id':'draft-1'}}}
            first=watch.submit_draft(identity,'FansDB','/library')
            second=watch.submit_draft(identity,'FansDB','/library')
            self.assertEqual(first,second);self.assertEqual(first['library_row_id'],5)
            self.assertEqual(post.call_count,1)

    def test_incomplete_submission_does_not_create_library_entry(self):
        identity=api.add({'name':'Bella'})['id']
        with patch.object(watch,'_library_creator') as create:
            with self.assertRaises(ValueError): watch.submit_draft(identity,'FansDB','/library')
            create.assert_not_called()

    def test_uncertain_submission_is_not_sent_again(self):
        identity=api.add({'name':'Bella','source_url':'https://example.com/profile'})['id']
        details={'image_url':'https://example.com/photo.jpg','gender':'Female','evidence':'Verified profile'}
        self.conn.execute('UPDATE performer_watchlist SET details=? WHERE id=?',(json.dumps(details),identity));self.conn.commit()
        with patch.object(watch,'_library_creator',return_value=5),patch.object(watch.db,'get_settings',return_value={'api_key_fansdb':'test'}),patch.object(watch.requests,'post',side_effect=TimeoutError) as post:
            with self.assertRaises(TimeoutError): watch.submit_draft(identity,'FansDB','/library')
            with self.assertRaisesRegex(ValueError,'unknown'): watch.submit_draft(identity,'FansDB','/library')
            self.assertEqual(post.call_count,1)

    def test_supplementary_link_keeps_main_database_searches_active(self):
        identity=api.add({'name':'Bella'})['id']
        coomer={'source':'Coomer','id':'creator','name':'Bella','url':'https://site1.coomer.com.co/onlyfans/user/creator','external_only':True}
        main={'source':'FansDB','id':'main-id','name':'Bella'}
        self.conn.execute('UPDATE performer_watchlist SET candidates=?,next_check=123 WHERE id=?',(json.dumps([coomer,main]),identity));self.conn.commit()
        api.confirm(identity,coomer)
        row=api.listing()['items'][0]
        self.assertIsNone(row['confirmed'])
        self.assertEqual(row['linked_profiles']['Coomer'],coomer)
        api.check_all()
        self.assertEqual(api.listing()['items'][0]['next_check'],0)
        api.confirm(identity,main)
        self.assertEqual(api.listing()['items'][0]['confirmed'],main)
        self.assertEqual(set(api.listing()['items'][0]['linked_profiles']),{'Coomer','FansDB'})
        api.confirm(identity,coomer)
        self.assertEqual(api.listing()['items'][0]['confirmed'],main)

    def test_old_external_confirmation_migrates_without_losing_details(self):
        identity=api.add({'name':'Bella'})['id']
        hit={'source':'IAFD','id':'123','name':'Bella','external_only':True}
        self.conn.execute('UPDATE performer_watchlist SET confirmed=?,details=?,next_check=999 WHERE id=?',(json.dumps(hit),json.dumps({'country':'US'}),identity));self.conn.commit()
        watch.install(self.conn)
        row=api.listing()['items'][0]
        self.assertIsNone(row['confirmed'])
        self.assertEqual(row['linked_profiles']['IAFD'],hit)
        self.assertEqual(row['details'],{'country':'US'})
        self.assertEqual(row['next_check'],0)
        watch.install(self.conn)
        self.assertEqual(api.listing()['items'][0]['linked_profiles'],row['linked_profiles'])
