"""Exercise the actual router without starting Top-Shelf's NAS jobs."""
import unittest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
import requests
import magpie_integration as magpie

class MagpieTests(unittest.TestCase):
    def setUp(self):
        magpie.invalidate()
        self.settings={'magpie_url':'http://nas:9080','magpie_token':'magpie_secret'}
        self.get=patch.object(magpie.db,'get_settings',side_effect=lambda:self.settings).start()
        self.filing=patch.object(magpie.db,'enrich_download_items_with_filing').start()
        self.source=patch.object(magpie.db,'enrich_download_items_with_source_scene').start()
        self.save=patch.object(magpie.db,'save_settings',side_effect=self.settings.update).start()
        self.session=MagicMock();self.response=MagicMock(status_code=200)
        self.response.json.return_value=[]
        self.session.request.return_value=self.response
        factory=patch.object(magpie.requests,'Session').start()
        factory.return_value.__enter__.return_value=self.session
        self.addCleanup(patch.stopall)
        app=FastAPI();app.include_router(magpie.router);self.client=TestClient(app)
        self.addCleanup(self.client.close)

    def test_history_removal_keeps_files_and_rejects_active_jobs(self):
        ident='a'*32
        with patch.object(magpie,'request',side_effect=[{'status':'completed'},{'ok':True}]) as upstream:
            self.assertEqual(self.client.delete('/api/magpie/downloads/'+ident).status_code,200)
            self.assertEqual(upstream.call_args.args,('DELETE','downloads/'+ident))
            self.assertEqual(upstream.call_args.kwargs,{})
        with patch.object(magpie,'request',return_value={'status':'downloading'}) as upstream:
            self.assertEqual(self.client.delete('/api/magpie/downloads/'+ident).status_code,409)
            self.assertEqual(upstream.call_count,1)
        self.assertEqual(self.client.delete('/api/magpie/downloads/invalid').status_code,400)

    def test_artwork_enrichment_includes_magpie_rows(self):
        job={'id':'a'*32,'status':'completed','current_filename':'Alice Studio.mp4','url':'https://example.com/gallery'}
        with patch.object(magpie,'job_snapshot',return_value=([job],None)):
            row=magpie.merge_downloads({})['items'][0]
        self.filing.assert_called_once_with([row])
        self.source.assert_called_once_with([row])
        self.assertEqual(row['source_url'],job['url'])

    def test_connection_uses_unsaved_credentials_without_saving(self):
        before = dict(self.settings)
        response = self.client.post('/api/magpie/test', json={'url':'http://new-host:9080', 'token':'magpie_new'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.settings, before)
        self.save.assert_not_called()
        self.assertEqual(self.session.request.call_args.args[1], 'http://new-host:9080/api/destinations')
        self.assertEqual(self.session.request.call_args.kwargs['headers']['Authorization'], 'Bearer magpie_new')

    def test_connection_cannot_forward_saved_token_to_changed_host(self):
        response = self.client.post('/api/magpie/test', json={'url':'http://other-host'})
        self.assertEqual(response.status_code, 400)
        self.session.request.assert_not_called()
        self.save.assert_not_called()

    def test_validation_errors_never_echo_tokens(self):
        token='magpie_'+'secret'*100
        response=self.client.post('/api/magpie/config',json={'url':'http://nas:9080','token':token})
        self.assertEqual(response.status_code,422)
        self.assertNotIn('secret',response.text)

    def test_config_never_returns_token(self):
        response=self.client.get('/api/magpie/config')
        self.assertEqual(response.json()['url'],'http://nas:9080')
        self.assertTrue(response.json()['token_set'])
        self.assertEqual(response.json()['categories'],{})
        self.assertNotIn('secret',response.text)

    def test_blank_token_preserved_only_for_same_host(self):
        self.assertEqual(self.client.post('/api/magpie/config',json={'url':'http://nas:9080/'}).status_code,200)
        self.assertEqual(self.settings['magpie_token'],'magpie_secret')
        self.assertEqual(self.client.post('/api/magpie/config',json={'url':'https://other.test'}).status_code,400)
        self.assertEqual(self.settings['magpie_url'],'http://nas:9080')

    def test_url_and_token_validation(self):
        for url in ['file:///tmp/file','http://user:pass@nas','http://nas:bad','https://nas?token=x']:
            self.assertEqual(self.client.post('/api/magpie/config',json={'url':url,'token':'magpie_new'}).status_code,400)
        self.assertEqual(self.client.post('/api/magpie/config',json={'url':'http://nas','token':'password'}).status_code,400)

    def test_list_paginates_and_uses_bearer_with_no_redirects(self):
        self.assertEqual(self.client.get('/api/magpie/downloads?status=failed&offset=50').status_code,200)
        args,kw=self.session.request.call_args
        self.assertEqual(args,('GET','http://nas:9080/api/downloads'))
        self.assertEqual(kw['headers']['Authorization'],'Bearer magpie_secret')
        self.assertEqual(kw['params'],{'status':'failed','limit':50,'offset':50})
        self.assertFalse(kw['allow_redirects']);self.assertFalse(self.session.trust_env)
        self.assertEqual(kw['timeout'],(3,12))

    def test_submit_and_actions(self):
        data={'url':'https://example.com/gallery/123','destination_id':'photos'}
        self.assertEqual(self.client.post('/api/magpie/downloads',json=data).status_code,200)
        self.assertEqual(self.session.request.call_args.kwargs['json'],data)
        for action in ['cancel','retry']:
            self.assertEqual(self.client.post('/api/magpie/downloads/'+'a'*32+'/'+action).status_code,200)
        self.session.request.reset_mock()
        for path in ['invalid/retry','a'*32+'/delete']:
            self.assertEqual(self.client.post('/api/magpie/downloads/'+path).status_code,400)
        self.session.request.assert_not_called()

    def test_invalid_submit_does_not_call_magpie(self):
        self.assertEqual(self.client.post('/api/magpie/downloads',json={'url':'file:///tmp/test','destination_id':'x'}).status_code,400)
        self.session.request.assert_not_called()

    def test_errors_do_not_echo_upstream_credentials(self):
        self.response.status_code=401
        self.response.json.return_value={'error':{'message':'magpie_secret'}}
        result=self.client.get('/api/magpie/destinations')
        self.assertEqual(result.status_code,502);self.assertNotIn('magpie_secret',result.text)
        self.response.status_code=302
        self.assertEqual(self.client.get('/api/magpie/destinations').status_code,502)

    def test_network_failure_releases_concurrency_slot(self):
        self.session.request.side_effect=requests.Timeout('secret in URL')
        for _ in range(5):
            result=self.client.get('/api/magpie/downloads')
            self.assertEqual(result.status_code,502);self.assertNotIn('secret',result.text)

    def test_unknown_status_or_negative_offset_never_calls_upstream(self):
        self.assertEqual(self.client.get('/api/magpie/downloads?status=bogus').status_code,400)
        self.assertEqual(self.client.get('/api/magpie/downloads?offset=-1').status_code,422)
        self.session.request.assert_not_called()

    def test_missing_configuration_and_bad_json(self):
        self.settings.clear()
        self.assertEqual(self.client.get('/api/magpie/downloads').status_code,409)
        self.settings.update(magpie_url='http://nas:9080',magpie_token='magpie_test')
        self.response.json.side_effect=ValueError('invalid')
        self.assertEqual(self.client.get('/api/magpie/downloads').status_code,502)

    def test_category_mapping_routes_submission_and_rejects_unmapped(self):
        self.settings['magpie_categories']='{"jav":"jav-folder","stars":"star-folder"}'
        result=self.client.post('/api/magpie/downloads',json={'url':'https://example.com/video','category':'jav','destination_id':'wrong'})
        self.assertEqual(result.status_code,200)
        self.assertEqual(self.session.request.call_args.kwargs['json']['destination_id'],'jav-folder')
        self.session.request.reset_mock()
        self.assertEqual(self.client.post('/api/magpie/downloads',json={'url':'https://example.com/video','category':'movies'}).status_code,400)
        self.session.request.assert_not_called()

    def test_submission_forwards_named_tags_always_at_start(self):
        self.settings['magpie_categories']='{"stars":"photos"}'
        for position in ['start','end']:
            response=self.client.post('/api/magpie/downloads',json={'url':'https://example.com/gallery','category':'stars','custom_fields':{'creator':'Alice','collection':'Nature'},'filename_position':position})
            self.assertEqual(response.status_code,200)
            body=self.session.request.call_args.kwargs['json']
            self.assertEqual(list(body['custom_fields'].items()),[('creator','Alice'),('collection','Nature')])
            self.assertEqual(body['filename_position'],'start')
        self.assertEqual(self.client.post('/api/magpie/downloads',json={'url':'https://example.com/gallery','category':'stars','filename_position':'middle'}).status_code,422)

    def test_saved_mappings_require_known_writable_destinations(self):
        self.response.json.return_value=[{'id':'photos','writable':True},{'id':'offline','writable':False}]
        self.assertEqual(self.client.post('/api/magpie/categories',json={'categories':{'stars':'photos','movies':''}}).status_code,200)
        self.assertEqual(magpie.mappings(),{'stars':'photos','movies':''})
        self.assertEqual(self.client.post('/api/magpie/categories',json={'categories':{'jav':'offline'}}).status_code,400)

    def test_snapshot_fetches_active_separately_deduplicates_and_caches(self):
        active={'id':'a'*32,'status':'downloading'}
        old={'id':'b'*32,'status':'completed'}
        self.response.json.side_effect=[[active],[],[active,old],{**active,'file_bytes':25,'file_total':100,'files':2,'skipped':1,'total_files':10}]
        jobs,error=magpie.job_snapshot()
        self.assertIsNone(error);self.assertEqual(len(jobs),2)
        self.assertEqual(len(magpie.job_snapshot()[0]),2)
        self.assertEqual(self.session.request.call_count,4)
        self.assertEqual(jobs[0]['file_bytes'],25)
        self.assertEqual(jobs[0]['total_files'],10)

    def test_combined_view_keeps_original_rows_and_adds_magpie_speed(self):
        self.settings['magpie_categories']='{"jav":"jav-folder"}'
        jobs=[{'id':'a'*32,'status':'downloading','destination_id':'jav-folder','speed':1024,'url':'https://example.com','files':2},
              {'id':'b'*32,'status':'failed','destination_id':'jav-folder','speed':9999}]
        original={'items':[{'source':'nzb','name':'existing'}],'bandwidth_down_bps':512,'categories':[]}
        with patch.object(magpie,'job_snapshot',return_value=(jobs,None)):
            combined=magpie.merge_downloads(original)
            self.assertEqual(len(combined['items']),3)
            self.assertEqual(len(original['items']),1)
            self.assertEqual(combined['bandwidth_down_bps'],1536)
            self.assertIsNone(combined['items'][1]['progress_pct'])
            self.assertTrue(combined['items'][2]['failed'])
            self.assertEqual(combined['items'][1]['magpie_action'],'cancel')
            self.assertEqual(combined['items'][2]['magpie_action'],'retry')
            self.assertEqual(len(magpie.merge_downloads(original,'@magpie:jav')['items']),2)
            self.assertEqual(len(magpie.merge_downloads(original,'@magpie:stars')['items']),0)

    def test_progress_uses_overall_then_current_file_and_handles_unknown_totals(self):
        cases = [
            ({'files':2,'skipped':1,'total_files':10,'file_bytes':50,'file_total':100}, (30,'Overall')),
            ({'file_bytes':25,'file_total':100}, (25,'Current file')),
            ({'file_bytes':125,'file_total':100}, (100,'Current file')),
            ({'files':2}, (None,'Total unknown')),
            ({'file_bytes':0,'file_total':0}, (None,'Finding media')),
            ({'file_bytes':'bad','file_total':100}, (None,'Finding media')),
            ({'status':'completed'}, (100,'Overall')),
        ]
        for job, expected in cases:
            with self.subTest(job=job):
                self.assertEqual(magpie.job_progress(job), expected)
        with patch.object(magpie,'job_snapshot',return_value=([{'id':'a'*32,'status':'downloading','file_bytes':50,'file_total':100}],None)):
            row=magpie.merge_downloads({})['items'][0]
            self.assertEqual(row['progress_pct'],50)
            self.assertEqual(row['progress_label'],'Current file')
            self.assertEqual(row['file_progress_pct'],50)
            self.assertIsNone(row['album_progress_pct'])
            self.assertFalse(row['completed'])

    def test_explicit_percentages_override_legacy_counts(self):
        job={'id':'a'*32,'status':'downloading','file_progress_percent':50.0,'album_progress_percent':40.0,'file_bytes':90,'file_total':100,'files':9,'total_files':10}
        with patch.object(magpie,'job_snapshot',return_value=([job],None)):
            row=magpie.merge_downloads({})['items'][0]
        self.assertEqual(row['file_progress_pct'],50)
        self.assertEqual(row['album_progress_pct'],40)
        self.assertEqual(row['progress_pct'],40)
        for value, expected in [(None,None),(0,0),(150,100),('invalid',None),(float('nan'),None)]:
            with self.subTest(value=value):
                self.assertEqual(magpie.explicit_progress({'p':value},'p',90),expected)
        self.assertEqual(magpie.explicit_progress({},'p',90),90)

    def test_completed_job_unknown_totals_stay_unknown_in_both_rings(self):
        with patch.object(magpie,'job_snapshot',return_value=([{'id':'a'*32,'status':'completed','file_total':None,'total_files':None}],None)):
            row=magpie.merge_downloads({})['items'][0]
        self.assertTrue(row['completed'])
        self.assertIsNone(row['file_progress_pct'])
        self.assertIsNone(row['album_progress_pct'])

    def test_snapshot_failure_retains_last_jobs_and_reports_error(self):
        import time
        magpie._cache=(('http://nas:9080','magpie_secret'),time.monotonic()-30,[{'id':'a'*32}],None)
        self.session.request.side_effect=requests.Timeout()
        jobs,error=magpie.job_snapshot()
        self.assertEqual(len(jobs),1);self.assertIn('last known',error)

if __name__=='__main__':unittest.main()
