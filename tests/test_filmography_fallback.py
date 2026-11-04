import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from app.core import matching as m
from app.integrations.scene_search import search_joint_cast
from tpdb_joint_scenes import joint_scenes
from test_duplicate_filing_safety import load

class FilmographyFallbackTests(unittest.TestCase):
    def test_single_performer_paginates_and_checks_ids(self):
        calls=[]
        def post(ep,key,q,v):
            if q=='performer': return {'searchPerformer':[{'id':'p1','name':'Demi Hawks'}]}
            calls.append(v['input'])
            rows=[{'id':str(i),'performers':[{'performer':{'id':'wrong'}}]} for i in range(100)] if len(calls)==1 else [{'id':'match','performers':[{'performer':{'id':'p1'}}]}]
            return {'queryScenes':{'scenes':rows}}
        rows,complete=search_joint_cast(['Demi Hawks'],[('StashDB','ep','key')],post,'performer','scenes',None,Mock())
        self.assertTrue(complete);self.assertEqual([r['id'] for r in rows],['match'])
        self.assertEqual([c['page'] for c in calls],[1,2]);self.assertEqual(calls[0]['performers']['value'],['p1'])

    def test_ambiguous_identity_and_incomplete_response_never_become_matches(self):
        post=Mock(return_value={'searchPerformer':[{'id':'a','name':'Demi Hawks'},{'id':'b','name':'Demi Hawks'}]})
        rows,complete=search_joint_cast(['Demi Hawks'],[('StashDB','ep','key')],post,'performer','scenes',None,Mock())
        self.assertEqual(rows,[]);post.assert_called_once()
        post=Mock(side_effect=[{'searchPerformer':[{'id':'a','name':'Demi Hawks'}]}, {'queryScenes':{}}])
        self.assertEqual(search_joint_cast(['Demi Hawks'],[('StashDB','ep','key')],post,'performer','scenes',None,Mock()),([],False))

    def test_tpdb_single_performer_uses_canonical_id(self):
        get=Mock(return_value=Mock(json=lambda:{'data':[{'id':'s','performers':[{'id':'credit','parent':{'id':'p1','name':'Demi Hawks'}}]}]}))
        rows,complete=joint_scenes(['p1'],'key',get)
        self.assertTrue(complete);self.assertEqual(rows[0]['performers'][0]['performer']['id'],'p1')

    def runner(self, text_results, cast_results, rejects=set()):
        db=SimpleNamespace(queue_search_record=lambda _:None,suggestion_performer_ids=lambda _: {},queue_suggestion_mark_attempt=Mock(),queue_suggestion_finish_attempt=Mock(),queue_suggestions_clear=Mock(),queue_suggestions_add=Mock(side_effect=lambda filename, rows, **kwargs: len(rows)),queue_suggestion_rejects_for_file=lambda _:rejects)
        env=dict(db=db,_split_query_for_search=lambda _:('Demi Hawks Summer Fling','Summer Fling','Demi Hawks','',['Demi Hawks']),
                 _suggestion_performer_names=m._suggestion_performer_names,_build_file_meta=lambda _: {'duration_s':3600,'date':'2023-10-13','year':2023},
                 _NAME_SEARCH_NON_IAFD_SOURCES={'TPDB'},search_all_databases=Mock(return_value=text_results),
                 search_scenes_with_all_performers=Mock(return_value=(cast_results,True)),
                 _suggestion_identity_matches=m._suggestion_identity_matches,_required_performers_in_cast=m._required_performers_in_cast,
                 _scene_passes_settings_filters=lambda _:True,_normalize_suggestion=lambda s,f:dict(source_db='TPDB',source_id=s['id'],payload=s),
                 _score_name_match=m._score_name_match,_SUGGESTION_MIN_SCORE=.5,_prefetch_suggestion_breakdowns_async=Mock(),emit=Mock())
        return load('main.py','_run_name_search_for_file',env),env

    def scene(self,id='match',title='Summer Fling',name='Demi Hawks',date='2023-10-13',duration=3600):
        return dict(id=id,title=title,_source='TPDB',performers=[{'name':name}],release_date=date,duration=duration)

    def test_irrelevant_text_results_fall_back_and_rank_date_duration(self):
        run,env=self.runner([self.scene(title='Unrelated')],[self.scene('older',date='2023-09-13'),self.scene('right'),self.scene('short',duration=600),self.scene('wrong',name='Other Person')])
        self.assertEqual(run('file.mp4'),3)
        self.assertEqual(env['db'].queue_suggestions_add.call_args.args[1][0]['source_id'],'right')
        env['search_scenes_with_all_performers'].assert_called_once_with(['Demi Hawks'])

    def test_source_failure_records_incomplete_attempt_without_clearing_saved_results(self):
        run,env=self.runner([],[])
        env['search_all_databases'].side_effect=TimeoutError('temporary source failure')
        env['search_scenes_with_all_performers'].return_value=([],False)
        self.assertEqual(run('file.mp4'),0)
        env['db'].queue_suggestion_finish_attempt.assert_called_once_with('file.mp4',complete=False)
        env['db'].queue_suggestions_clear.assert_not_called()

    def test_good_title_hit_skips_fallback_and_rejection_survives(self):
        run,env=self.runner([self.scene()],[])
        self.assertEqual(run('file.mp4'),1);env['search_scenes_with_all_performers'].assert_not_called()
        run,env=self.runner([],[self.scene()],{('TPDB','match')})
        self.assertEqual(run('file.mp4'),0);self.assertEqual(env['db'].queue_suggestions_add.call_args.args[1], [])

    def test_partial_success_merges_candidates_without_clearing_saved_results(self):
        from scene_search_runtime import Results
        batch = Results([self.scene()], warnings=['StashDB: timed out'])
        run, env = self.runner(batch, [])
        self.assertEqual(run('file.mp4'), 1)
        env['db'].queue_suggestions_clear.assert_not_called()
        env['db'].queue_suggestions_add.assert_called_once()
        env['db'].queue_suggestion_finish_attempt.assert_called_once_with('file.mp4', complete=False)

    def test_text_hits_use_saved_ids_even_when_names_match(self):
        scene = self.scene()
        scene['performers'] = [{'name':'Demi Hawks', 'id':'wrong'}]
        run, env = self.runner([scene], [])
        env['db'].suggestion_performer_ids = lambda _: {'Demi Hawks': {'TPDB': {'correct'}}}
        self.assertEqual(run('file.mp4'), 0)
        scene['performers'] = [{'name':'Different spelling', 'id':'correct'}]
        self.assertEqual(run('file.mp4'), 1)

    def test_overlapping_search_is_skipped_and_guard_released_on_failure(self):
        from app.core import suggestion_guard
        run, env = self.runner([self.scene()], [])
        entered = []
        def search(**kwargs):
            entered.append(run('file.mp4'))
            return [self.scene()]
        env['search_all_databases'].side_effect = search
        self.assertEqual(run('file.mp4'), 1)
        self.assertEqual(entered, [0])
        self.assertEqual(env['db'].queue_suggestion_mark_attempt.call_count, 1)
        env['db'].queue_suggestion_finish_attempt.side_effect = RuntimeError('write failed')
        with self.assertRaises(RuntimeError):
            run('file.mp4')
        self.assertTrue(suggestion_guard.claim('file.mp4'))
        suggestion_guard.release('file.mp4')

    def test_saved_queue_fields_drive_search_instead_of_filename_guesses(self):
        run, env = self.runner([self.scene(title='Corrected title')], [])
        record = dict(file_id='stable-id', revision=2,
                      fields=dict(title='Corrected title', performers=['Demi Hawks'], studio='', date='', scene_number=None, term=''),
                      performer_links=[{'name':'Demi Hawks','source_ids':{}}])
        env['db'].queue_search_record = lambda _: record
        self.assertEqual(run('wrong filename.mp4'), 1)
        self.assertEqual(env['search_all_databases'].call_args.kwargs['title'], 'Corrected title')
        self.assertEqual(env['db'].queue_suggestions_add.call_args.kwargs['expected_record'], record)
