import unittest
from unittest.mock import Mock
from app.core import matching as m
from test_duplicate_filing_safety import load

class JointCastTests(unittest.TestCase):
    def test_cast_only_is_opt_in_and_requires_every_performer(self):
        names = ['Alecia Fox', 'Kira Kyox']
        scene = {'title': 'Different Title', 'performers': [{'performer': {'name': n}} for n in names]}
        args = dict(query_title='unknown', query_performers=names)
        self.assertEqual(m._score_name_match('unknown', scene, **args), 0)
        reasons = []
        self.assertEqual(m._score_name_match('unknown', scene, **args, allow_joint_cast=True, reasons=reasons), .5)
        self.assertIn('title unconfirmed', reasons[0]['label'])
        scene['performers'].pop()
        self.assertEqual(m._score_name_match('unknown', scene, **args, allow_joint_cast=True), 0)
        self.assertEqual(m._score_name_match('unknown', scene, query_performers=names[:1], allow_joint_cast=True), 0)

    def test_source_ids_all_filter_and_returned_cast_validation(self):
        calls = []
        def post(endpoint, key, query, variables):
            calls.append((query, variables))
            if query == 'performers':
                name = variables['term']
                return {'searchPerformer': [{'id': name, 'name': name}, {'id': 'wrong', 'name': 'Wrong Person'}]}
            return {'queryScenes': {'scenes': [
                {'id': 'both', 'performers': [{'performer': {'id': n}} for n in ['Alecia Fox', 'Kira Kyox']]},
                {'id': 'one', 'performers': [{'performer': {'id': 'Alecia Fox'}}]},
            ]}}
        env = dict(get_api_keys=lambda:dict(stashdb='key',tpdb='',fansdb='',javstash=''),
                   STASHDB_ENDPOINT='stash',TPDB_ENDPOINT='tpdb',FANSDB_ENDPOINT='fans',JAVSTASH_ENDPOINT='jav',
                   SEARCH_PERFORMER_QUERY='performers',QUERY_SCENES_QUERY='scenes',_stashbox_post=post,emit=Mock())
        search = load('main.py', 'search_scenes_with_all_performers', env)
        rows, complete = search(['Alecia Fox', 'Kira Kyox'])
        self.assertTrue(complete)
        self.assertEqual([r['id'] for r in rows], ['both'])
        self.assertEqual(calls[-1][1]['input']['performers'], {'value':['Alecia Fox','Kira Kyox'],'modifier':'INCLUDES_ALL'})
        env['_stashbox_post'] = Mock(return_value={'searchPerformer':[{'id':'1','name':'Alecia Fox'},{'id':'2','name':'Alecia Fox'}]})
        self.assertEqual(search(['Alecia Fox', 'Kira Kyox']), ([], True))
        env['_stashbox_post'].assert_called_once()

    def test_runner_saves_cast_only_result_but_not_partial_cast(self):
        from types import SimpleNamespace
        names = ['Alecia Fox', 'Kira Kyox']
        scene = {'id':'both','_source':'TPDB','title':'Unknown Title',
                 'performers':[{'performer':{'name':n}} for n in names]}
        partial = dict(scene, id='partial', performers=scene['performers'][:1])
        db = SimpleNamespace(queue_suggestion_mark_attempt=Mock(), queue_suggestions_clear=Mock(),
                             queue_suggestions_add=Mock(), queue_suggestion_rejects_for_file=lambda f:set())
        env = dict(db=db, _split_query_for_search=lambda f:(' '.join(names),'','Alecia Fox','',names),
                   _suggestion_performer_names=m._suggestion_performer_names, _build_file_meta=lambda f:{},
                   _NAME_SEARCH_NON_IAFD_SOURCES={'TPDB'}, search_all_databases=Mock(return_value=[]),
                   search_scenes_with_all_performers=Mock(return_value=([scene,partial],True)),
                   _suggestion_identity_matches=m._suggestion_identity_matches, _scene_passes_settings_filters=lambda s:True,
                   _normalize_suggestion=lambda s,f:dict(source_db='TPDB',source_id=s['id'],payload=s),
                   _score_name_match=m._score_name_match, _SUGGESTION_MIN_SCORE=.5,
                   _required_performers_in_cast=m._required_performers_in_cast,
                   _prefetch_suggestion_breakdowns_async=Mock(),emit=Mock())
        run = load('main.py','_run_name_search_for_file',env)
        self.assertEqual(run('file.mp4'),1)
        self.assertEqual(db.queue_suggestions_add.call_args.args[1][0]['source_id'],'both')
        env['search_scenes_with_all_performers'].assert_called_once_with(names)

    def test_tpdb_uses_rest_not_stashbox_scene_filter(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        post=Mock(side_effect=lambda endpoint,key,query,v:{'searchPerformer':[{'id':v['term'],'name':v['term']}]})
        env=dict(get_api_keys=lambda:dict(stashdb='',tpdb='key',fansdb='',javstash=''),
                 STASHDB_ENDPOINT='stash',TPDB_ENDPOINT='tpdb',FANSDB_ENDPOINT='fans',JAVSTASH_ENDPOINT='jav',
                 SEARCH_PERFORMER_QUERY='performers',QUERY_SCENES_QUERY='scenes',_stashbox_post=post,
                 requests=SimpleNamespace(get=Mock()),emit=Mock())
        search=load('main.py','search_scenes_with_all_performers',env)
        with patch('tpdb_joint_scenes.joint_scenes',return_value=([{'id':'both','_source':'TPDB'}],False)) as rest:
            rows,complete=search(['Alecia Fox','Kira Kyox'])
            self.assertFalse(complete);self.assertEqual(rows[0]['id'],'both')
            rest.assert_called_once_with(['Alecia Fox','Kira Kyox'],'key',env['requests'].get)
        self.assertEqual(post.call_count,2)
