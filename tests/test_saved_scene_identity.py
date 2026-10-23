import unittest
from unittest.mock import Mock
from test_duplicate_filing_safety import load


class SavedSceneIdentityTests(unittest.TestCase):
    def test_saved_id_is_used_for_each_source(self):
        for source in ('TPDB', 'StashDB', 'FansDB', 'JAVStash'):
            with self.subTest(source=source):
                scene={'id':'saved-id','title':'Updated title','performers':[{'performer':{'id':'performer-id'}}]}
                fetch=Mock(return_value=scene)
                search=Mock(side_effect=AssertionError('saved match must not be searched again'))
                env={'_fetch_grab_scene_for_filing':fetch,'_stashbox_normalize_match_source':lambda s:s,
                     '_endpoint_query_var_for_match_source':search}
                fn=load('main.py','_fetch_scene_for_processed_row',env)
                self.assertEqual(fn({'match_source':source,'match_external_id':'saved-id','match_title':'Old title'}),(scene,source))
                fetch.assert_called_once_with({'source_id':'saved-id','source_db':source})
                search.assert_not_called()

    def test_missing_wrong_or_unavailable_saved_scene_does_not_fallback(self):
        for result in (None,{'id':'other-id'},RuntimeError('service unavailable')):
            with self.subTest(result=result):
                fetch=Mock(side_effect=result) if isinstance(result,Exception) else Mock(return_value=result)
                search=Mock(side_effect=AssertionError('unexpected fallback'))
                fn=load('main.py','_fetch_scene_for_processed_row',{'_fetch_grab_scene_for_filing':fetch,
                    '_endpoint_query_var_for_match_source':search})
                self.assertEqual(fn({'match_source':'StashDB','match_external_id':'saved-id'}),(None,''))
                search.assert_not_called()

    def test_legacy_row_without_id_keeps_existing_lookup(self):
        scene={'id':'legacy-id','title':'Scene','release_date':'2026-01-01'}
        env={'_endpoint_query_var_for_match_source':lambda s:('stash','key','full'),
             '_stashbox_normalize_match_source':lambda s:s,'JAVSTASH_ENDPOINT':'jav','STASHDB_QUERY':'query',
             'query_stashbox':lambda *a:[scene]}
        load('main.py','_pick_scene_from_matches',env)
        fn=load('main.py','_fetch_scene_for_processed_row',env)
        self.assertEqual(fn({'match_source':'StashDB','phash':'hash','match_title':'Scene','match_date':'2026-01-01'}),(scene,'StashDB'))
