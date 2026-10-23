import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class CastIdentityTests(unittest.TestCase):
    def test_same_name_does_not_assign_missing_id(self):
        update=Mock();name_lookup=Mock(side_effect=AssertionError('name matching forbidden'))
        env={'db':SimpleNamespace(favourite_find_performer_folders_by_crosswalk=Mock(return_value=[]),favourite_lookup_performer_by_name=name_lookup,favourite_update_matches=update),
             '_submit_enrichment':Mock(),'_log':SimpleNamespace(debug=Mock())}
        load('main.py','_performer_crosswalk_ids',env)
        fn=load('main.py','_auto_link_scene_cast_to_favourites',env)
        fn({'performers':[{'performer':{'id':'other-person','name':'Alex'}}]},'StashDB')
        name_lookup.assert_not_called();update.assert_not_called();env['_submit_enrichment'].assert_not_called()
        env['db'].favourite_find_performer_folders_by_crosswalk.assert_called_once_with('','other-person','','')

    def test_exact_source_id_required_and_groups_are_excluded(self):
        for source,col in [('TPDB','tpdb'),('StashDB','stashdb'),('FansDB','fansdb'),('JAVStash','javstash')]:
            for case in ('verified','conflict','group','already_named'):
                with self.subTest(source=source,case=case):
                    row={f'match_{col}_id':'pid' if case!='conflict' else 'different','is_group':case=='group'}
                    if case=='already_named':row[f'match_{col}_name']='Name'
                    update=Mock();enrich=Mock()
                    env={'db':SimpleNamespace(favourite_find_performer_folders_by_crosswalk=lambda *a:[{'id':1}],favourite_get=lambda i:row,favourite_update_matches=update),
                         '_submit_enrichment':enrich,'_enrich_performer_ext_links_bg':Mock(),'_log':SimpleNamespace(debug=Mock())}
                    load('main.py','_performer_crosswalk_ids',env)
                    fn=load('main.py','_auto_link_scene_cast_to_favourites',env)
                    fn({'performers':[{'performer':{'id':'pid','name':'Name'}}]*2},source)
                    if case=='verified':update.assert_called_once_with(1,**{f'match_{col}_name':'Name'})
                    else:update.assert_not_called()
                    self.assertEqual(enrich.call_count,1 if case=='verified' else 0)
