import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from app.core import matching as m
from test_duplicate_filing_safety import load

class SuggestionAliasSourceTests(unittest.TestCase):
    def test_alias_removed_without_changing_canonical_identity(self):
        rows=[{'name':'Example Studio','slug':'example-studio','aliases_json':'["ExampleAlias"]'}]
        scene={'title':'POV','studio':{'name':'Example Studio'}}
        with patch.object(m.db,'_library_guess_from_release_name',return_value={'performers':[]}),patch.object(m.db,'studio_logo_list_all',return_value=rows):
            for filename in ('ExampleAlias POV.mp4','ExampleAlias/POV.mp4','Example Studio POV.mp4'):
                full,title,performer,studio,cast=m._split_query_for_search(filename)
                self.assertEqual(title,'POV')
                self.assertEqual(studio,'Example Studio')
                self.assertTrue(m._suggestion_identity_matches(cast,studio,scene))
                self.assertEqual(m._score_name_match(full,scene,query_title=title,file_meta={}),1)

    def test_manual_includes_iafd_background_excludes_it(self):
        search=Mock(return_value=[])
        sources={'StashDB','TPDB','FansDB','JAVStash'}
        env={'db':SimpleNamespace(queue_suggestion_mark_attempt=Mock(),queue_suggestions_clear=Mock()),
             '_split_query_for_search':lambda f:('Studio POV','POV','','Studio',[]),
             '_suggestion_performer_names':lambda *a:[], '_build_file_meta':lambda f:{},
             '_NAME_SEARCH_NON_IAFD_SOURCES':sources, 'search_all_databases':search}
        run=load('main.py','_run_name_search_for_file',env)
        run('file.mp4')
        self.assertEqual(search.call_args.kwargs['sources'],sources|{'IAFD'})
        run('file.mp4',include_iafd=False)
        self.assertEqual(search.call_args.kwargs['sources'],sources)
        self.assertNotIn('IAFD',sources)
