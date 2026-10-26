import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from title_search_queries import title_search_queries
from app.core import matching as m
from test_duplicate_filing_safety import load

class LowercaseTitleQueryTests(unittest.TestCase):
    def test_general_segmentation_keeps_original_and_numbers(self):
        for raw,spaced in [('naughtytherapy','naughty therapy'),('privatelesson2','private lesson 2'),('theperfectweekend','the perfect weekend')]:
            self.assertEqual(title_search_queries(raw),(raw,spaced))
        for raw in ('therapy','private lesson','POV','Naughty Therapy'):
            self.assertEqual(title_search_queries(raw),(raw,))
        self.assertEqual(title_search_queries('x'*600),('x'*600,))

    def test_spaced_search_finds_candidate_without_bypassing_identity(self):
        for correct in (True,False):
            scene={'id':'one','_source':'TPDB','title':'Naughty Therapy','performers':[{'name':'Alecia Fox' if correct else 'Other Person'}]}
            search=Mock(side_effect=lambda **kw:[scene] if kw['title']=='naughty therapy' else [])
            db=SimpleNamespace(queue_suggestion_mark_attempt=Mock(),queue_suggestions_clear=Mock(),queue_suggestions_add=Mock(),queue_suggestion_rejects_for_file=lambda f:set())
            env={'db':db,'_split_query_for_search':lambda f:('Alecia Fox naughtytherapy','naughtytherapy','Alecia Fox','',['Alecia Fox']),
                 '_suggestion_performer_names':m._suggestion_performer_names,'_build_file_meta':lambda f:{},
                 '_NAME_SEARCH_NON_IAFD_SOURCES':{'TPDB'},'search_all_databases':search,
                 '_suggestion_identity_matches':m._suggestion_identity_matches,'_scene_passes_settings_filters':lambda s:True,
                 '_normalize_suggestion':lambda s,f:{'source_db':'TPDB','source_id':'one','payload':s},
                 '_score_name_match':m._score_name_match,'_SUGGESTION_MIN_SCORE':.5,
                 '_required_performers_in_cast':m._required_performers_in_cast,
                 '_prefetch_suggestion_breakdowns_async':Mock(),'emit':lambda s:None}
            run=load('main.py','_run_name_search_for_file',env)
            self.assertEqual(run('file.mp4'),int(correct))
            self.assertEqual([c.kwargs['title'] for c in search.call_args_list],['naughtytherapy','naughty therapy'])
            self.assertEqual(db.queue_suggestions_add.call_count,int(correct))
