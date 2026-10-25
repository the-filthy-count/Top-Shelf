import unittest
from unittest.mock import patch
from app.core import matching as m

class SuggestionNameTests(unittest.TestCase):
    def test_compact_and_folder_names_keep_correct_title(self):
        candidate={'title':'POV','performers':[{'name':'Leana Lovings'}]}
        with patch.object(m.db,'_library_guess_from_release_name',return_value={'performers':['Leana Lovings']}),patch.object(m.db,'studio_logo_list_all',return_value=[]):
            for name in ('Leana Lovings POV.mp4','LeanaLovings POV.mp4','Leana Lovings/POV.mp4','LeanaLovings/POV.mp4'):
                with self.subTest(name=name):
                    full,title,perf,studio,cast=m._split_query_for_search(name)
                    self.assertEqual(title,'POV')
                    self.assertEqual(m._score_name_match(full,candidate,query_title=title,query_performers=cast,file_meta={}),1)

    def test_title_words_and_folder_boundaries_preserved(self):
        with patch.object(m.db,'_library_guess_from_release_name',return_value={'performers':['Jane Doe'],'studio':'Example Studio'}):
            self.assertEqual(m._split_query_for_search('ExampleStudio/Jane Doe Jane Comes Home 2.mp4')[1],'Jane Comes Home 2')
            self.assertEqual(m._split_query_for_search('Jane/Doe Private Lesson.mp4')[1],'Jane Doe Private Lesson')
            self.assertEqual(m._split_query_for_search('Jane Doe.mp4')[1],'')

    def test_cast_requires_complete_ordered_name(self):
        for name in ('Leana Lovings','LeanaLovings','LEANA-LOVINGS'):
            self.assertTrue(m._required_performers_in_cast(['Leana Lovings'],{'performers':[{'name':name}]}))
        for name in ('Lovings Leana','LovingsLeana','Leana','Leana Lovings Other','OtherLeanaLovings'):
            self.assertFalse(m._required_performers_in_cast(['Leana Lovings'],{'performers':[{'name':name}]}))
