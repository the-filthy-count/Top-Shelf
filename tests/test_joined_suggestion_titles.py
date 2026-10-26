import unittest
from unittest.mock import patch
from app.core import matching as m
from scene_relevance import rank_manual_results

class JoinedTitleTests(unittest.TestCase):
    def test_reported_filename_searches_spaced_title(self):
        filename='Alecia Fox/[Alecia Fox] AleciaFox KiraKyox NaughtyTherapy 1920x1080.mp4'
        scene={'title':'Naughty Therapy','performers':[{'name':'Alecia Fox'},{'name':'Kira Kyox'}]}
        with patch.object(m.db,'_library_guess_from_release_name',return_value={'performers':['Alecia Fox','Kira Kyox']}),patch.object(m.db,'studio_logo_list_all',return_value=[]):
            full,title,perf,studio,cast=m._split_query_for_search(filename)
        self.assertEqual(title,'Naughty Therapy')
        self.assertTrue(m._suggestion_identity_matches(cast,studio,scene))
        self.assertEqual(m._score_name_match(full,scene,query_title=title,query_performers=cast,file_meta={}),1)
        self.assertFalse(m._suggestion_identity_matches(cast,studio,dict(scene,performers=[{'name':'Other Person'}])))

    def test_spacing_only_variants_and_acronyms(self):
        for query,title in [('naughtytherapy','Naughty Therapy'),('NaughtyTherapy','Naughty Therapy'),('POVTherapy','POV Therapy'),('PrivateLesson2','Private Lesson 2')]:
            row={'title':title}
            self.assertEqual(rank_manual_results([row],query),[row])

    def test_wrong_title_or_volume_still_rejected(self):
        for query,title in [('NaughtyTherapy','Naughty Teacher'),('PrivateLesson2','Private Lesson 3'),('PrivateLesson2','Private Lesson'),('PrivateLesson12','Private Lesson 1 2')]:
            self.assertEqual(rank_manual_results([{'title':title}],query),[])
