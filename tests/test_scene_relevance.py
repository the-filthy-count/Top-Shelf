import unittest
from scene_relevance import rank_manual_results, title_query

class RelevanceTests(unittest.TestCase):
    def test_reported_irrelevant_results_are_rejected(self):
        titles=['Two White Babes Service Black Guy','The Groomsmen Part 3','Straight To The Point','Straight to the A 3','Straight to the A 2']
        rows=[{'title':title} for title in titles]
        for performer in ('','Romana Kunova'):
            result=rank_manual_results(rows,'Romana Kunova Straight to the A 3',performer)
            self.assertEqual([r['title'] for r in result],['Straight to the A 3'])

    def test_known_performer_removed_without_dash(self):
        self.assertEqual(title_query('Romana.Kunova.Straight to the A 3','Romana Kunova'),'straight to the a 3')

    def test_parent_movie_title_is_valid_evidence(self):
        row={'title':'Scene One','movies':[{'movie':{'title':'Straight to the A 3'}}]}
        self.assertEqual(rank_manual_results([row],'Straight to the A 3'),[row])

    def test_punctuation_and_typo(self):
        row={'title':"Cindy's Great Adventure"}
        self.assertEqual(rank_manual_results([row],'Cindys Great Adventurre'),[row])

    def test_performer_only_preserves_results(self):
        rows=[{'title':'Other film'}]
        self.assertEqual(rank_manual_results(rows,'Romana Kunova','Romana Kunova'),rows)

if __name__=='__main__':unittest.main()
