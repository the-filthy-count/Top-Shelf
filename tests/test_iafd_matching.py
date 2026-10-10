import unittest
from iafd_matching import choose_film, choose_scene

def film(title, year='2005', studio='', url=None):
    return dict(title=title,year=year,studio=studio,url=url or title+year)

class IafdMatchingTests(unittest.TestCase):
    def test_release_noise_and_performer_prefix(self):
        expected=film("Cindy's Great Adventure 2")
        self.assertEqual(choose_film([expected],{},'Cindy Star/Cindy.Star.Cindys.Great.Adventure.2.2005.Scene.3.1080p.x264.mp4','Cindy Star'),expected)

    def test_filename_recovers_an_overtrimmed_parser_title(self):
        expected=film('A Very Special Night')
        self.assertEqual(choose_film([expected],{'title':'Night'},'A.Very.Special.Night.Scene.2.mp4','Fixture'),expected)

    def test_sequel_number_must_match(self):
        self.assertIsNone(choose_film([film('Great Adventure 2')],{'title':'Great Adventure 3'},'Great.Adventure.3.mp4','Fixture'))

    def test_remakes_require_disambiguation(self):
        films=[film('Great Adventure','2005'),film('Great Adventure','2015')]
        self.assertIsNone(choose_film(films,{},'Great.Adventure.mp4','Fixture'))
        self.assertEqual(choose_film(films,{},'Great.Adventure.2015.mp4','Fixture'),films[1])
        films[1]['studio']='Example Studio'
        self.assertEqual(choose_film(films,{'site':'Example Studio'},'Great.Adventure.mp4','Fixture'),films[1])

    def test_short_titles_need_exact_match(self):
        self.assertIsNone(choose_film([film('Night')],{},'A.Very.Special.Night.mp4','Fixture'))
        self.assertEqual(choose_film([film('Night')],{},'Night.1080p.mp4','Fixture')['title'],'Night')

    def test_minor_typo_in_long_title(self):
        expected=film('A Very Special Adventure')
        self.assertEqual(choose_film([expected],{},'A.Very.Special.Adventurre.mp4','Fixture'),expected)

    def test_scene_must_be_unique_or_explicit(self):
        scenes=[{'number':1,'cast':['Fixture']},{'number':2,'cast':['Fixture','Other']}]
        self.assertEqual(choose_scene(scenes,'Fixture'),-1)
        self.assertEqual(choose_scene(scenes,'Fixture',2),1)
        self.assertEqual(choose_scene(scenes,'Fixture',3),-1)
        self.assertEqual(choose_scene(scenes,'Other'),1)
        self.assertEqual(choose_scene(scenes,'Fix'),-1)

    def test_bulk_scanner_wiring_and_ambiguous_scene(self):
        import ast
        from pathlib import Path
        from unittest.mock import Mock
        path=Path(__file__).resolve().parents[1]/'main.py'
        tree=ast.parse(path.read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_iafd_bulk_scan_for_performer')
        db=Mock()
        db.get_settings.return_value={}
        db.iafd_get_filmography.return_value=[film('Great Adventure')]
        namespace=dict(db=db,emit=lambda _:None,
            _iafd_performer_url=lambda _: 'performer-url',
            parse_filename=lambda *_:{'title':'Great Adventure'},
            _filename_scene_number=lambda _:None,
            _iafd_scene_breakdowns=lambda _:[{'number':1,'cast':['Fixture']},{'number':2,'cast':['Fixture']}],
            _iafd_scene_detail_performers=lambda _:[])
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),namespace)
        result=namespace['_iafd_bulk_scan_for_performer']('Fixture',['Great Adventure.mp4'])
        self.assertEqual(result['unmatched'],[])
        self.assertEqual(result['groups'][0]['files'][0]['scene_idx'],-1)

    def test_review_candidates_leave_ambiguous_match_unselected(self):
        from iafd_matching import film_suggestions
        films=[film('Great Adventure','2005'),film('Great Adventure','2015')]
        suggestions=film_suggestions(films,{},'Great Adventure.mp4','Fixture')
        self.assertEqual(len(suggestions),2)
        self.assertIn('Exact cleaned title',suggestions[0]['reasons'])
        self.assertIsNone(choose_film(films,{},'Great Adventure.mp4','Fixture'))

    def test_saved_alias_avoids_live_lookup(self):
        import ast,json
        from pathlib import Path
        from unittest.mock import Mock
        path=Path(__file__).resolve().parents[1]/'main.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='_iafd_performer_url')
        db=Mock()
        db.favourite_lookup_performer_by_name.return_value={'id':1,'aliases_json':'["Known Alias"]'}
        db.iafd_get_performer_by_name.side_effect=lambda name: {'url':'saved-url'} if name=='Known Alias' else None
        live=Mock(side_effect=AssertionError('Unexpected live lookup'))
        ns=dict(db=db,json=json,_performers_iafd_read_meta=lambda _: {},emit=lambda _:None,_iafd_performer_query=live)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),ns)
        self.assertEqual(ns['_iafd_performer_url']('Folder Name'),'saved-url')
        live.assert_not_called()

if __name__=='__main__':unittest.main()
