import ast
import re
import unittest
from pathlib import Path
from test_duplicate_filing_safety import load, ROOT

class MovieFilenameTests(unittest.TestCase):
    def test_release_names_and_folder_fallback(self):
        parse=load('main.py','guess_movie_query_from_filename',{'Path':Path,'re':re})
        cases={
            'The Movie (2020)/The Movie (2020).mp4':('The Movie','2020'),
            'The.Movie.2020.1080p.WEB-DL.mp4':('The Movie','2020'),
            'The Movie [2020] 2160p BluRay x265-GROUP.mkv':('The Movie','2020'),
            'Inbox/The Movie.mp4':('The Movie',None),
            'The Movie (2020)/video.mp4':('The Movie','2020'),
            'The Movie (2020)/CD1.mkv':('The Movie','2020'),
            '1917.mp4':('1917',None),
            '1917.2019.1080p.mkv':('1917','2019'),
            '2001 A Space Odyssey (1968).mkv':('2001 A Space Odyssey','1968'),
            'The Movie (2020)\\The Movie (2020).mp4':('The Movie','2020'),
        }
        for filename,expected in cases.items():
            with self.subTest(filename=filename):self.assertEqual(parse(filename),expected)
        choose=load('main.py','_verified_movie_candidate',{'re':re})
        title,year=parse('The.Movie.2020.1080p.WEB-DL.mp4')
        self.assertIsNotNone(choose([{'id':'correct','title':'The Movie','year':'2020'}],title,year))

    def test_jav_history_uses_exact_relative_path(self):
        tree=ast.parse((ROOT/'main.py').read_text())
        assignment=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='mv_row' for t in n.targets) and 'movie_rows_map.get(' in ast.unparse(n))
        code=compile(ast.Module(body=[assignment],type_ignores=[]),'<test>','exec')
        rows={'clip.mp4':{'status':'filed'},'jav://clip.mp4':{'status':'unmatched'},'jav://other/clip.mp4':{'status':'error'}}
        for key,expected in [('new/clip.mp4',{}),('clip.mp4',rows['jav://clip.mp4']),('other/clip.mp4',rows['jav://other/clip.mp4'])]:
            env={'movie_rows_map':rows,'jav_rel_key':key,'f':Path('/jav')/key}
            exec(code,env)
            self.assertEqual(env['mv_row'],expected)
