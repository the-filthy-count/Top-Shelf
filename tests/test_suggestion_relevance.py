import ast
import re
import unittest
from pathlib import Path
from unittest.mock import Mock
from scene_search_runtime import Results

ROOT=Path(__file__).resolve().parents[1]

def scorer():
    tree=ast.parse((ROOT/'app/core/matching.py').read_text())
    names={'_name_match_tokenize','_candidate_performer_names','_performer_name_tokens_match','_required_performers_in_cast','_suggestion_performer_names','_score_name_match'}
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in {'_NAME_SEARCH_NOISE_TOKENS','_SCENE_NUMBER_RE'} for t in n.targets)]
    ns={'re':re}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'matching.py','exec'),ns)
    return ns['_score_name_match']

class SuggestionTests(unittest.TestCase):
    def test_title_and_volume_beat_performer_overlap(self):
        score=scorer()
        for title in ('Straight To The Point','Straight to the A 2','The Groomsmen Part 3'):
            row={'title':title,'performers':[{'performer':{'name':'Romana Kunova'}}]}
            self.assertEqual(score('Romana Kunova Straight to the A 3',row,query_performer='Romana Kunova'),0)
        row={'title':'Straight to the A 3','performers':[{'performer':{'name':'Romana Kunova'}}]}
        self.assertEqual(score('Romana Kunova Straight to the A 3',row,query_performer='Romana Kunova'),1)

    def test_explicit_scene_marker_and_parent_title(self):
        score=scorer()
        row={'title':'Scene Three','movies':[{'movie':{'title':'Great Adventure 2'}}]}
        self.assertGreater(score('Great Adventure 2 Scene 3',row,require_performers_in_cast=False),.5)

    def test_empty_success_clears_but_failure_preserves(self):
        node=next(n for n in ast.parse((ROOT/'main.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='_run_name_search_for_file')
        for batch in (Results(),Results(warnings=['TPDB: timed out'])):
            db=Mock()
            ns=dict(db=db,_split_query_for_search=lambda _:('Fixture 3','Fixture 3','','',[]),
                    _build_file_meta=lambda _:{},search_all_databases=lambda **_:batch,emit=lambda _:None)
            exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
            self.assertEqual(ns['_run_name_search_for_file']('Fixture 3.mp4'),0)
            self.assertEqual(db.queue_suggestions_clear.call_count,0 if batch.warnings else 1)

if __name__=='__main__':unittest.main()
