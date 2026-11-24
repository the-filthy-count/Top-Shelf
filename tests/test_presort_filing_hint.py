import ast
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from app.utils.text import _sanitize_fs_component

class PresortHintTests(unittest.TestCase):
    def test_unique_group_folder_and_boundaries(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_presort_library_performer')
        rows=[];ns={'Path':Path,'_sanitize_fs_component':_sanitize_fs_component,'db':SimpleNamespace(favourite_list=lambda:rows)}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        hint=ns['_presort_library_performer']
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);queue=root/'queue';library=root/'library'/'Marie Twins';library.mkdir(parents=True)
            settings={'source_dir':str(queue)}
            rows.append(dict(kind='performer',folder_name='Marie Twins',path=str(library),is_group=1))
            video=queue/'Marie Twins'/'scene.mp4'
            self.assertEqual(hint(video,settings),(library,'Marie Twins'))
            self.assertEqual(hint(video.parent/'Release Folder'/'video.mp4',settings),(library,'Marie Twins'))
            studio=root/'studio';studio.mkdir()
            rows.append(dict(kind='studio',folder_name='My Series',path=str(studio)))
            self.assertEqual(hint(queue/'My Series'/'Release'/'video.mp4',settings,kind='studio'),(studio,'My Series'))
            rows.pop()
            self.assertIsNone(hint(queue/'scene.mp4',settings))
            self.assertIsNone(hint(root/'other'/'Marie Twins'/'scene.mp4',settings))
            rows.append(dict(rows[0]));self.assertIn('multiple',hint(video,settings)['error'])
            rows.pop()
            rows[0]['folder_name']='Marie: Twins'
            self.assertEqual(hint(video,settings),(library,'Marie: Twins'))
            rows.append(dict(kind='studio',folder_name='Marie: Twins',path=str(studio)))
            self.assertEqual(hint(video,settings,kind='studio'),(studio,'Marie: Twins'))
            rows.pop()
            rows.append(dict(rows[0],folder_name='Marie / Twins'))
            self.assertIn('multiple',hint(video,settings)['error'])
            rows.pop();rows[0]['path']=str(root/'missing');self.assertIn('unavailable',hint(video,settings)['error'])
        plan=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_scene_filing_plan')
        guard=next(n for n in ast.walk(plan) if isinstance(n,ast.If) and any(isinstance(x,ast.Call) and isinstance(x.func,ast.Name) and x.func.id=='_presort_library_performer' for statement in n.body for x in ast.walk(statement)))
        self.assertIn('not performers',ast.unparse(guard.test))

    def test_gender_priority_keeps_all_cast(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_performer_objects_in_match_order')
        ns={};exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        cast=[{'performer':{'id':'male','gender':'MALE'}},{'performer':{'id':'female','gender':'FEMALE'}},{'performer':{'id':'unknown'}}]
        self.assertEqual([p['id'] for p in ns['_performer_objects_in_match_order'](cast)],['female','unknown','male'])
