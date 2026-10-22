import ast
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]

class FilingIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tree = ast.parse((ROOT / 'main.py').read_text())
        self.env = {'Path': Path, 'emit': lambda _: None,
                    '_performer_objects_in_match_order': lambda rows: rows}
        for name in ('_performer_crosswalk_ids', '_find_performer_dir_via_library_index'):
            fn = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
            exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), self.env)

    def test_source_namespace(self):
        resolve = self.env['_performer_crosswalk_ids']
        for index, source in enumerate(('TPDB', 'STASHDB', 'FANSDB', 'JAVSTASH')):
            expected = [''] * 4
            expected[index] = '123'
            self.assertEqual(resolve({'id': '123'}, source), tuple(expected))
        for source in ('', 'MANUAL', 'unknown'):
            self.assertEqual(resolve({'id': '123'}, source), ('', '', '', ''))

    def test_wrong_id_is_not_rescued_by_same_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = {'path': tmp, 'folder_name': 'Same Name'}
            self.env['db'] = SimpleNamespace(favourite_find_performer_folder_by_crosswalk=
                lambda *ids: row if ids == ('correct', '', '', '') else None)
            find = self.env['_find_performer_dir_via_library_index']
            self.assertEqual(find([{'id': 'wrong', 'name': 'Same Name'}], 'TPDB'), [])
            self.assertEqual(find([{'name': 'Same Name'}], 'TPDB'), [])
            self.assertEqual(len(find([{'id': 'correct', 'name': 'Different Name'}], 'TPDB')), 1)

    def test_final_plan_has_no_name_or_alias_performer_fallback(self):
        plan = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == '_scene_filing_plan')
        calls = {n.func.id for n in ast.walk(plan) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertIn('_find_performer_dir_via_library_index', calls)
        self.assertNotIn('find_performer_dir', calls)
        self.assertNotIn('_find_performer_dir_with_aliases', calls)

    def test_studio_source_namespace(self):
        from app.core.filing import _studio_crosswalk_ids
        for index, source in enumerate(('TPDB', 'StashDB', 'FansDB')):
            expected = [''] * 3
            expected[index] = '123'
            self.assertEqual(_studio_crosswalk_ids({'id': '123'}, source), tuple(expected))
        for source in ('Manual', '', 'unknown', 'JAVStash'):
            self.assertEqual(_studio_crosswalk_ids({'id': '123'}, source), ('', '', ''))
        self.assertEqual(_studio_crosswalk_ids({'name': 'Same Studio'}, 'TPDB'), ('', '', ''))

    def test_final_plan_has_no_studio_name_fallback(self):
        plan = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == '_scene_filing_plan')
        calls = {n.func.id for n in ast.walk(plan) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertNotIn('find_studio_dir', calls)
        self.assertNotIn('_library_index_queue_matching_enabled', calls)
