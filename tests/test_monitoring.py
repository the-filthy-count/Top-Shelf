import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]

class MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.rows = []
        self.ns = {'db': SimpleNamespace(favourite_list=lambda: self.rows), 'json': json,
                   '_performer_aliases_as_strings': lambda value: value or []}
        tree = ast.parse((ROOT / 'main.py').read_text())
        names = {'_library_performer_name_keys', '_scene_credit_names',
                 '_filter_scenes_to_library_performers', '_library_stashdb_performer_ids',
                 '_library_fansdb_performer_ids', '_library_javstash_performer_ids',
                 '_library_stashdb_studio_ids'}
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                exec(compile(ast.Module(body=[node], type_ignores=[]), 'main.py', 'exec'), self.ns)

    def test_defaults_and_disabled_sources(self):
        self.rows[:] = [dict(kind='performer', folder_name='A', match_stashdb_id='a', match_fansdb_id='a', match_javstash_id='a'),
                        dict(kind='performer', folder_name='B', is_monitored=0, match_stashdb_id='b', match_fansdb_id='b', match_javstash_id='b')]
        for source in ('stashdb', 'fansdb', 'javstash'):
            self.assertEqual(self.ns[f'_library_{source}_performer_ids'](), ['a'])
        cards = [{'performer': 'A'}, {'performer': 'B'}]
        self.assertEqual(self.ns['_filter_scenes_to_library_performers'](cards), cards[:1])

    def test_all_unmonitored_is_empty(self):
        self.rows[:] = [dict(kind='performer', folder_name='A', is_monitored=0)]
        self.assertEqual(self.ns['_filter_scenes_to_library_performers']([{'performer': 'A'}]), [])

    def test_studios(self):
        self.rows[:] = [dict(kind='studio', match_stashdb_id='a'), dict(kind='studio', match_stashdb_id='b', is_monitored=0)]
        self.assertEqual(self.ns['_library_stashdb_studio_ids'](), ['a'])

    def test_alias_eligibility(self):
        self.rows[:] = [dict(kind='performer', folder_name='A', aliases_json='["Alias"]')]
        cards = [{'performer': 'Alias'}]
        self.assertEqual(self.ns['_filter_scenes_to_library_performers'](cards), cards)
        self.rows[0]['is_monitored'] = 0
        self.assertEqual(self.ns['_filter_scenes_to_library_performers'](cards), [])

if __name__ == '__main__':
    unittest.main()
