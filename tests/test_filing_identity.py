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
            self.env['db'] = SimpleNamespace(favourite_find_performer_folders_by_crosswalk=
                lambda *ids: [row] if ids == ('correct', '', '', '') else [])
            find = self.env['_find_performer_dir_via_library_index']
            self.assertEqual(find([{'id': 'wrong', 'name': 'Same Name'}], 'TPDB'), [])
            self.assertEqual(find([{'name': 'Same Name'}], 'TPDB'), [])
            self.assertEqual(len(find([{'id': 'correct', 'name': 'Different Name'}], 'TPDB')), 1)

    def test_final_plan_has_no_name_or_alias_performer_fallback(self):
        plan = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == '_scene_filing_plan')
        calls = {n.func.id for n in ast.walk(plan) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertIn('_find_performer_dir_via_library_index', calls)
        manual = next(n for n in ast.walk(plan) if isinstance(n, ast.If)
                      and ast.unparse(n.test) == "source == 'Manual'")
        self.assertIn('find_performer_dir', ast.unparse(manual))
        self.assertNotIn('_find_performer_dir_with_aliases', calls)

    def test_missing_folder_does_not_hide_valid_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ('one', 'two'): (root / name).mkdir()
            self.env['db'] = SimpleNamespace(favourite_find_performer_folders_by_crosswalk=lambda *ids: [
                {'path': str(root / name), 'folder_name': name, 'weight': weight}
                for name, weight in [('missing', 10), ('one', 1), ('two', 8)]])
            hits = self.env['_find_performer_dir_via_library_index']([{'id':'id', 'name':'Person'}], 'TPDB')
            self.assertEqual([p.name for p, _, _ in hits], ['one', 'two'])
            self.assertEqual(max(hits, key=lambda hit: hit[2]['weight'])[0].name, 'two')

    def test_exact_grab_scene_retains_ids_and_rejects_wrong_scene(self):
        from unittest.mock import Mock
        fn = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == '_fetch_grab_scene_for_filing')
        scene = {'id':'scene1', 'performers':[{'performer':{'id':'performer1'}}]}
        env = {'_stashbox_normalize_match_source':lambda s:s,
               '_endpoint_query_var_for_match_source':lambda s:('endpoint','key','unused'),
               '_stashbox_post':Mock(return_value={'findScene':scene})}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), env)
        fetch = env[fn.name]
        self.assertEqual(fetch({'source_id':'scene1','source_db':'StashDB'}),scene)
        self.assertIsNone(fetch({'source_id':'wrong','source_db':'StashDB'}))
        self.assertIsNone(fetch({'source_id':'scene1','source_db':'Manual'}))

    def test_database_returns_direct_and_group_matches(self):
        import sqlite3, json
        conn = sqlite3.connect(':memory:'); self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        conn.execute('CREATE TABLE favourite_entities (id INTEGER, kind TEXT, path TEXT, folder_name TEXT, is_favourite INTEGER, weight INTEGER, is_group INTEGER, group_ids_json TEXT, match_tpdb_id TEXT, match_stashdb_id TEXT, match_fansdb_id TEXT, match_javstash_id TEXT)')
        for i in (1,2):
            conn.execute("INSERT INTO favourite_entities VALUES (?, 'performer', ?, 'Name', 0, 1, 0, '', 'pid', '', '', '')", (i,str(i)))
        conn.execute("INSERT INTO favourite_entities VALUES (3, 'performer', 'group', 'Group', 0, 8, 1, ?, '', '', '', '')", (json.dumps({'tpdb':['pid']}),))
        tree = ast.parse((ROOT/'database.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'favourite_find_performer_folders_by_crosswalk')
        env = {'get_conn':lambda:conn,'json':json}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'database.py', 'exec'), env)
        self.assertEqual([r['id'] for r in env[fn.name]('pid','','')], [1,2,3])

    def test_manual_exact_directory_match(self):
        fn = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == 'find_performer_dir')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'Selected Performer').mkdir(); (root/'Other Performer').mkdir()
            env={'Path':Path, 'normalise':lambda s:s.lower(),
                 '_gender_priority_names':lambda ps:[p['performer']['name'] for p in ps],
                 'iter_performer_folders':lambda root,bucket:list(root.iterdir()),
                 'db':SimpleNamespace(favourite_get_by_path=lambda p:None)}
            exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),env)
            hits=env[fn.name]([{'performer':{'name':'Selected Performer'}}],[{'path':tmp,'rank':1}])
            self.assertEqual([p.name for p,_,_ in hits],['Selected Performer'])

    def test_tpdb_grab_preserves_performer_ids(self):
        from unittest.mock import Mock
        from urllib.parse import quote
        fn=next(n for n in self.tree.body if isinstance(n,ast.FunctionDef) and n.name=='_fetch_grab_scene_for_filing')
        response=Mock(); response.json.return_value={'data':{'id':'scene', 'title':'Title',
            'performers':[{'id':'person','name':'Person'}], 'site':{'id':'studio','name':'Studio'}}}
        env={'_stashbox_normalize_match_source':lambda s:s,'quote':quote,
             'db':SimpleNamespace(get_settings=lambda:{'api_key_tpdb':'test'}),
             '_tpdb_headers':lambda:{}, 'requests':SimpleNamespace(get=Mock(return_value=response))}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),env)
        scene=env[fn.name]({'source_id':'scene','source_db':'TPDB'})
        self.assertEqual(scene['performers'][0]['performer']['id'],'person')
        self.assertEqual(scene['studio']['id'],'studio')
