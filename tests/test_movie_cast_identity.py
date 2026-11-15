import ast
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock
from urllib.parse import quote_plus


class MovieCastIdentityTests(TestCase):
    def test_tpdb_cast_retains_source_ids_and_distinct_namesakes(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'movies_tpdb_detail')
        fn.decorator_list = []
        response = Mock(status_code=200)
        response.json.return_value = {'data': {'title': 'Example', 'performers': [
            {'performer': {'id': 'gia-id', 'slug': 'gia-jordan', 'name': 'Gia Jordan'}},
            {'id': 'gia-id', 'name': 'Gia Jordan'},
            {'_id': 'different-id', 'name': 'Gia Jordan'},
            'Name Only',
        ]}}
        ns = {'requests': Mock(get=Mock(return_value=response)), '_tpdb_headers': lambda: {},
              '_resolve_library_performers_for_movie': lambda names: [],
              '_tpdb_scene_tag_names': lambda movie: [], 'quote_plus': quote_plus,
              'JSONResponse': lambda data, **kw: data}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), ns)
        result = ns['movies_tpdb_detail']('movie-id')
        self.assertNotIn('error', result)
        self.assertEqual([p['tpdb_id'] for p in result['performer_links']], ['gia-id', 'different-id', ''])
        self.assertEqual(result['performer_links'][0]['url'], 'https://theporndb.net/performers/gia-jordan')
