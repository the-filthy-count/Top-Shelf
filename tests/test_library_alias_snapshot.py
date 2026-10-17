import ast
import json
import sqlite3
import unittest
from pathlib import Path
from types import SimpleNamespace


class AliasSnapshotTests(unittest.TestCase):
    def test_saved_aliases_override_stale_snapshot_without_mutating_it(self):
        source = Path(__file__).resolve().parents[1] / 'main.py'
        tree = ast.parse(source.read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_favourites_current_aliases')
        conn = sqlite3.connect(':memory:')
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        conn.execute('CREATE TABLE favourite_entities (id INTEGER, kind TEXT, aliases_json TEXT)')
        conn.execute('INSERT INTO favourite_entities VALUES (1, ?, ?)', ('performer', '["badbrittxo"]'))
        scope = {'db': SimpleNamespace(get_conn=lambda: conn), 'json': json}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), scope)
        cached = {'performers': [{'id': 1, 'folder_name': 'Julia Brooks', 'aliases': ['old alias']}], 'studios': []}
        fresh = scope['_favourites_current_aliases'](cached)
        self.assertEqual(fresh['performers'][0]['aliases'], ['badbrittxo'])
        self.assertEqual(cached['performers'][0]['aliases'], ['old alias'])
        conn.execute('UPDATE favourite_entities SET aliases_json = NULL')
        self.assertEqual(scope['_favourites_current_aliases'](cached)['performers'][0]['aliases'], [])


if __name__ == '__main__':
    unittest.main()
