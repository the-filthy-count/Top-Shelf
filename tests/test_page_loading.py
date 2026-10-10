"""Regression tests with disposable data; no live NAS access."""
import ast
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]

def function(path, name, namespace):
    node = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == name)
    node.decorator_list = []
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name]

class PageLoadingTests(unittest.TestCase):
    def test_home_skips_disk_for_full_buckets_and_fetches_batches(self):
        rows = [dict(id=i, destination=f'/library/{"star" if i%2 else "studio"}/{i}.mp4', performers='Person' if i%2 else '', processed_at=str(300-i)) for i in range(300)]
        conn = Mock()
        context = Mock()
        context.__enter__ = Mock(return_value=conn)
        context.__exit__ = Mock(return_value=False)
        conn.execute.side_effect = lambda sql, args: Mock(fetchall=lambda: rows[args[0]:args[0]+100])
        db = Mock()
        db.get_conn.return_value = context
        db.favourite_list.return_value = []
        db.get_settings.return_value = {}
        run = function(ROOT/'main.py', '_home_downloaded_scenes_by_kind', dict(db=db, Path=Path))
        with patch.object(Path, 'is_file', return_value=True) as exists:
            result = run(2)
        self.assertEqual(len(result['all']), 2)
        self.assertEqual(len(result['star']), 2)
        self.assertEqual(len(result['studio']), 2)
        exists.assert_not_called()
        self.assertTrue(all('LIMIT 100 OFFSET ?' in call.args[0] for call in conn.execute.call_args_list))

    def test_derived_cache_restores_full_on_success_and_failure(self):
        conn = sqlite3.connect(':memory:')
        try:
            conn.execute('CREATE TABLE cache(value)')
            conn.execute('PRAGMA synchronous=FULL')
            run = function(ROOT/'database.py', '_write_derived_file_cache', dict(get_conn=lambda:conn))
            run('INSERT INTO cache VALUES (?)', ('ok',))
            self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
            with self.assertRaises(sqlite3.OperationalError):
                run('INSERT INTO nonexistent VALUES (?)', ('fail',))
            self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
            conn.execute('BEGIN')
            run('INSERT INTO cache VALUES (?)', ('rollback',))
            self.assertTrue(conn.in_transaction)
            conn.rollback()
            self.assertEqual(conn.execute('SELECT value FROM cache').fetchall(), [('ok',)])
        finally:
            conn.close()

    def test_vice_logo_repairs_locally_without_remote_lookup(self):
        from starlette.responses import FileResponse, JSONResponse
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            logo = Path(root)/'logo.png'
            vice = dict(id='1', name='Fixture')
            def ensure(row, settings, **kwargs):
                self.assertFalse(kwargs['include_tpdb'])
                logo.write_bytes(b'fixture')
                return logo
            run = function(ROOT/'main.py', 'api_vice_logo', dict(
                _vice_logo_path_for_name=lambda name:logo,
                _load_vices=lambda:[vice], _ensure_vice_logo=ensure,
                db=Mock(), FileResponse=FileResponse, JSONResponse=JSONResponse))
            self.assertIsInstance(run('Fixture'), FileResponse)
            self.assertTrue(logo.exists())

    def test_home_uses_filed_history_and_known_removed_flags(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('CREATE TABLE processed_files(id INTEGER, destination TEXT, status TEXT, processed_at TEXT, performers TEXT)')
            conn.execute('CREATE TABLE library_files(destination TEXT, is_removed INTEGER)')
            conn.executemany('INSERT INTO processed_files VALUES (?, ?, ?, ?, ?)', [
                (1, '/library/one.mp4', 'filed', '2026-10-01', ''),
                (2, '/library/two.mp4', 'pending', '2026-10-02', ''),
                (3, '/library/three.mp4', 'filed', '2026-10-03', '')])
            conn.execute('INSERT INTO library_files VALUES (?, 1)', ('/library/three.mp4',))
            conn.commit()
            db = Mock()
            db.get_conn.return_value=conn
            db.favourite_list.return_value=[]
            db.get_settings.return_value={}
            run=function(ROOT/'main.py', '_home_downloaded_scenes_by_kind', dict(db=db,Path=Path))
            with patch.object(Path, 'is_file', side_effect=AssertionError('No disk scans from Home')):
                self.assertEqual([r['history_id'] for r in run()['all']], [1])
        finally:
            conn.close()
