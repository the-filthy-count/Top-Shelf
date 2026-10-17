import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from startup_attempts import signature, unchanged, remember

class StartupAttemptTests(unittest.TestCase):
    def test_persistent_signature_and_changed_file(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'video.mkv';path.write_bytes(b'original')
            database=Path(root)/'db.sqlite'
            @contextmanager
            def get_conn():
                conn=sqlite3.connect(database)
                try:
                    with conn: yield conn
                finally: conn.close()
            db=SimpleNamespace(get_conn=get_conn)
            self.assertFalse(unchanged(db,path))
            before=signature(path)
            remember(db,path,before,{'status':'error'})
            self.assertFalse(unchanged(db,path))
            remember(db,path,before,{'status':'unmatched'})
            self.assertTrue(unchanged(db,path))
            path.write_bytes(b'changed file')
            self.assertFalse(unchanged(db,path))
            remember(db,path,before,{'status':'unmatched'})
            self.assertFalse(unchanged(db,path))
            path.unlink()
            remember(db,path,before,{'status':'filed'})
