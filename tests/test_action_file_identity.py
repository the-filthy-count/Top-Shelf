import ast
import asyncio
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from fastapi import FastAPI, Body, BackgroundTasks
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
import database as db
import test_library_recovery as fixtures


class ActionIdentityTests(unittest.TestCase):
    setUp = fixtures.LibraryRecoveryTests.setUp
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def endpoints(self):
        names = {'api_queue_suggestions_reject', 'api_queue_suggestions_accept', 'file_movie_endpoint'}
        tree = ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        env = {'app': FastAPI(), 'Body': Body, 'BackgroundTasks': BackgroundTasks, 'Path': Path,
               'JSONResponse': JSONResponse, 'run_in_threadpool': run_in_threadpool, 'db': db,
               'processing_state': {'running': False}, 'emit': Mock(), 'get_tmdb_movie': lambda _: {'id': 'scene'},
               'file_movie': Mock()}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name in names], type_ignores=[]), 'identity-api', 'exec'), env)
        return env

    def test_reject_resolves_original_id_after_rename(self):
        db.upsert_file('old.mp4'); identity = db.file_identity('old.mp4')
        db.rename_file('old.mp4', 'new.mp4'); db.upsert_file('old.mp4')
        env = self.endpoints()
        with patch.object(db, 'queue_suggestions_reject', return_value=1) as reject:
            result = asyncio.run(env['api_queue_suggestions_reject']({'filename': 'old.mp4', 'file_id': identity['id'], 'source_db': 'tpdb', 'source_id': '1'}))
        self.assertTrue(result['ok']); reject.assert_called_once_with('new.mp4', 'tpdb', '1')

    def test_accept_never_falls_back_from_missing_id(self):
        db.upsert_file('old.mp4')
        env = self.endpoints()
        with patch.object(db, 'queue_suggestions_for_file') as lookup:
            result = asyncio.run(env['api_queue_suggestions_accept']({'filename':'old.mp4', 'file_id':'missing', 'source_db':'tpdb', 'source_id':'1'}))
        self.assertFalse(result['ok']); lookup.assert_not_called()

    def test_movie_action_resolves_id_again_when_worker_runs(self):
        inbox = self.base/'inbox'; inbox.mkdir(); (inbox/'old.mp4').write_bytes(b'original')
        db.save_settings({'movies_source_dir': str(inbox)})
        db.upsert_movie('old.mp4'); identity = db.file_identity('old.mp4', 'processed_movies')
        env = self.endpoints(); tasks = BackgroundTasks()
        result = asyncio.run(env['file_movie_endpoint']({'filename':'old.mp4', 'file_id':identity['id'], 'tmdb_id':'1'}, tasks))
        self.assertTrue(result['started'])
        (inbox/'old.mp4').rename(inbox/'new.mp4'); (inbox/'old.mp4').write_bytes(b'other')
        db.rename_movie('old.mp4', 'new.mp4'); db.upsert_movie('old.mp4')
        asyncio.run(tasks())
        self.assertEqual(env['file_movie'].call_args.args[0], inbox/'new.mp4')
        self.assertEqual((inbox/'old.mp4').read_bytes(), b'other')
