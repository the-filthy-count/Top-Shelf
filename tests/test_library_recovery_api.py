"""Real library removal/restore routes, isolated from application startup."""
import ast
import unittest
from pathlib import Path
from unittest.mock import Mock

from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.concurrency import run_in_threadpool
import database as db
import test_library_recovery as fixtures


class LibraryRecoveryAPITests(unittest.TestCase):
    setUp = fixtures.LibraryRecoveryTests.setUp
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def test_remove_and_restore_through_http(self):
        names = {'api_library_entity_delete_from_disk', '_library_delete_favourite_from_disk',
                 'queue_recovery_list', 'queue_recovery_restore', 'queue_recovery_purge'}
        tree = ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        code = compile(ast.Module(body=[node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                                        and node.name in names], type_ignores=[]), 'library-api', 'exec')
        app = FastAPI(); warm = Mock()
        env = {'app': app, 'Body': Body, 'JSONResponse': JSONResponse, 'db': db,
               'run_in_threadpool': run_in_threadpool, '_queue_warm': Mock(), '_movies_queue_warm': Mock(),
               '_favourites_warm': warm, '_favourites_name_index_bump': Mock()}
        exec(code, env)
        with TestClient(app) as client:
            response = client.post('/api/library/entity/delete-from-disk', json={'id': self.row_id})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()['recoverable']); self.assertFalse(self.video.exists())
            record = client.get('/api/queue/recovery').json()['files'][0]
            response = client.post('/api/queue/recovery/' + record['id'] + '/restore')
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(self.video.read_bytes(), b'video')
            self.assertEqual(warm.invalidate.call_count, 2)
            self.assertEqual(db.favourite_get(self.row_id)['match_tpdb_id'], 'studio-id')
