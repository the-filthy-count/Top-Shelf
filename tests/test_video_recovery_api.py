import ast
import unittest
from pathlib import Path
from unittest.mock import Mock

from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.concurrency import run_in_threadpool
import database as db
from download_safety import VIDEO_EXTENSIONS
import test_library_recovery as fixtures


class VideoRecoveryAPITests(unittest.TestCase):
    setUp = fixtures.LibraryRecoveryTests.setUp
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def test_health_removal_and_restore(self):
        names = {'health_delete_video', 'queue_recovery_list', 'queue_recovery_restore'}
        tree = ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        code = compile(ast.Module(body=[node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                                       and node.name in names], type_ignores=[]), 'video-api', 'exec')
        app = FastAPI()
        env = {'app': app, 'Body': Body, 'JSONResponse': JSONResponse, 'db': db, 'Path': Path,
               'VIDEO_EXTENSIONS': VIDEO_EXTENSIONS, 'run_in_threadpool': run_in_threadpool,
               '_health_library_roots': lambda: [{'path': str(self.root)}],
               '_health_assert_path_under_roots': lambda raw, _: Path(raw), '_health_scan_cache_strip': Mock(),
               '_queue_warm': Mock(), '_movies_queue_warm': Mock(), '_favourites_warm': Mock(),
               '_favourites_name_index_bump': Mock()}
        exec(code, env)
        with TestClient(app) as client:
            result = client.post('/api/health/delete-video', json={'path': str(self.video)})
            self.assertEqual(result.status_code, 200, result.text)
            self.assertTrue(result.json()['recoverable']); self.assertEqual(result.json()['freed_bytes'], 0)
            self.assertFalse(self.video.exists())
            operation = result.json()['recovery_id']
            restored = client.post('/api/queue/recovery/' + operation + '/restore')
            self.assertEqual(restored.status_code, 200, restored.text)
            self.assertEqual(self.video.read_bytes(), b'video')
