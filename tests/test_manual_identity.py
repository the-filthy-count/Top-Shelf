import ast
import asyncio
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import unittest
from pathlib import Path
from unittest.mock import Mock
import database as db
import test_library_recovery as fixtures
from app.core.manual_identity import bind_payload, resolve_payload


class ManualIdentityTests(unittest.TestCase):
    setUp = fixtures.LibraryRecoveryTests.setUp
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def test_deferred_job_follows_original_not_reused_name(self):
        payload = bind_payload({'filename': 'old.mp4', 'scene': {'id': 'scene'}})
        db.rename_file('old.mp4', 'moved.mp4'); db.upsert_file('old.mp4')
        self.assertEqual(resolve_payload(payload), 'moved.mp4')
        self.assertNotEqual(db.file_identity('old.mp4')['id'], payload['file_id'])

    def test_removed_identity_cannot_fall_back(self):
        payload = bind_payload({'filename': 'old.mp4'})
        db.update_file('old.mp4', status='removed'); db.upsert_file('old.mp4')
        with self.assertRaises(ValueError):
            resolve_payload(payload)
        with self.assertRaises(ValueError):
            bind_payload(payload)

    def test_jav_namespace_is_preserved(self):
        payload = bind_payload({'filename': 'JAV/folder/file.mp4'})
        self.assertEqual(payload['file_id'], db.file_identity('jav://folder/file.mp4', 'processed_movies')['id'])
        self.assertEqual(resolve_payload(payload), 'JAV/folder/file.mp4')

    def test_manual_endpoint_resolves_renamed_file_before_validation(self):
        root = self.base / 'inbox'; root.mkdir()
        old = root / 'old.mp4'; old.write_bytes(b'video')
        payload = bind_payload({'filename': 'old.mp4', 'scene': {'title': 'Scene', 'performers': [{'performer': {'id': 'one'}}]}})
        old.rename(root / 'new.mp4'); db.rename_file('old.mp4', 'new.mp4')
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'file_manual')
        async def inline(func, *args):
            return func(*args)
        enqueue = Mock(return_value=1)
        env = {'app': FastAPI(), 'BackgroundTasks': BackgroundTasks, 'JSONResponse': JSONResponse,
               'Path': Path, 'db': db, 'run_in_threadpool': inline,
               '_resolve_queue_file_path': lambda name, settings: root / name,
               '_scene_display_name': lambda scene: 'Scene', '_manual_action_enqueue_persistent': enqueue}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'manual-api', 'exec'), env)
        response = asyncio.run(env['file_manual'](payload, BackgroundTasks()))
        self.assertTrue(response['queued'])
        self.assertEqual(enqueue.call_args.kwargs['payload']['filename'], 'new.mp4')
        self.assertEqual(enqueue.call_args.kwargs['payload']['file_id'], payload['file_id'])
        payload['file_id'] = 'missing'; enqueue.reset_mock()
        response = asyncio.run(env['file_manual'](payload, BackgroundTasks()))
        self.assertEqual(response.status_code, 409); enqueue.assert_not_called()

    def test_failed_manual_filing_is_not_marked_complete(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_manual_run_manual_scene')
        fn.decorator_list = []
        env = {'Path': Path, 'emit': Mock(), 'file_scene_from_match': Mock(return_value={'status':'no_dir', 'error':'No eligible folder'}),
               '_manual_runner_wrap': lambda payload, label, callback: callback('file.mp4', Path('file.mp4'))}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'manual-runner', 'exec'), env)
        with self.assertRaisesRegex(ValueError, 'No eligible folder'):
            env['_manual_run_manual_scene']({'filename':'file.mp4', 'scene':{}})
