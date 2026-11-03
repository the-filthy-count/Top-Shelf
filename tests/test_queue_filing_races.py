import ast
import threading
import unittest
from pathlib import Path
from typing import Callable
from unittest.mock import Mock
import database as db
from app.core.manual_identity import bind_payload, filing_activity
from app.core.queue_recovery import archive, _check_context
import test_queue_recovery as fixtures


class QueueFilingRaceTests(unittest.TestCase):
    setUp = fixtures.QueueRecoveryTests.setUp
    video = fixtures.QueueRecoveryTests.video

    def load(self, name, env):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        node.decorator_list = []
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'queue-races', 'exec'), env)
        return env[name]

    def test_manual_filing_blocks_maintenance_and_removal_then_releases(self):
        video = self.video(); payload = bind_payload({'filename': video.name})
        lock = threading.Lock(); state = {}
        env = {'Path': Path, 'Callable': Callable, 'db': db, 'processing_state': state,
               'emit': Mock(), '_pipeline_lock': lock,
               '_manual_runner_prep_video': lambda payload: (video.name, video)}
        run = self.load('_manual_runner_wrap', env)
        def during_filing(name, path):
            self.assertFalse(lock.acquire(blocking=False))
            with self.assertRaisesRegex(ValueError, 'being processed'):
                archive(name, path, self.inbox, 'processed_files', name)
            self.assertTrue(video.exists())
        run(payload, 'test', during_filing)
        self.assertFalse(state['running']); self.assertTrue(lock.acquire(blocking=False)); lock.release()
        self.assertIsNotNone(archive(video.name, video, self.inbox, 'processed_files', video.name)['recovery_id'])

    def test_failed_manual_claim_is_terminal_and_releases_protection(self):
        video = self.video(); payload = bind_payload({'filename': video.name})
        with self.assertRaisesRegex(RuntimeError, 'filing failed'):
            with filing_activity(payload):
                with self.assertRaisesRegex(ValueError, 'already being processed'):
                    with filing_activity(payload):
                        self.fail('Double claim accepted')
                raise RuntimeError('filing failed')
        with db.get_conn() as conn:
            row = conn.execute('SELECT state,error FROM processing_jobs WHERE file_id=?', (payload['file_id'],)).fetchone()
        self.assertEqual(tuple(row), ('failed', 'filing failed'))
        self.assertEqual(db.processing_jobs_ready(), [])

    def test_validation_rechecks_activity_after_removal_preflight(self):
        video = self.video(); payload = bind_payload({'filename':video.name})
        with db.get_conn() as conn:
            record = dict(conn.execute('SELECT * FROM processed_files WHERE filename=?', (video.name,)).fetchone())
        context = {'table':'processed_files', 'filename':video.name, 'root':str(self.inbox),
                   'paths':[str(video)], 'record':record, 'file_id':payload['file_id']}
        with filing_activity(payload), db.file_operation_connection() as conn:
            with self.assertRaisesRegex(ValueError, 'being processed'):
                _check_context(conn, context)

    def test_delete_worker_reresolves_id_and_missing_path_never_changes_history(self):
        video = self.video(); db.upsert_file(video.name); original = db.file_identity(video.name)
        moved = video.with_name('moved.mp4'); video.rename(moved); db.rename_file(video.name, moved.name)
        db.upsert_file(video.name); db.update_file(video.name, status='unmatched')
        env = {'Path':Path, 'db':db, 'emit':Mock(), '_archive_queue_file':archive}
        delete = self.load('_queue_delete_filename_sync', env)
        result = delete(video.name, expected_file_id=original['id'])
        self.assertTrue(result['deleted']); self.assertFalse(moved.exists())
        result = delete(video.name)
        self.assertTrue(result['already_missing'])
        with db.get_conn() as conn:
            status = conn.execute('SELECT status FROM processed_files WHERE filename=?', (video.name,)).fetchone()[0]
        self.assertEqual(status, 'unmatched')
        self.assertFalse(delete(video.name, expected_file_id='missing')['ok'])
