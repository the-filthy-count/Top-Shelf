"""Workflow tests use real temporary files and an isolated application database."""
import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database as db
from app.core import jobs
from app.utils import fs


class FileLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        db.close_thread_connection()
        old_db = db.DB_PATH
        db.DB_PATH = self.root / 'test.sqlite'
        self.addCleanup(setattr, db, 'DB_PATH', old_db)
        self.addCleanup(db.close_thread_connection)
        db.init_db()
        old_connect = fs._operation_connection
        fs.configure_operation_history(db.file_operation_connection)
        self.addCleanup(fs.configure_operation_history, old_connect)

    def test_move_restart_retry_and_filing_preserve_identity(self):
        src = self.root / 'arrival.mp4'
        src.write_bytes(b'video-content')
        dst = self.root / 'Performer' / 'arrival.mp4'
        db.upsert_file(src.name)
        asset = db.file_identity(src.name)
        db.processing_job_enqueue(src.name)
        self.assertEqual(db.processing_job_start(src.name), asset['id'])
        fs.safe_move(src, dst)
        self.assertTrue(db.rename_file(src.name, 'Performer/arrival.mp4'))
        with db.get_conn() as conn:
            jobs.recover(conn)
            conn.execute('UPDATE queue_changes SET available_at=0')
            conn.commit()
        self.assertEqual(db.processing_jobs_ready(), ['Performer/arrival.mp4'])
        self.assertEqual(db.processing_job_start('Performer/arrival.mp4'), asset['id'])
        db.update_file('Performer/arrival.mp4', status='filed', destination=str(dst))
        db.processing_job_finish(asset['id'], 'filed')
        db.init_db()
        result = db.file_identity('Performer/arrival.mp4')
        self.assertEqual(result['id'], asset['id'])
        self.assertEqual(result['original_path'], 'arrival.mp4')
        self.assertEqual(result['current_path'], str(dst))
        self.assertEqual(dst.read_bytes(), b'video-content')
        self.assertFalse(src.exists())
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT state FROM file_operations').fetchone()[0], 'complete')
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute('DELETE FROM file_events')

    def test_removing_queue_record_cancels_pending_work_but_retains_history(self):
        db.upsert_file('removed.mp4')
        asset = db.file_identity('removed.mp4')
        db.processing_job_enqueue('removed.mp4')
        with db.get_conn() as conn:
            conn.execute("DELETE FROM processed_files WHERE filename='removed.mp4'")
            conn.commit()
            self.assertEqual(conn.execute('SELECT state FROM processing_jobs').fetchone()[0], 'cancelled')
            self.assertIsNotNone(conn.execute('SELECT id FROM file_assets WHERE id=?', (asset['id'],)).fetchone())
        self.assertEqual(db.processing_jobs_ready(), [])

    def test_failed_journal_prevents_move(self):
        src = self.root / 'source.mp4'
        src.write_bytes(b'original')
        dst = self.root / 'destination.mp4'
        with patch('app.core.file_history.record_operation', side_effect=OSError('journal unavailable')):
            with self.assertRaises(OSError):
                fs.safe_move(src, dst)
        self.assertEqual(src.read_bytes(), b'original')
        self.assertFalse(dst.exists())

    def test_confirmation_failure_does_not_report_completed_move_as_failed(self):
        src = self.root / 'source.mp4'
        src.write_bytes(b'original')
        dst = self.root / 'destination.mp4'
        with patch('app.core.file_history.finish_operation', side_effect=OSError('journal unavailable')):
            with self.assertLogs('app.utils.fs', level='ERROR'):
                fs.safe_move(src, dst)
        self.assertFalse(src.exists())
        self.assertEqual(dst.read_bytes(), b'original')
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT state FROM file_operations').fetchone()[0], 'prepared')


class QueueRenderingTests(unittest.TestCase):
    def test_refresh_cannot_dispatch_imports_or_change_media_paths(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        prohibited = {'_group_queue_root_files_by_hint', '_auto_enqueue_vice_jav_from_snapshot',
                      'processed_files_purge_source_orphans', 'reconcile_movies_source',
                      'safe_move', 'unlink', 'rename', 'rmtree'}
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in ('_build_queue_snapshot', '_build_movies_queue_snapshot'):
                calls = {getattr(call.func, 'id', getattr(call.func, 'attr', ''))
                         for call in ast.walk(node) if isinstance(call, ast.Call)}
                self.assertFalse(calls & prohibited, node.name)
