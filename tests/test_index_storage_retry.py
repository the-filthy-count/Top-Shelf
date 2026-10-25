import ast
import os
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from storage_health import StorageUnavailable


class IndexStorageRetryTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(Path('main.py').read_text())
        names = {'run_library_index_job', '_schedule_library_index_retry'}
        module = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
        self.env = dict(os=os, Path=Path, datetime=datetime, timedelta=timedelta,
                        _runtime_stop=threading.Event(), _library_index_sema=threading.Semaphore(1),
                        scheduler=MagicMock(timezone=timezone.utc), db=MagicMock(), emit=MagicMock())
        for name in ('_health_library_roots', '_health_enumerate_videos_under_roots',
                     '_library_walk_prune_basenames', '_library_index_progress_start_walk',
                     '_library_index_progress_set_persist', '_library_index_progress_set_apply',
                     '_library_index_progress_finish'):
            self.env[name] = MagicMock()
        self.env['_health_library_roots'].return_value = [{'path': '/library/Series'}]
        self.env['db'].wanted_reconcile_with_library.return_value = {}
        exec(compile(module, 'main.py', 'exec'), self.env)

    def test_busy_walk_does_not_infer_removals_and_retries(self):
        self.env['_health_enumerate_videos_under_roots'].side_effect = StorageUnavailable('busy')
        self.env['run_library_index_job']()
        self.env['db'].library_files_mark_removed_bulk.assert_not_called()
        args = self.env['scheduler'].add_job.call_args.kwargs
        self.assertEqual(args['kwargs']['retry_roots'], ['/library/Series'])
        self.assertEqual(args['kwargs']['retry_attempt'], 1)
        self.assertTrue(self.env['_library_index_sema'].acquire(False))

    def test_busy_removal_check_does_not_mark_files_removed(self):
        def walked(roots, **kwargs):
            kwargs['completed_roots']['/library/Series'] = (1, 2, '/library/Series/anchor.mkv')
            return []
        self.env['_health_enumerate_videos_under_roots'].side_effect = walked
        with patch('storage_health.root_operation', side_effect=StorageUnavailable('busy')):
            self.env['run_library_index_job']()
        self.env['db'].library_files_mark_removed_bulk.assert_not_called()
        self.env['scheduler'].add_job.assert_called_once()
        messages = [call.args[0] for call in self.env['emit'].call_args_list]
        self.assertNotIn('LIBRARY INDEX complete', messages)

    def test_retries_are_bounded(self):
        self.env['_schedule_library_index_retry'](['/library/Series'], 3)
        self.env['scheduler'].add_job.assert_not_called()

    def test_retry_does_not_scan_other_roots(self):
        self.env['_health_library_roots'].return_value.append({'path': '/library/Other'})
        self.env['_health_enumerate_videos_under_roots'].return_value = []
        self.env['run_library_index_job'](retry_roots=['/library/Series'], retry_attempt=1)
        calls = self.env['_health_enumerate_videos_under_roots'].call_args_list
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].args[0], [{'path': '/library/Series'}])
