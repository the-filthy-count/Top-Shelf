import ast
import tempfile
import threading
import time
import subprocess
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import media_budget
import storage_health
from runtime_jobs import KeyedExecutor

ROOT = Path(__file__).resolve().parents[1]

class StabilityTests(unittest.TestCase):
    def setUp(self):
        storage_health._states.clear()

    def test_storage_failure_backs_off_without_reprobing(self):
        with patch('storage_health.monotonic', return_value=100), patch.object(Path, 'stat', side_effect=OSError('offline')) as stat:
            with self.assertRaises(OSError):
                with storage_health.root_operation('/missing'): pass
            with self.assertRaises(storage_health.StorageUnavailable):
                with storage_health.root_operation('/missing'): pass
            self.assertEqual(stat.call_count, 1)
            self.assertEqual(storage_health.snapshot()['/missing']['retry_seconds'], 30)

    def test_inflight_storage_rejects_duplicate(self):
        with tempfile.TemporaryDirectory() as root:
            with storage_health.root_operation(root):
                with self.assertRaises(storage_health.StorageUnavailable):
                    with storage_health.root_operation(root): pass
                self.assertTrue(storage_health.snapshot()[root]['active'])
            with storage_health.root_operation(root): pass

    def test_stuck_media_child_retained_and_replacements_rejected(self):
        child = Mock()
        child.wait.side_effect = subprocess.TimeoutExpired('fake', 2)
        child.poll.return_value = None
        try:
            with patch('subprocess.Popen', return_value=child):
                with self.assertRaises(subprocess.TimeoutExpired):
                    media_budget.run_process(['fake'], timeout=0)
                self.assertIn(child, media_budget._processes)
                self.assertIn(child, media_budget._stuck)
                with self.assertRaisesRegex(RuntimeError, 'unreaped'):
                    media_budget.run_process(['fake'], timeout=1)
                child.wait.assert_called_with(timeout=2)
        finally:
            media_budget._stuck.discard(child)
            media_budget._processes.discard(child)

    def test_keyed_work_deduplicates_and_releases(self):
        release = threading.Event()
        with KeyedExecutor(max_workers=1, pending=0) as pool:
            self.assertTrue(pool.submit_once('a', release.wait, 2))
            self.assertFalse(pool.submit_once('a', lambda: None))
            self.assertFalse(pool.submit_once('b', lambda: None))
            release.set()
        self.assertEqual(pool._keys, set())

    def test_external_scan_busy_is_deferred(self):
        import media_scan_runtime as scans
        scans._last.clear(); scans._active.clear()
        send, defer = Mock(), Mock()
        with patch.object(scans, 'busy', return_value=True):
            scans.dispatch('stash', {}, send, defer)
        send.assert_not_called(); defer.assert_called_once_with(60)

    def test_external_scan_success_enforces_cooldown(self):
        import media_scan_runtime as scans
        scans._last.clear(); scans._active.clear()
        send, defer = Mock(), Mock()
        with patch.object(scans, 'busy', return_value=False):
            scans.dispatch('stash', {}, send, defer)
            scans.dispatch('stash', {}, send, defer)
        send.assert_called_once(); self.assertGreater(defer.call_args.args[0], 1700)

    def tag_feed(self):
        names = {'_discover_tpdb_tag_feed'}
        nodes = [n for n in ast.parse((ROOT/'main.py').read_text()).body if isinstance(n, ast.FunctionDef) and n.name in names]
        db = Mock(); db.get_settings.return_value = {}; db.feed_display_pool_get.return_value = None
        scheduler = Mock(timezone=timezone.utc); scheduler.get_job.return_value = None
        request = Mock()
        request.get.side_effect = lambda *a, **k: SimpleNamespace(raise_for_status=lambda:None,
            iter_content=lambda size: [__import__('json').dumps({'data':[{'id':str(dict(k['params'])['page'])}], 'meta':{'last_page':10}}).encode()], close=lambda:None)
        ns = dict(db=db, scheduler=scheduler, requests=request, datetime=datetime, timedelta=timedelta,
            time=time, json=__import__('json'), _discover_tag_progress={}, _discover_tag_running=threading.Event(),
            _runtime_stop=threading.Event(), _discover_tpdb_tags_lock=threading.Lock(),
            _discover_tpdb_tags=lambda *a: [] if a else [{'id':'1','name':'tag'}],
            _content_filters_fingerprint=lambda:'key', _discover_tag_unique_cards=lambda x:x,
            _filter_feed_cards=lambda x:x, _discover_tag_is_excluded=lambda *a:False,
            _tpdb_scene_rows_from_api_payload=lambda x,**k:x, _tpdb_headers=lambda:{},
            _persist_scenes_feed_display_pool=lambda key,fk,rows:rows,
            _scenes_feed_rewrite_thumbs=lambda x:None, emit=lambda x:None,
            _run_discover_tag_batch=Mock())
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), ns)
        return ns

    def test_tag_feed_serves_without_network_and_queues_refresh(self):
        ns = self.tag_feed()
        result = ns['_discover_tpdb_tag_feed']()
        self.assertTrue(result['refreshing'])
        ns['requests'].get.assert_not_called()
        ns['scheduler'].add_job.assert_called_once()

    def test_tag_pagination_resumes_and_stops_at_end(self):
        ns = self.tag_feed(); feed = ns['_discover_tpdb_tag_feed']
        feed(_background=True)
        self.assertEqual(ns['requests'].get.call_count, 4)
        self.assertEqual(next(iter(ns['_discover_tag_progress'].values()))['page'], 5)
        feed(_background=True); feed(_background=True)
        self.assertEqual(ns['requests'].get.call_count, 10)
        self.assertTrue(next(iter(ns['_discover_tag_progress'].values()))['done'])
        feed(_background=True)
        self.assertEqual(ns['requests'].get.call_count, 10)

    def test_unavailable_index_root_fails_closed(self):
        tree = ast.parse((ROOT/'main.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name=='_health_enumerate_videos_under_roots')
        ns = dict(Path=Path, Callable=__import__('typing').Callable, os=__import__('os'),
                  _LIBRARY_WALK_PROGRESS_EVERY=20, _library_walk_prune_basenames=lambda _:frozenset())
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'), ns)
        with self.assertRaises(OSError):
            ns[node.name]([{'path':'/nonexistent-top-shelf-test-root'}])

    def test_manual_sort_refills_bounded_window_without_losing_jobs(self):
        import queue, json, logging
        names = {'_manual_action_rehydrate', '_manual_action_enqueue'}
        nodes = [n for n in ast.parse((ROOT/'main.py').read_text()).body if isinstance(n, ast.FunctionDef) and n.name in names]
        rows = {i:dict(id=i, job_type='test', label=str(i), display_name='', payload_json=json.dumps({'id':i})) for i in range(130)}
        completed = []
        db = Mock()
        db.manual_job_list_active.side_effect = lambda limit: list(rows.values())[:limit]
        db.manual_job_delete.side_effect = lambda ident: rows.pop(ident)
        ns = dict(queue=queue, json=json, db=db, _log=logging.getLogger(),
            _manual_rehydrate_lock=threading.Lock(), _manual_action_state_lock=threading.Lock(),
            _manual_action_queue=queue.Queue(maxsize=128), _manual_action_pending=[],
            _manual_action_current=('', ''), _manual_action_peak=0, _manual_enqueued_ids=set(),
            _MANUAL_JOB_RUNNERS={'test':lambda payload: completed.append(payload['id'])})
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'main.py','exec'),ns)
        self.assertEqual(ns['_manual_action_rehydrate'](), 64)
        self.assertEqual(ns['_manual_action_rehydrate'](), 0)
        while not ns['_manual_action_queue'].empty():
            label, display, callback = ns['_manual_action_queue'].get_nowait()
            ns['_manual_action_pending'].remove((label, display))
            callback()
            ns['_manual_action_rehydrate']()
            self.assertLessEqual(ns['_manual_action_queue'].qsize(), 64)
        self.assertEqual(completed, list(range(130)))
        self.assertFalse(rows)
        db.manual_job_mark_failed.assert_not_called()

    def test_tag_errors_keep_cursor_for_retry(self):
        ns = self.tag_feed()
        ns['requests'].get.side_effect = TimeoutError('offline')
        ns['_discover_tpdb_tag_feed'](_background=True)
        state = next(iter(ns['_discover_tag_progress'].values()))
        self.assertEqual(state['page'], 1)
        self.assertFalse(state['done'])
        self.assertTrue(ns['scheduler'].add_job.called)

    def test_tag_large_response_is_closed_and_not_consumed(self):
        ns = self.tag_feed()
        response = Mock()
        response.iter_content.return_value = [b'x' * (4*1024*1024+1)]
        ns['requests'].get.side_effect = None
        ns['requests'].get.return_value = response
        ns['_discover_tpdb_tag_feed'](_background=True)
        response.close.assert_called_once()
        self.assertEqual(next(iter(ns['_discover_tag_progress'].values()))['page'], 1)
