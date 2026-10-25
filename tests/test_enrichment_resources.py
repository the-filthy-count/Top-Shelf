import ast
import gc
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import media_scan_runtime as scans
from runtime_jobs import KeyedExecutor, enrichment_admitted

class ResourceTests(unittest.TestCase):
    def test_thread_exit_closes_connection_without_gc(self):
        tree = ast.parse(Path('database.py').read_text())
        names = {'_ThreadConnectionOwner', 'TimedConnection', 'get_conn', 'close_thread_connection'}
        nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in names]
        ns = {'sqlite3': sqlite3, '_conn_thread_local': threading.local()}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'database.py', 'exec'), ns)
        with tempfile.TemporaryDirectory() as root:
            ns['DB_PATH'] = Path(root) / 'test.db'
            baseline = len(list(Path('/proc/self/fd').iterdir()))
            connections = []
            def work():
                conn = ns['get_conn']()
                connections.append(conn)  # Even retained references must be closed.
                conn.execute('SELECT 1')
            enabled = gc.isenabled()
            gc.disable()
            try:
                for _ in range(100):
                    thread = threading.Thread(target=work)
                    thread.start()
                    thread.join()
                for conn in connections:
                    # Cross-thread checks would otherwise mask whether it closed.
                    self.assertRaises(sqlite3.ProgrammingError, conn.execute, 'SELECT 1')
                self.assertLessEqual(len(list(Path('/proc/self/fd').iterdir())), baseline + 2)
            finally:
                if enabled:
                    gc.enable()

    def test_duplicate_and_overflow_jobs_are_not_started(self):
        release = threading.Event()
        with KeyedExecutor(max_workers=1, pending=1) as pool:
            try:
                self.assertTrue(pool.submit_once('a', release.wait, 2))
                self.assertFalse(pool.submit_once('a', release.wait, 2))
                self.assertTrue(pool.submit_once('b', lambda: None))
                self.assertFalse(pool.submit_once('c', lambda: None))
            finally:
                release.set()

    def test_stash_graphql_error_defers_without_scanning(self):
        response = Mock()
        response.json.return_value = {'errors': [{'message': 'queue unavailable'}], 'data': {'jobQueue': None}}
        send, defer = Mock(), Mock()
        with patch.object(scans.requests, 'post', return_value=response):
            scans.dispatch('stash', {'stash_url': 'http://test'}, send, defer)
        send.assert_not_called()
        defer.assert_called_once_with(300)
        response.close.assert_called_once()

    def test_empty_stash_queue_is_idle(self):
        response = Mock()
        response.json.return_value = {'data': {'jobQueue': []}}
        with patch.object(scans.requests, 'post', return_value=response):
            self.assertFalse(scans.busy('stash', {'stash_url': 'http://test'}))
        response.close.assert_called_once()

    def test_null_stash_queue_is_idle(self):
        response = Mock()
        response.json.return_value = {'data': {'jobQueue': None}}
        with patch.object(scans.requests, 'post', return_value=response):
            self.assertFalse(scans.busy('stash', {'stash_url': 'http://test'}))
        response.close.assert_called_once()

    def test_missing_or_malformed_stash_queue_is_not_idle(self):
        for payload in ({'data': {}}, {'data': {'jobQueue': {}}}, {'data': {'jobQueue': [{}]}}, None):
            response = Mock()
            response.json.return_value = payload
            with self.subTest(payload=payload), patch.object(scans.requests, 'post', return_value=response):
                with self.assertRaises(ValueError):
                    scans.busy('stash', {'stash_url': 'http://test'})
            response.close.assert_called_once()

    def test_fd_pressure_defers_enrichment(self):
        with patch('runtime_jobs.descriptor_usage', return_value={'open': 800, 'soft_limit': 1024}):
            self.assertFalse(enrichment_admitted())
