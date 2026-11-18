import ast
from pathlib import Path
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock


class RecentSceneFailuresTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_scenes_recent_cached_or_compute')
        self.response = Mock(status_code=200)
        self.response.json.return_value = {'data': []}
        self.ns = {
            'time': time, 'threading': threading,
            'requests': Mock(get=Mock(return_value=self.response)),
            'db': SimpleNamespace(get_settings=lambda: {'api_key_tpdb':'test'}), '_tpdb_headers': lambda: {},
            '_SCENES_RECENT_CACHE': {}, '_SCENES_RECENT_CACHE_LOCK': threading.Lock(),
            '_SCENES_RECENT_CACHE_TTL': 1800, '_SCENES_RECENT_CACHE_MAX': 400,
            '_discover_rewrite_scene_thumbs_in_payload': lambda payload: None, 'emit': Mock(),
        }
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), self.ns)

    def fetch(self):
        return self.ns['get_scenes_recent_cached_or_compute']('TPDB', 'performer-id')

    def test_http_failure_is_not_cached_and_retry_can_succeed(self):
        self.response.status_code = 503
        self.assertIn('tpdb', self.fetch()['errors'])
        self.assertFalse(self.ns['_SCENES_RECENT_CACHE'])
        self.response.status_code = 200
        self.assertNotIn('errors', self.fetch())
        self.assertTrue(self.ns['_SCENES_RECENT_CACHE'])
        self.assertEqual(self.ns['requests'].get.call_count, 2)

    def test_invalid_payload_is_not_treated_as_no_scenes(self):
        self.response.json.return_value = {'message': 'Unavailable'}
        self.assertIn('tpdb', self.fetch()['errors'])
        self.assertFalse(self.ns['_SCENES_RECENT_CACHE'])

    def test_network_exception_is_not_cached(self):
        self.ns['requests'].get.side_effect = TimeoutError()
        self.assertIn('tpdb', self.fetch()['errors'])
        self.assertFalse(self.ns['_SCENES_RECENT_CACHE'])

    def test_successful_empty_list_is_cached(self):
        self.assertEqual(self.fetch()['scenes'], [])
        self.fetch()
        self.ns['requests'].get.assert_called_once()

    def test_missing_credentials_are_errors_for_every_source(self):
        self.ns['db'] = SimpleNamespace(get_settings=lambda: {})
        for source in ('TPDB', 'StashDB', 'FansDB', 'JAVStash'):
            result = self.ns['get_scenes_recent_cached_or_compute'](source, 'id')
            self.assertIn(source.lower(), result['errors'])
            self.assertFalse(self.ns['_SCENES_RECENT_CACHE'])
        self.ns['requests'].get.assert_not_called()
