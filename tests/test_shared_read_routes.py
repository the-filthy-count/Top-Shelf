import ast
import unittest
from pathlib import Path
from types import SimpleNamespace

TREE = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())

class SharedReadTests(unittest.TestCase):
    def test_blocking_artwork_reads_use_fastapi_worker_pool(self):
        for name in ('api_performers_headshots_by_name', 'api_performers_genders_by_name',
                     'library_favourite_lookup', 'api_favourites_folder_logo', 'api_studio_logo_asset', 'queue_file_mtime'):
            with self.subTest(route=name):
                route = next(n for n in TREE.body if getattr(n, 'name', '') == name)
                self.assertIsInstance(route, ast.FunctionDef)
                self.assertTrue(route.decorator_list)

    def test_cloudflare_body_is_not_exposed_as_fingerprint_error(self):
        from unittest.mock import patch
        import sys
        response = SimpleNamespace(status_code=403, text='<!DOCTYPE html><title>Cloudflare</title>')
        response.json = lambda: (_ for _ in ()).throw(ValueError())
        cooldown = SimpleNamespace(send=lambda endpoint, send: response)
        integrations = SimpleNamespace(fingerprint_cooldown=cooldown)
        node = next(n for n in TREE.body if getattr(n, 'name', '') == 'query_stashbox')
        ns = {}
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'main.py', 'exec'), ns)
        with patch.dict(sys.modules, {'app.integrations': integrations}):
            with self.assertRaisesRegex(RuntimeError, '^HTTP 403: blocked by Cloudflare; retry later$'):
                ns['query_stashbox']('hash', 'endpoint', 'key', 'query', 'full')

if __name__ == '__main__': unittest.main()
