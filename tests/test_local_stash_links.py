import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

class LocalStashLinksTests(unittest.TestCase):
    def resolve(self, kind, rows, enabled='true'):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'stash_resolve_local_entity_url')
        post = Mock(return_value=SimpleNamespace(status_code=200, json=lambda: {'data': {
            'findPerformers' if kind == 'performer' else 'findStudios': {
                'performers' if kind == 'performer' else 'studios': rows}}}))
        env = {'db': SimpleNamespace(get_settings=lambda: {'stash_url':'http://stash.local:9999','stash_api_key':'test','stash_enabled':enabled}),
               'requests':SimpleNamespace(post=post), '_stash_local_endpoint_variants':lambda source:['https://stashdb.org/graphql']}
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'local-stash', 'exec'), env)
        return env['stash_resolve_local_entity_url'](kind, 'stashdb', 'external-id'), post

    def test_links_use_local_ids_for_both_kinds(self):
        for kind in ('performer','studio'):
            url, post = self.resolve(kind, [{'id':'123'}])
            self.assertEqual(url, f'http://stash.local:9999/{kind}s/123')
            self.assertIn('per_page: 2', post.call_args.kwargs['json']['query'])
            self.assertIn('external-id', str(post.call_args.kwargs['json']['variables']))

    def test_missing_or_ambiguous_matches_have_no_link(self):
        for rows in ([], [{'id':'1'},{'id':'2'}]):
            self.assertIsNone(self.resolve('performer', rows)[0])

    def test_disabled_integration_does_not_contact_stash(self):
        url, post = self.resolve('studio', [{'id':'123'}], 'false')
        self.assertIsNone(url)
        post.assert_not_called()
