"""External link parsing must not invent Coomer profiles."""
import ast
import re
import unittest
from urllib.parse import urlparse, quote, unquote
from types import SimpleNamespace
from pathlib import Path

module = ast.parse((Path(__file__).resolve().parents[1] / "main.py").read_text())
nodes = [n for n in module.body if isinstance(n, ast.FunctionDef) and n.name in ("_parse_performer_ext_links", "_coomer_profile_url", "_coomer_fetch_headshot")]
namespace = {"re": re, "urlparse": urlparse, "quote": quote, "unquote": unquote}
exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), namespace)
parse = namespace["_parse_performer_ext_links"]

class CoomerLinksTests(unittest.TestCase):
    def test_retired_source_not_imported(self):
        self.assertEqual(parse(['https://www.babepedia.com/babe/Example']), {})

    def test_social_profiles_do_not_create_archive_links(self):
        for service in ("fansly", "onlyfans"):
            self.assertNotIn("coomer", parse([f"https://{service}.com/example"]))

    def test_explicit_link_wins_regardless_of_order(self):
        link = "https://coomer.st/fansly/user/123456"
        social = "https://fansly.com/example"
        for urls in ([social, link], [link, social]):
            self.assertEqual(parse(urls)["coomer"], "https://site1.coomer.com.co/fansly/user/123456")

    def test_profile_validation(self):
        normalise = namespace["_coomer_profile_url"]
        self.assertEqual(normalise("http://www.coomer.st/fansly/user/123/?x=1"), "https://site1.coomer.com.co/fansly/user/123")
        self.assertEqual(normalise("https://site1.coomer.com.co/candfans/user/example"), "https://site1.coomer.com.co/candfans/user/example")
        for url in ("https://coomer.st.evil.test/fansly/user/123", "https://other.test/?coomer.st", "https://coomer.st/fansly/user/123/post/456", "https://coomer.st/fansly/user/a%2Fb", "https://coomer.st/"):
            self.assertIsNone(normalise(url))

    def test_missing_icon_does_not_switch_services(self):
        calls = []
        def get(url, **kwargs):
            calls.append(url)
            return SimpleNamespace(status_code=404)
        namespace["requests"] = SimpleNamespace(get=get)
        namespace["emit"] = lambda message: None
        self.assertEqual(namespace["_coomer_fetch_headshot"]("https://coomer.st/fansly/user/123"), (None, None))
        self.assertEqual(calls, ["https://img.coomer.st/icons/fansly/123"])

    def test_nested_metadata_and_other_sources(self):
        self.assertEqual(parse({"extras": {"links": [{"url": "https://www.themoviedb.org/person/42"}]}}), {"tmdb": "42"})

if __name__ == "__main__":
    unittest.main()

class CoomerSearchTests(unittest.TestCase):
    def test_profile_popup_uses_shared_search_and_reports_failures(self):
        import asyncio
        from unittest.mock import patch
        fn = next(n for n in module.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'api_performers_search_ext_link')
        fn.decorator_list = []
        async def run(fn, *args): return fn(*args)
        env = {'Body': lambda *a: None, '_ext_site_key': str.lower,
               'db': SimpleNamespace(favourite_get=lambda rid: {'kind':'performer','folder_name':'Example'}),
               're':re, 'run_maint':run, 'JSONResponse':lambda content, **kw: (content,kw)}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),env)
        url = 'https://site1.coomer.com.co/onlyfans/user/example'
        with patch('app.core.performer_watchlist.search_external', return_value=([{'name':'Example','url':url,'image':'avatar'}], 'Checked')) as search:
            result = asyncio.run(env[fn.name]({'row_id':1,'site':'coomer','q':'Example'}))
            self.assertEqual(result['results'][0], {'name':'Example','url':url,'thumb_url':'avatar'})
            self.assertEqual(namespace['_coomer_profile_url'](url), url)
            search.assert_called_once_with('Coomer',['Example'])
        with patch('app.core.performer_watchlist.search_external', return_value=([], 'Check failed — retry later')):
            result = asyncio.run(env[fn.name]({'row_id':1,'site':'coomer'}))
            self.assertEqual(result[1]['status_code'],503)
