import ast
import json
from pathlib import Path
import re
from types import SimpleNamespace
import unicodedata
import unittest
from unittest.mock import Mock
from urllib.parse import urlparse, parse_qsl, urlencode

class URLMatchingTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((Path(__file__).resolve().parents[1]/'app/core/performer_watchlist.py').read_text())
        self.ns = dict(json=json, re=re, unicodedata=unicodedata, urlparse=urlparse, parse_qsl=parse_qsl, urlencode=urlencode,
                       SOURCES={'FansDB':('key','https://example.test/graphql')})
        for node in tree.body:
            if isinstance(node,ast.FunctionDef) and node.name in ('name_key','matches_names','profile_url_key','profile_urls','matches_profile','search_terms','search_source','watchlist_urls'):
                exec(compile(ast.Module(body=[node],type_ignores=[]),'watchlist.py','exec'),self.ns)
    def test_profile_variations(self):
        key=self.ns['profile_url_key']
        for url in ('https://onlyfans.com/Performer','http://www.onlyfans.com/performer/','www.onlyfans.com/performer/trial/abc?ref=123','onlyfans.com/performer?trial=abc'):
            self.assertEqual(key(url),'onlyfans.com/performer')
        self.assertNotEqual(key('onlyfans.com/performer2'),key('onlyfans.com/performer'))
        self.assertNotEqual(key('example.com/performer/trial'),key('example.com/performer'))
        self.assertNotEqual(key('example.com/profile?id=1'),key('example.com/profile?id=2'))
        self.assertEqual(key('https://www.example.com/User/?utm_source=test#top'),'example.com/User')
        self.assertEqual(key('javascript:alert(1)'),'')
    def test_url_match_survives_different_name(self):
        hit={'name':'Different', 'urls':[{'url':'https://onlyfans.com/person/'}]}
        self.assertTrue(self.ns['matches_profile'](hit,['Alias'],['https://www.onlyfans.com/person/trial/123']))
        self.assertFalse(self.ns['matches_profile'](hit,['Alias'],['https://onlyfans.com/other']))
    def test_searches_canonical_url_and_keeps_evidence(self):
        response=Mock()
        response.json.return_value={'data':{'searchPerformer':[{'id':'123','name':'Different','urls':[{'url':'https://onlyfans.com/person/'}]}]}}
        requests=SimpleNamespace(post=Mock(return_value=response))
        self.ns['requests']=requests
        hits,status=self.ns['search_source']('FansDB',['Alias'],{'key':'test'},['https://www.onlyfans.com/person/trial/abc'])
        self.assertEqual(status,'Checked')
        self.assertEqual(hits[0]['urls'],['https://onlyfans.com/person/'])
        self.assertIn('https://onlyfans.com/person',[c.kwargs['json']['variables']['name'] for c in requests.post.call_args_list])
