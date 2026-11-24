import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT=Path(__file__).resolve().parents[1]

def load(path,name,ns):
    tree=ast.parse((ROOT/path).read_text())
    node=next(n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),path,'exec'),ns)
    return ns[name]

class FingerprintFailureTests(unittest.TestCase):
    def run_case(self, outcomes, asynchronous=False, keys=None):
        keys=keys if keys is not None else dict.fromkeys(('stashdb','tpdb','fansdb','javstash'),'key')
        ns={'get_api_keys':lambda:keys,'emit':lambda m:None,'logger':SimpleNamespace(info=lambda m:None)}
        for source in ('STASHDB','TPDB','FANSDB','JAVSTASH'):
            ns[source+'_ENDPOINT']=source.lower();ns[source+'_QUERY']='query'
        def query(phash,endpoint,*args):
            result=outcomes[endpoint]
            if isinstance(result,Exception):raise result
            return result
        if asynchronous:
            class Client:
                async def __aenter__(self):return self
                async def __aexit__(self,*args):pass
            async def client():return Client()
            async def aq(client,*args):return query(*args)
            ns.update(get_async_client=client,query_stashbox=aq)
            fn=load('app/integrations/stashbox.py','query_with_fallback',ns)
            return asyncio.run(fn('hash',keys))
        ns['query_stashbox']=query
        return load('main.py','query_with_fallback',ns)('hash')
    def test_failures_do_not_turn_into_misses(self):
        for async_ in (False,True):
            with self.subTest(async_=async_),self.assertRaisesRegex(RuntimeError,'incomplete'):
                self.run_case(dict(stashdb=RuntimeError('offline'),tpdb=[],fansdb=[],javstash=[]),async_)
    def test_later_match_is_still_usable(self):
        for async_ in (False,True):
            self.assertEqual(self.run_case(dict(stashdb=RuntimeError('offline'),tpdb=[{'id':'1'}]),async_),([{'id':'1'}],'TPDB'))
    def test_genuine_miss_and_missing_keys(self):
        for async_ in (False,True):
            self.assertEqual(self.run_case({'fansdb':[]},async_,{'fansdb':'key'}),([], 'none'))
            with self.assertRaisesRegex(RuntimeError,'No fingerprint'):
                self.run_case({},async_,{})
    def test_ambiguous_source_does_not_hide_later_match(self):
        for async_ in (False, True):
            result = self.run_case(dict(stashdb=[{'id':'a'},{'id':'b'}],tpdb=[{'id':'clear'}]),async_)
            self.assertEqual(result,([{'id':'clear'}],'TPDB'))
            result = self.run_case(dict(stashdb=[{'id':'a'},{'id':'b'}],tpdb=[],fansdb=[],javstash=[]),async_)
            self.assertEqual(result[1],'StashDB')

    def test_malformed_results_raise(self):
        for payload in ({}, {'data':{}}, *({'data':{'findScenesBySceneFingerprints':value}} for value in (None, [None], [{}], [{'id':''}], [[], None], [[None]]))):
            response=SimpleNamespace(status_code=200,json=lambda:payload)
            fn=load('main.py','query_stashbox',{'requests':SimpleNamespace(post=lambda *a,**k:response)})
            with self.assertRaisesRegex(RuntimeError,'Invalid fingerprint'):
                fn('hash','endpoint','key','query','scene')
