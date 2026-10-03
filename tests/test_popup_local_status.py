import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest

class PopupLocalStatusTests(unittest.TestCase):
    def test_cached_bio_uses_current_library_flags(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        endpoint=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_performer_popup')
        branch=next(n for n in endpoint.body if isinstance(n,ast.If) and ast.unparse(n.test)=='not refresh')
        wrapper=ast.parse('async def cached_response(): pass').body[0]
        wrapper.body=[branch]
        module=ast.fix_missing_locations(ast.Module(body=[wrapper],type_ignores=[]))
        cached={'bio':{'biography':'Cached external biography'},'library_status':{'row_id':1,'in_library':True,'matches_locked':False,'is_favourite':False}}
        live={'matches_locked':1,'is_favourite':1}
        async def run(fn,*args): return fn(*args)
        ns={'refresh':False,'cache_key':'row_1','_performer_popup_cache_get':lambda _:cached,
            'run_in_threadpool':run,'JSONResponse':lambda data:data,
            'db':SimpleNamespace(performer_bio_merge=lambda rid,bio:bio,favourite_get=lambda rid:live)}
        exec(compile(module,'cached-popup','exec'),ns)
        result=asyncio.run(ns['cached_response']())
        self.assertTrue(result['library_status']['matches_locked'])
        self.assertTrue(result['library_status']['is_favourite'])
        self.assertEqual(result['bio']['biography'],'Cached external biography')
        live['matches_locked']=0
        self.assertFalse(asyncio.run(ns['cached_response']())['library_status']['matches_locked'])
        ns['db'].favourite_get=lambda rid:None
        self.assertFalse(asyncio.run(ns['cached_response']())['library_status']['in_library'])

if __name__=='__main__': unittest.main()
