import ast
import threading
import time
import unittest
from pathlib import Path
from scene_search_runtime import run_sources

class SceneSearchRuntimeTests(unittest.TestCase):
    def test_slow_source_does_not_hold_fast_results(self):
        release=threading.Event()
        def slow():
            release.wait(2)
            return [('slow','IAFD')]
        start=time.monotonic()
        try:
            result=run_sources([('Fast',lambda:[('fast','TPDB')]),('IAFD',slow)],timeout=.05)
            self.assertLess(time.monotonic()-start,.5)
            self.assertEqual(list(result),[('fast','TPDB')])
            self.assertTrue(any('IAFD' in warning for warning in result.warnings))
        finally:
            release.set()

    def test_failure_does_not_discard_other_results(self):
        def fail():raise ValueError('fixture')
        result=run_sources([('Failed',fail),('Fast',lambda:[1])],timeout=1)
        self.assertEqual(list(result),[1])
        self.assertTrue(result.warnings)

    def test_route_runs_as_synchronous_worker_handler(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        route=next(n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name=='search_scenes')
        self.assertIsInstance(route,ast.FunctionDef)

    def test_metadata_route_queries_sources_separately(self):
        from unittest.mock import Mock
        path=Path(__file__).resolve().parents[1]/'main.py'
        tree=ast.parse(path.read_text())
        route=next(n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name=='metadata_search')
        self.assertIsInstance(route,ast.FunctionDef)
        route.decorator_list=[]
        search=Mock(side_effect=lambda name,**kw:[{'name':name,'source':kw['source']}])
        ns=dict(search_performers=search,ALLOWED_GENDER_BUCKETS={'female'})
        exec(compile(ast.Module(body=[route],type_ignores=[]),str(path),'exec'),ns)
        result=ns['metadata_search']('Fixture',genders='female',strict='0')
        self.assertEqual(len(result['results']),4)
        for call in search.call_args_list:
            self.assertFalse(call.kwargs['hydrate_aliases'])
            self.assertEqual(call.kwargs['gender_allowlist'],frozenset({'female'}))

    def test_retry_reuses_unfinished_source(self):
        release=threading.Event()
        calls=[]
        def slow():
            calls.append(1)
            release.wait(2)
            return ['late result']
        key=('retry-test',time.monotonic())
        try:
            first=run_sources([('Slow',slow)],timeout=.02,cache_key=key)
            self.assertTrue(first.warnings)
            release.set()
            second=run_sources([('Slow',slow)],timeout=1,cache_key=key)
            self.assertEqual(list(second),['late result'])
            self.assertEqual(len(calls),1)
        finally:
            release.set()

    def test_ready_text_hits_skip_date_fallback(self):
        from unittest.mock import Mock
        path=Path(__file__).resolve().parents[1]/'main.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='search_all_databases')
        db=Mock()
        db.get_settings.return_value={'iafd_search_enabled':'false'}
        date_search=Mock(side_effect=AssertionError('Unnecessary date request'))
        ns=dict(db=db,get_api_keys=lambda:dict(stashdb='',tpdb='fixture',fansdb='',javstash=''),
                STASHDB_ENDPOINT='stash',TPDB_ENDPOINT='tpdb',FANSDB_ENDPOINT='fans',JAVSTASH_ENDPOINT='jav',
                _build_search_term=lambda **kwargs:'fixture-ready-test',
                search_scenes_on_db=lambda *_:[{'id':'ready','title':'Fixture','release_date':'2026-09-10'}],
                query_scenes_by_date=date_search)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),ns)
        result=ns['search_all_databases'](title='fixture-ready-test',date_from='2026-09-01',date_to='2026-09-15')
        self.assertEqual(result[0]['id'],'ready')
        date_search.assert_not_called()

if __name__=='__main__':unittest.main()
