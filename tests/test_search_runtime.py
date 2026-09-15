import ast
from pathlib import Path
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import search_runtime as runtime

class SearchRuntimeTests(unittest.TestCase):
    def test_page_budget_returns_resumable_offset(self):
        def get(url, **kw):
            offset=kw['params']['offset']
            text=f'<rss xmlns:n="urn:newznab"><channel><n:response offset="{offset}" total="999"/><item><title>{offset}</title></item></channel></rss>'
            return SimpleNamespace(status_code=200,text=text)
        parser=lambda text,*args:[{'guid':text,'title':text}]
        with patch.object(runtime,'MAX_PAGES',2):
            result=runtime.fetch_indexer('http://fixture','key',{'id':1,'name':'Fixture'},'query',SimpleNamespace(get=get),parser,lambda:None)
        self.assertEqual(len(result),2)
        self.assertEqual(result.continuation,{'1':2})
        self.assertTrue(result.warnings)

    def test_expired_deadline_does_not_start_requests(self):
        request=Mock()
        result=runtime.fetch_indexer('http://fixture','key',{'id':1,'name':'Fixture'},'query',request,Mock(),Mock(),time.monotonic()-1)
        request.get.assert_not_called()
        self.assertEqual(result.continuation,{'1':0})

    def test_capacity_failure_is_explicit_and_retryable(self):
        with patch.object(runtime,'ADMISSION') as admission:
            admission.acquire.return_value=False
            fetch=Mock()
            result=runtime.run_indexers('http://fixture','key',[{'id':1,'name':'Fixture'}],'q',fetch)
        self.assertEqual(result.continuation,{'1':0})
        self.assertTrue(result.warnings)
        fetch.assert_not_called()

    def test_return_paths_reject_schemes_authorities_and_encoded_controls(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_safe_return_path')
        ns={};exec(compile(ast.Module(body=[node],type_ignores=[]),'return-path','exec'),ns)
        safe=ns['_safe_return_path']
        for value in ['javascript:alert(1)','https://example.test','//example.test','/\\example.test','/%2fexample.test','/%0aevil',None]:
            self.assertEqual(safe(value),'/unmatched')
        self.assertEqual(safe('/performer/1?tab=images'),'/performer/1?tab=images')

if __name__=='__main__':unittest.main()
