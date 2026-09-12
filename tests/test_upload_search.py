import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock
import unittest

ROOT = Path(__file__).resolve().parents[1]
def load(names, ns):
    tree = ast.parse((ROOT/'main.py').read_text())
    nodes = [n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'test','exec'),ns)
    return ns

class UploadSearchTests(unittest.TestCase):
    def test_upload_limits_without_trusting_size(self):
        read=load({'_read_image_upload'},dict(UploadFile=object,MAX_IMAGE_UPLOAD_BYTES=10))['_read_image_upload']
        for size,content in ((11,b''),(None,b'x'*11),(0,b'')):
            upload=SimpleNamespace(filename='example.png',size=size,read=AsyncMock(return_value=content))
            with self.assertRaises(ValueError): asyncio.run(read(upload))
        upload=SimpleNamespace(filename='ok.png',size=None,read=AsyncMock(return_value=b'x'*10))
        self.assertEqual(asyncio.run(read(upload)),b'x'*10)
        upload.read.assert_awaited_once_with(11)

    def test_indexer_pagination_and_repeated_page_guard(self):
        import xml.etree.ElementTree as ET
        def xml(offset,titles):
            return '<rss xmlns:n="urn:newznab"><channel><n:response offset="%s" total="3"/>%s</channel></rss>' % (offset,''.join('<item><title>%s</title></item>'%x for x in titles))
        get=Mock(side_effect=[SimpleNamespace(status_code=200,text=xml(0,['a','b'])),SimpleNamespace(status_code=200,text=xml(2,['c']))])
        parse=lambda text,*_: [dict(title=x.text,guid=x.text) for x in ET.fromstring(text).findall('.//title')]
        ns=load({'_search_indexer'},dict(requests=SimpleNamespace(get=get),db=SimpleNamespace(clear_indexer_cache=Mock()),_parse_newznab_xml=parse))
        search=ns['_search_indexer']
        self.assertEqual(len(search('http://test','key',{'id':1,'name':'Test'},'name')),3)
        self.assertEqual(get.call_args.kwargs['params']['offset'],2)
        get.side_effect=None;get.return_value=SimpleNamespace(status_code=200,text=xml(0,['a','b']));get.reset_mock()
        self.assertEqual(len(search('http://test','key',{'id':1,'name':'Test'},'name')),2)
        self.assertEqual(get.call_count,2)

    def test_results_are_not_cut_off_at_25(self):
        items=[dict(type='nzb',age=i,title=str(i)) for i in range(80)]
        ns=load({'prowlarr_search', '_prowlarr_result_sort_key'},dict(_prowlarr_url=lambda:'http://test',db=SimpleNamespace(get_settings=lambda:{'prowlarr_api_key':'test'}),_get_indexers=lambda:[{}],_search_indexer=lambda *args:items))
        self.assertEqual(len(ns['prowlarr_search']('performer')),80)

    def test_results_group_by_type_then_publication_not_seeders(self):
        key=load({'_prowlarr_result_sort_key'}, {})['_prowlarr_result_sort_key']
        rows=[{'title':'old torrent','type':'torrent','published_ts':100,'seeders':900},
              {'title':'undated nzb','type':'nzb'},
              {'title':'new torrent','type':'torrent','published_ts':400,'seeders':1},
              {'title':'old nzb','type':'nzb','published_ts':200},
              {'title':'new nzb','type':'nzb','published_ts':300},
              {'title':'undated torrent','type':'torrent'}]
        self.assertEqual([r['title'] for r in sorted(rows,key=key)],
            ['new nzb','old nzb','undated nzb','new torrent','old torrent','undated torrent'])
        self.assertLess(key({'type':'nzb','age':1}),key({'type':'nzb','age':10}))
