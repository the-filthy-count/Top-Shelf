import ast
import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load, ROOT
from download_safety import PartialDownloadPending

class MovieAndReviewTests(unittest.TestCase):
    def test_movie_changes_defer_before_destructive_work(self):
        for stage in ('hash', 'lookup', 'stable'):
            with self.subTest(stage=stage),tempfile.TemporaryDirectory() as tmp:
                video=Path(tmp)/'movie.mp4';video.write_bytes(b'original')
                def fingerprint(p):
                    if stage=='hash':p.write_bytes(b'changed')
                    return 'hash'
                def detail(mid):
                    if stage=='lookup':video.write_bytes(b'changed')
                    return {'id':mid,'title':'Movie'}
                compare=Mock(return_value=None);file=Mock(return_value={'status':'filed'})
                env={'Path':Path,'PartialDownloadPending':PartialDownloadPending,'emit':Mock(),
                     'db':SimpleNamespace(get_settings=lambda:{'api_key_tmdb':'key'},upsert_movie=Mock(),update_movie=Mock(),media_file_signature=lambda p:p.read_bytes()),
                     'require_complete_queue_video':lambda *a:None,'compute_phash':fingerprint,
                     '_queue_resolve_phash_vs_indexed_library':compare,'guess_movie_query_from_filename':lambda n:('Movie',None),
                     'search_tmdb':lambda *a:[{'id':'movie-id','title':'Movie'}],'get_tmdb_movie':detail,
                     'file_movie':file,'_cleanup_superseded_library_copies':Mock()}
                env['re']=__import__('re')
                load('main.py','_verified_movie_candidate',env)
                result=load('main.py','process_movie_file',env)(video.name,src=video.parent)
                self.assertEqual(result['status'],'filed' if stage=='stable' else 'deferred')
                self.assertEqual(file.call_count,1 if stage=='stable' else 0)
                self.assertEqual(compare.call_count,0 if stage=='hash' else 1)

    def test_duplicate_link_uses_saved_id_without_network(self):
        node=next(n for n in ast.parse((ROOT/'main.py').read_text()).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='health_duplicate_db_record')
        node.decorator_list=[]
        lookup=Mock(side_effect=AssertionError('should not search again'))
        env={'Query':lambda x:x,'db':SimpleNamespace(library_file_get_by_destination=lambda p:{'source_record_id':1},
             processed_file_get_by_id=lambda i:{'match_source':'StashDB','match_external_id':'saved-id','match_title':'Original'}),
             '_api_source_tag':lambda s:'stashdb','_endpoint_query_var_for_match_source':lookup}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<test>','exec'),env)
        result=asyncio.run(env['health_duplicate_db_record']('/library/movie.mp4'))
        self.assertEqual(result['match_id'],'saved-id');self.assertEqual(result['title'],'Original')
        lookup.assert_not_called()
