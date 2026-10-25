"""Run actual image/refresh functions in isolation; never start NAS background jobs."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import tempfile
import unittest

SOURCE=Path(__file__).resolve().parents[1]/'main.py'
NAMES={'_favourite_images_locked','_maybe_kick_performer_image_enrichment','api_favourites_refresh','_favourites_refresh_all_loop','favourites_refresh_entity_row','favourites_refresh_entity_images','_enrich_performer_headshot_bg','_enrich_performer_headshot_ondemand','_enrich_performer_ext_links_bg','_save_headshot_bytes_from_source','_tmdb_save_performer_headshot','_performers_iafd_save','_ensure_performer_posters','_ensure_movie_images','_ensure_movie_back_image','_ensure_studio_images','_ensure_studio_poster_path','_sync_entity_library_images','_restore_performer_from_library_mirrors','_restore_studio_from_library_mirrors'}
TREE=ast.parse(SOURCE.read_text())
FUNCS=[n for n in TREE.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in NAMES]
for f in FUNCS:f.decorator_list=[]
CODE=compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),*FUNCS],type_ignores=[])),str(SOURCE),'exec')

class LockedImages(unittest.TestCase):
 def setUp(self):
  self.row={'id':1,'kind':'performer','matches_locked':1,'folder_name':'Fixture'}
  self.db=SimpleNamespace(favourite_get=Mock(side_effect=lambda _:dict(self.row)),get_settings=Mock(return_value={'api_key_tmdb':'fixture'}))
  self.g={'db':self.db,'Body':lambda x:x,'Path':Path,'emit':Mock(),'_submit_enrichment':Mock(),'_favourites_progress_update':Mock(),'favourites_clear_image_disk_cache':Mock(),'requests':Mock(),'save_image_bytes_optimized':Mock()}
  exec(CODE,self.g)
 def test_refresh_never_clears_locked_art(self):
  result=asyncio.run(self.g['api_favourites_refresh']({'id':1}))
  self.assertTrue(result['skipped']);self.g['favourites_clear_image_disk_cache'].assert_not_called()
  self.assertTrue(self.g['favourites_refresh_entity_row'](1)['skipped'])
  self.assertTrue(self.g['favourites_refresh_entity_images'](1)['skipped'])
  self.g['_submit_enrichment'].assert_not_called()
 def test_bulk_uses_current_lock_not_queued_snapshot(self):
  self.g['_favourites_refresh_all_loop']([dict(self.row,matches_locked=0)])
  self.g['favourites_clear_image_disk_cache'].assert_not_called()
  self.assertEqual(self.g['_favourites_progress_update'].call_args.args,('Fixture',1))
 def test_locked_workers_and_writer_do_no_work(self):
  for name in ['_enrich_performer_headshot_bg','_enrich_performer_headshot_ondemand','_enrich_performer_ext_links_bg']:
   self.g[name](1)
  self.g['_maybe_kick_performer_image_enrichment'](self.row)
  self.assertFalse(self.g['_save_headshot_bytes_from_source'](1,'Fixture',b'bytes','test'))
  self.assertFalse(self.g['_tmdb_save_performer_headshot'](1,'123'))
  self.g['requests'].get.assert_not_called();self.g['save_image_bytes_optimized'].assert_not_called()
 def test_locked_iafd_keeps_existing_metadata(self):
  self.g['_performers_iafd_read_meta']=Mock(return_value={'image_files':['original.jpg']})
  self.assertEqual(self.g['_performers_iafd_save'](1,'https://example.test'),({'image_files':['original.jpg']},0))
 def test_locked_posters_still_serve_existing(self):
  self.g['_perf_poster_resolve']=lambda _,role:Path('/original.jpg') if role=='poster_primary' else None
  self.assertEqual(self.g['_ensure_performer_posters'](1),[Path('/original.jpg')])
 def test_locked_mirrors_no_writes(self):
  self.assertEqual(self.g['_sync_entity_library_images'](1)['saved'],[])
  for kind in ['performer','studio']:
   self.assertEqual(self.g['_restore_'+kind+'_from_library_mirrors'](1),{'restored':[]})
 def test_movie_and_studio_keep_cached_images_and_do_not_fill_gaps(self):
  with tempfile.TemporaryDirectory() as folder:
   self.g['_metadata_dir_for']=lambda *_:Path(folder)
   self.g['_sync_entity_library_images_safe']=Mock()
   for kind,name in [('movie','_ensure_movie_images'),('jav','_ensure_movie_images'),('studio','_ensure_studio_images')]:
    self.row['kind']=kind
    self.assertIsNone(self.g[name](1))
    path=Path(folder)/('1_logo.webp' if kind=='studio' else '1_poster.webp')
    path.write_bytes(b'x'*600)
    self.assertEqual(self.g[name](1),path)
    self.assertEqual(path.read_bytes(),b'x'*600);path.unlink()
 def test_unlocked_refresh_still_runs(self):
  self.row['matches_locked']=0
  refresh=Mock(return_value={'ok':True});self.g['favourites_refresh_entity_row']=refresh
  asyncio.run(self.g['api_favourites_refresh']({'id':1}))
  refresh.assert_called_once();self.g['favourites_clear_image_disk_cache'].assert_called_once_with(1)
 def test_lock_during_tmdb_download_blocks_write(self):
  self.row['matches_locked']=0
  first=Mock();first.json.return_value={'profiles':[{'file_path':'/fixture.jpg'}]}
  second=Mock();second.content=b'fixture'
  def get(*args,**kwargs):
   if args[0].endswith('/images'):return first
   self.row['matches_locked']=1;return second
  self.g['requests'].get.side_effect=get
  self.g['TMDB_BASE']='https://example.test';self.g['_iafd_bytes_look_like_image']=lambda _:True
  self.assertFalse(self.g['_tmdb_save_performer_headshot'](1,'123'))
  self.g['save_image_bytes_optimized'].assert_not_called()
if __name__=='__main__':unittest.main()
