import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class ApplyMetadataSafetyTests(unittest.TestCase):
    def test_collision_and_failures_preserve_original(self):
        for case in ('collision','move_failure','write_failure','write_exception','success'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);src=root/'old.mp4';dst=root/'new.mp4';src.write_bytes(b'original')
                nfo=src.with_suffix('.nfo');nfo.write_text('old metadata')
                if case=='collision':dst.write_bytes(b'incumbent')
                def move(a,b):
                    self.assertTrue(nfo.exists())
                    if case=='move_failure':raise OSError('disk full')
                    a.rename(b)
                def write(*a,**kw):
                    self.assertTrue(nfo.exists())
                    if case=='write_exception':raise OSError('write denied')
                    if case=='write_failure':return {'status':'error','error':'write denied'}
                    dst.with_suffix('.nfo').write_text('new metadata')
                    return {'status':'ok'}
                db=SimpleNamespace(library_file_get_by_destination=lambda p:{'id':1,'source_record_id':2},
                    processed_file_get_by_id=lambda i:{'id':2},get_settings=lambda:{},
                    library_file_update_destination_row=Mock(),library_files_clear_duplicate_pending=Mock(),processed_file_update_library_destination=Mock())
                env={'Path':Path,'db':db,'_fetch_scene_for_processed_row':lambda p:({'id':'scene'},'StashDB'),
                     '_scene_filing_plan':lambda *a:{'status':'ok','dest_season':root,'base_name':'new'},
                     'safe_move':Mock(side_effect=move),'write_library_scene_nfo_and_thumb':Mock(side_effect=write)}
                load('main.py','_health_unlink_nfo_thumb_sidecars',env)
                result=load('main.py','_duplicate_apply_metadata_sync',env)(str(src),2)
                self.assertEqual(result['ok'],case=='success')
                if case=='success':
                    self.assertFalse(src.exists());self.assertFalse(nfo.exists())
                    self.assertEqual(dst.read_bytes(),b'original')
                    db.library_file_update_destination_row.assert_called_once()
                else:
                    self.assertEqual(src.read_bytes(),b'original');self.assertEqual(nfo.read_text(),'old metadata')
                    db.library_file_update_destination_row.assert_not_called()
                if case=='collision':
                    self.assertEqual(dst.read_bytes(),b'incumbent');env['safe_move'].assert_not_called()
