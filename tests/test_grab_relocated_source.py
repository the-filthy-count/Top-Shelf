import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class GrabRelocatedSourceTests(unittest.TestCase):
    def test_relocation_and_filing_errors_never_fall_back(self):
        for fail in (False,True):
            with self.subTest(fail=fail),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);video=root/'clip.mp4';video.touch();group=root/'Group';group.mkdir()
                def file(*args,**kw):
                    video.rename(group/video.name)
                    if fail:raise OSError('filing failed')
                    return {'status':'no_dir'}
                query=Mock(side_effect=AssertionError('must not fall back'))
                db=SimpleNamespace(get_settings=lambda:{},queue_file_key=lambda *a:'clip.mp4',upsert_file=Mock(),validate_media_cache=lambda *a:'sig',get_directories=lambda *a:[],get_media_duration=lambda *a:60,get_phash=lambda *a:'hash',media_file_signature=lambda *a:'sig',update_file=Mock(),lookup_scene_grab_for_filename=lambda *a:{'id':1,'source_id':'scene','source_title':'Title','source_db':'StashDB'},mark_scene_grab_consumed=Mock())
                env={'Path':Path,'db':db,'emit':Mock(),'require_complete_queue_video':lambda *a:None,'_queue_resolve_phash_vs_indexed_library':lambda *a:None,'_strip_ts_uid':lambda n:(n,''),'_stashbox_normalize_match_source':lambda s:s,'_fetch_grab_scene_for_filing':lambda g:{'id':'scene'},'file_scene_from_match':file,'query_with_fallback':query}
                process=load('main.py','process_single',env)
                if fail:
                    with self.assertRaises(OSError):process(video)
                else:self.assertEqual(process(video),{'status':'no_dir'})
                query.assert_not_called();db.mark_scene_grab_consumed.assert_not_called()
                self.assertTrue((group/'clip.mp4').exists())
                self.assertEqual(db.update_file.call_count,1)
