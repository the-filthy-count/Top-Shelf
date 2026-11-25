import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from app.core.pipeline import ProcessingPipeline

class GrabRelocatedSourceTests(unittest.TestCase):
    def test_relocation_and_filing_errors_never_fall_back(self):
        for fail in (False,True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as tmp:
                video=Path(tmp)/'clip.mp4';video.touch();group=Path(tmp)/'Group';group.mkdir()
                def file(*args,**kw):
                    video.rename(group/video.name)
                    if fail:raise OSError('filing failed')
                    return {'status':'no_dir'}
                pipeline=ProcessingPipeline({},Mock(),Mock(),threading.Lock(),{
                    'queue_resolve_phash':lambda *a:None,'fetch_grab_scene':lambda g:{'id':'scene'},'file_scene_from_match':file})
                async def inline(fn,*args,**kw):return fn(*args,**kw)
                with patch('app.core.pipeline.db') as db, patch('app.core.pipeline.require_complete_queue_video'), patch('app.core.queue_media.read',return_value={'phash':'hash','media_signature':'signature'}), patch('app.core.queue_media.save_match'), patch('app.core.queue_media.assert_signature'), patch('app.core.pipeline.query_with_fallback',new_callable=AsyncMock) as query, patch.object(asyncio,'to_thread',inline):
                    db.get_settings.return_value={};db.queue_file_key.return_value='clip.mp4'
                    db.lookup_scene_grab_by_ts_uid.return_value=None
                    db.lookup_scene_grab_for_filename.return_value={'id':1,'source_id':'scene','source_title':'Title','source_db':'StashDB'}
                    if fail:
                        with self.assertRaises(OSError):asyncio.run(pipeline.process_single(video))
                    else:self.assertEqual(asyncio.run(pipeline.process_single(video)),{'status':'no_dir'})
                    query.assert_not_awaited();db.mark_scene_grab_consumed.assert_not_called()
                    self.assertTrue((group/'clip.mp4').exists())
