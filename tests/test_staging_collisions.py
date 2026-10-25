import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load
from download_safety import require_complete_download, require_complete_queue_video, PartialDownloadPending

class StagingSafetyTests(unittest.TestCase):
    def test_multiple_collisions_preserve_existing_files(self):
        move=load('main.py','_move_staged_video',{'Path':Path,'_staging_move_lock':threading.Lock(),'safe_move':lambda a,b:a.rename(b)})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);destination=root/'queue';destination.mkdir()
            for name in ('clip.mp4','clip_1.mp4','clip_2.mp4'):(destination/name).write_bytes(b'original')
            src=root/'incoming.mp4';src.write_bytes(b'new')
            self.assertEqual(move(src,destination/'clip.mp4').name,'clip_3.mp4')
            for name in ('clip.mp4','clip_1.mp4','clip_2.mp4'):self.assertEqual((destination/name).read_bytes(),b'original')

    def test_loose_staging_ignores_unrelated_partial(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);video=root/'clip.mp4';video.touch();(root/'other.part').touch()
            move=Mock(return_value=root/'out'/'clip.mp4')
            env={'Path':Path,'require_complete_download':require_complete_download,'emit':Mock(),
                 '_strip_ts_uid':lambda s:(s,''),'db':SimpleNamespace(lookup_scene_grab_by_final_name=lambda n:None),
                 'VIDEO_EXTENSIONS':{'.mp4'},'_clean_release_filename':lambda n:n,'_move_staged_video':move}
            stage=load('main.py','_process_stage_entry_to_queue',env)
            stage(video,root/'out','jav');move.assert_called_once()
            (root/'clip.mp4.part').touch()
            with self.assertRaises(PartialDownloadPending):stage(video,root/'out','jav')
            self.assertTrue(video.exists())

    def test_movie_partial_defers_before_database_or_fingerprint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);album=root/'album';album.mkdir();(album/'clip.mp4').touch();(album/'pending.part').touch()
            db=SimpleNamespace(get_settings=lambda:{},get_movie_by_filename=Mock())
            compute=Mock()
            env={'Path':Path,'db':db,'emit':Mock(),'require_complete_queue_video':require_complete_queue_video,
                 'PartialDownloadPending':PartialDownloadPending,'compute_phash':compute}
            process=load('main.py','process_movie_file',env)
            self.assertEqual(process('album/clip.mp4',src=root,dest_kind='jav'),{'status':'deferred'})
            compute.assert_not_called();db.get_movie_by_filename.assert_not_called()
