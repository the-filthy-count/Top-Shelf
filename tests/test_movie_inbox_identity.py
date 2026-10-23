import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load
from download_safety import PartialDownloadPending

class MovieInboxIdentityTests(unittest.TestCase):
    def test_same_relative_name_has_different_history_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);movies=root/'movies';jav=root/'jav';movies.mkdir();jav.mkdir()
            for folder in (movies,jav):(folder/'clip.mp4').touch()
            upsert=Mock();compare=Mock(return_value={'status':'removed'})
            env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{'movies_source_dir':str(movies),'jav_download_watch_dir':str(jav)},upsert_movie=upsert,update_movie=Mock(),media_file_signature=lambda p:'stable'),
                'require_complete_queue_video':lambda *a:None,'PartialDownloadPending':PartialDownloadPending,'emit':Mock(),'compute_phash':lambda p:'hash','_queue_resolve_phash_vs_indexed_library':compare}
            process=load('main.py','process_movie_file',env)
            process('clip.mp4',src=movies);process('clip.mp4',src=jav)
            self.assertEqual([c.args[0] for c in upsert.call_args_list],['clip.mp4','jav://clip.mp4'])
            self.assertEqual([c.args[1] for c in compare.call_args_list],['clip.mp4','jav://clip.mp4'])

    def test_empty_configuration_never_scans_or_reconciles(self):
        db=SimpleNamespace(get_settings=lambda:{},reconcile_movies_source=Mock(),get_movie_rows_map=Mock())
        scan=Mock(side_effect=AssertionError('unexpected scan'))
        env={'Path':Path,'db':db,'_movie_source_video_relpaths':scan,'_movie_pipeline_lock':threading.Lock(),'processing_state':{'running':False},'emit':Mock()}
        self.assertIn('error',load('main.py','_build_movies_queue_snapshot',env)())
        load('main.py','run_movie_pipeline',env)()
        self.assertEqual(load('main.py','process_movie_file',env)('clip.mp4')['status'],'error')
        scan.assert_not_called();db.reconcile_movies_source.assert_not_called()
