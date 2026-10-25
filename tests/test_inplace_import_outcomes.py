from contextvars import ContextVar
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from test_duplicate_filing_safety import load
from download_safety import PartialDownloadPending, require_complete_download

class ImportOutcomeTests(unittest.TestCase):
    def check_outcome(self,kind,status,loose=False):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);entry=root/'album';entry.mkdir();video=entry/'clip.mp4';video.touch()
            if loose:
                video.unlink();video=root/'clip.mp4';video.touch();entry=video
                (root/'unrelated.mp4.part').touch()
            cleanup=Mock();retry=Mock()
            def process(*args,**kwargs):
                self.assertTrue(env['_defer_client_cleanup'].get())
                if status=='exception':raise PartialDownloadPending('download resumed')
                if status=='error':raise OSError('failed')
                if status in ('filed','removed'):video.unlink()
                return {'status':status}
            env={'_defer_client_cleanup':ContextVar('test_cleanup',default=False),'Path':Path,'require_complete_download':require_complete_download,'VIDEO_EXTENSIONS':{'.mp4'},
                 '_clean_release_filename':lambda n:n,'emit':Mock(),'PartialDownloadPending':PartialDownloadPending,
                 'db':SimpleNamespace(upsert_file=Mock(),queue_file_key=lambda v,s:v.name),
                 '_pipeline_lock':threading.Lock(),'_movie_pipeline_lock':threading.Lock(),
                 'processing_state':{},'process_single':process,'process_movie_file':process,
                 '_remove_client_jobs_matching_watch_folder':cleanup,'_retry_partial_download':retry,
                 '_pending_vice_downloads':{},'_pending_jav_downloads':{}}
            fn=load('main.py',f'_process_{kind}_download_entry',env)
            with patch('startup_attempts.remember'),patch('startup_attempts.unchanged',return_value=False):
                if kind=='vice':fn(entry,root,{'vice_download_watch_dir':str(root)})
                else:fn(entry,{'jav_download_watch_dir':str(root)})
            self.assertEqual(cleanup.call_count,1 if status in ('filed','removed') else 0)
            self.assertEqual(retry.call_count,1 if status in ('exception','deferred') else 0)
            self.assertFalse(env['_defer_client_cleanup'].get())
            self.assertFalse(env['processing_state']['running'])
            self.assertFalse(env['_pipeline_lock'].locked());self.assertFalse(env['_movie_pipeline_lock'].locked())
    def test_outcomes_for_both_importers(self):
        for kind in ('vice','jav'):
            for status in ('filed','removed','unmatched','deferred','exception','error'):
                with self.subTest(kind=kind,status=status):self.check_outcome(kind,status)

    def test_loose_files_ignore_unrelated_partial(self):
        for kind in ('vice', 'jav'):
            with self.subTest(kind=kind):
                self.check_outcome(kind, 'filed', loose=True)

    def test_release_folder_with_partial_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            entry=Path(tmp);(entry/'clip.mp4').touch();(entry/'other.part').touch()
            for kind in ('vice', 'jav'):
                fn=load('main.py',f'_process_{kind}_download_entry',{'Path':Path,'require_complete_download':require_complete_download})
                with self.subTest(kind=kind),self.assertRaises(PartialDownloadPending):
                    if kind=='vice':fn(entry,entry,{})
                    else:fn(entry,{})
