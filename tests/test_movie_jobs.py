import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class MovieJobsTests(unittest.TestCase):
    def test_movie_and_jav_admission_use_same_worker_with_separate_identities(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);jav=root/'jav';movies=root/'movies'
            enqueue=Mock();upsert=Mock();run=Mock(return_value={'asset':{'status':'filed'}})
            db=SimpleNamespace(get_settings=lambda:{'jav_download_watch_dir':str(jav)},upsert_movie=upsert,
                processing_job_enqueue=enqueue,file_identity=lambda *a:{'id':'asset'})
            process=load('main.py','process_movie_file',{'Path':Path,'db':db,'run_pipeline':run})
            self.assertEqual(process('same.mp4',movies)['status'],'filed')
            self.assertEqual(process('same.mp4',jav,'jav')['status'],'filed')
            self.assertEqual([c.args[0] for c in upsert.call_args_list],['same.mp4','jav://same.mp4'])
            self.assertEqual([c.args for c in enqueue.call_args_list],[('same.mp4','processed_movies'),('jav://same.mp4','processed_movies')])
            self.assertEqual(run.call_count,2)
