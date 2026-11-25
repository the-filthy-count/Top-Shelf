import sys,unittest,asyncio

from test_unified_matching_pipeline import UnifiedPipelineTests
from test_duplicate_filing_safety import load
from unittest.mock import Mock,patch
from pathlib import Path
from download_safety import PartialDownloadPending
from app.core import queue_changes
import database as db
import startup_attempts

class PipelineRetryBoundaries(UnifiedPipelineTests):
    async def test_requeue_preserves_interrupted_filing_review(self):
        _,key,fid=self.add('interrupted.mp4')
        db.processing_job_start(key);queue_changes.begin_effects()
        with db.get_conn() as c:
            queue_changes.recover(c)
            self.assertEqual(c.execute('SELECT state FROM queue_changes').fetchone()[0],'blocked')
        queue_changes.release_processing()
        db.processing_job_enqueue(key)
        self.assertEqual(db.processing_work_ready(),[])
        self.assertFalse(queue_changes.schedule_suggestions(key))
        self.assertNotIn(key, db.get_retry_files())
        with db.get_conn() as c:
            self.assertEqual(c.execute('SELECT state FROM queue_changes').fetchone()[0],'blocked')

    async def test_suggestion_result_preserves_video_outcome_for_startup_tracking(self):
        video,key,fid=self.add('retry-on-boot.mp4')
        sig=startup_attempts.signature(video)
        results=await self.pipeline.run_pipeline([])
        self.assertEqual(db.processing_work_result(fid)['status'],'unmatched')
        self.assertEqual(results[fid]['status'],'unmatched')
        startup_attempts.remember(db,video,sig,results[fid])
        self.assertTrue(startup_attempts.unchanged(db,video))

    async def test_movie_provider_outage_is_retryable(self):
        video,key,fid=self.add('movie.mp4','processed_movies','movies')
        db.save_settings({'api_key_tmdb':'test-key'})
        sig=db.media_file_signature(video)
        env={'Path':Path,'db':db,'PartialDownloadPending':PartialDownloadPending,
             'json':__import__('json'),'emit':Mock(),'require_complete_queue_video':Mock(),
             'get_video_duration':Mock(),'compute_phash':Mock(),
             '_queue_resolve_phash_vs_indexed_library':lambda *a,**kw:None,
             'guess_movie_query_from_filename':lambda name:('Movie',None),
             'search_tmdb':Mock(side_effect=TimeoutError('provider unavailable')),
             '_verified_movie_candidate':lambda *a:None,'search_tpdb_movie_first_hit':lambda *a:None}
        self.pipeline.callbacks['process_movie']=load('main.py','_process_movie_file_impl',env)
        with patch('app.core.queue_media.read',return_value={'phash':'hash','media_signature':sig}):
            results=await self.pipeline.run_pipeline([])
        self.assertEqual(results[fid]['status'],'error')
        self.assertEqual(db.processing_work_result(fid)['status'],'error')
        self.assertEqual(db.processing_work_ready(),[])
        with db.get_conn() as c:
            self.assertEqual(c.execute('SELECT state FROM queue_changes').fetchone()[0],'pending')


    async def test_normal_no_destination_can_be_requeued_after_effects(self):
        _,key,fid=self.add('no-destination.mp4')
        db.processing_job_start(key)
        queue_changes.begin_effects()
        with db.get_conn() as c:
            queue_changes.finish_processing(c,fid,'no_dir',None)
        db.processing_job_enqueue(key)
        self.assertEqual(db.processing_work_ready()[0]['file_id'],fid)

class MovieProviderTests(unittest.TestCase):
    def test_tpdb_http_failure_is_not_a_miss(self):
        response=Mock()
        response.raise_for_status.side_effect=RuntimeError('HTTP 503')
        env={'db':Mock(get_settings=lambda:{'api_key_tpdb':'test'}),
             'requests':Mock(get=Mock(return_value=response)),
             '_verified_movie_candidate':Mock()}
        search=load('main.py','search_tpdb_movie_first_hit',env)
        with patch('app.integrations.fingerprint_cooldown.send',side_effect=lambda endpoint,call:call()):
            with self.assertRaisesRegex(RuntimeError,'503'):
                search('Movie',None)
        env['_verified_movie_candidate'].assert_not_called()
