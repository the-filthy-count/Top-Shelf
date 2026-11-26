from unittest.mock import patch, AsyncMock
from test_unified_matching_pipeline import UnifiedPipelineTests
from app.integrations import fingerprint_cooldown as cooldown
import database as db
import time

class MatchingPriorityTests(UnifiedPipelineTests):
    async def test_manual_turn_between_files_releases_lock(self):
        order=[]
        jobs=[{'file_id':'one','ready_count':2},{'file_id':'two','ready_count':1}]
        pending=[False]
        async def run(job):
            self.assertTrue(self.pipeline.lock.locked())
            order.append(job['file_id'])
            if job['file_id']=='one':pending[0]=True
            return {'status':'completed'}
        def manual():
            if pending[0]:
                self.assertFalse(self.pipeline.state['running'])
                self.assertTrue(self.pipeline.lock.acquire(False))
                order.append('manual')
                self.pipeline.lock.release();pending[0]=False
                return True
            return False
        self.pipeline.callbacks['manual_pending']=manual
        with patch.object(db,'processing_work_ready',side_effect=[[jobs[0]],[jobs[1]],[]]), patch.object(self.pipeline,'run_job',side_effect=run):
            await self.pipeline.run_pipeline([])
        self.assertEqual(order,['one','manual','two'])
        self.assertFalse(self.pipeline.lock.locked())

    async def test_rate_limits_preserve_retry_time_and_do_not_exhaust_budget(self):
        _,key,fid=self.add('limited.mp4')
        self.query.side_effect=cooldown.RateLimited('HTTP 429', 123)
        for _ in range(4):
            started=time.time()
            result=await self.pipeline.run_pipeline([])
            self.assertEqual(result[fid]['status'],'rate_limited')
            with db.get_conn() as c:
                row=c.execute('select state,available_at,attempts from queue_changes where file_id=?',(fid,)).fetchone()
                self.assertEqual(row['state'],'pending')
                self.assertEqual(row['attempts'],0)
                self.assertGreaterEqual(row['available_at'],started+123)
                c.execute('update queue_changes set available_at=0 where file_id=?',(fid,))
        self.assertEqual(self.hash.call_count,1)
        self.assertFalse(self.pipeline.lock.locked())

    async def test_deferred_transport_does_not_sleep_or_resend(self):
        endpoint='https://cooldown-test.invalid/graphql'
        request=AsyncMock()
        try:
            with cooldown.defer_waits():
                try:cooldown.rate_limited(endpoint,'120')
                except cooldown.RateLimited:pass
                with self.assertRaises(cooldown.RateLimited):
                    await cooldown.send_async(endpoint,request)
                self.assertGreater(cooldown.deferred_retry(), 110)
            request.assert_not_called()
        finally:
            with cooldown._lock:cooldown._until.pop('cooldown-test.invalid',None)
