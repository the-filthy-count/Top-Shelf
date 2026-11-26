import asyncio
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
import database as db
from app.core.pipeline import ProcessingPipeline
from app.core import queue_changes, queue_media


class UnifiedPipelineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        async def inline(fn, *args, **kwargs): return fn(*args, **kwargs)
        self.enterContext(patch.object(asyncio, "to_thread", inline))
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        db.close_thread_connection()
        self.enterContext(patch.object(db, 'DB_PATH', self.root/'state.db'))
        self.enterContext(patch.object(db, '_settings_cache', None))
        self.addCleanup(db.close_thread_connection)
        db.init_db()
        for name in ('scenes','movies','jav','vice'):(self.root/name).mkdir()
        db.save_settings({'source_dir':str(self.root/'scenes'), 'movies_source_dir':str(self.root/'movies'),
            'jav_download_watch_dir':str(self.root/'jav'), 'vice_download_watch_dir':str(self.root/'vice')})
        self.enterContext(patch('app.core.pipeline.require_complete_queue_video'))
        self.enterContext(patch('app.core.pipeline.get_video_duration', return_value=100))
        self.hash = self.enterContext(patch('app.core.pipeline.compute_phash', return_value='1234567890abcdef'))
        self.query = self.enterContext(patch('app.core.video_matching.engine.fingerprints', new_callable=AsyncMock, return_value=([],'')))
        self.enterContext(patch.object(db,'queue_search_record',return_value={}))
        self.events = []
        def movie(name, **kw):
            self.events.append(('movie',name,kw.get('dest_kind')))
            key = 'jav://'+name if kw.get('dest_kind') == 'jav' else name
            db.update_movie(key,'unmatched')
            return {'status':'unmatched'}
        def suggestions(name, **kw):
            self.events.append(('suggestions',name));return 2
        self.state={}
        self.pipeline = ProcessingPipeline(self.state, Mock(), Mock(), threading.Lock(), {
            'queue_resolve_phash':lambda *a:None,
            'queue_suggestions':queue_changes.schedule_suggestions,
            'search_suggestions':suggestions,'prewarm_queue_thumbs':Mock(),
            'process_movie':movie, 'file_scene_from_match':Mock(return_value={'status':'no_dir'}),
            'fetch_grab_scene':Mock(return_value=None)})

    def add(self, name, table='processed_files', root='scenes'):
        path=self.root/root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'video')
        key='jav://'+name if root=='jav' else 'Vices/'+name if root=='vice' else name
        (db.upsert_movie if table=='processed_movies' else db.upsert_file)(key)
        db.processing_job_enqueue(key,table)
        return path,key,db.file_identity(key,table)['id']

    async def test_one_list_drains_all_kinds_then_suggestions(self):
        self.add('s.mp4');self.add('m.mp4','processed_movies','movies');self.add('j.mp4','processed_movies','jav');self.add('Category/v.mp4',root='vice')
        results=await self.pipeline.run_pipeline([])
        self.assertEqual(len(results),4)
        self.assertEqual(db.processing_work_ready(),[])
        self.assertEqual(self.hash.call_count,2)
        self.assertEqual([e[0] for e in self.events],['movie','movie','suggestions','suggestions'])
        self.assertIn(('movie','j.mp4','jav'),self.events)
        self.assertFalse(self.state['running']);self.assertFalse(self.pipeline.lock.locked())

    async def test_rename_and_filing_retry_reuse_analysis_and_saved_match(self):
        video,key,fid=self.add('s.mp4')
        queue_media.read(key,video,duration_probe=lambda p:100,phash_probe=lambda p:'hash')
        scene={'id':'verified','performers':[]}
        queue_media.save_match(key,video,{'scene':scene,'source':'StashDB'}, expected_signature=db.media_file_signature(video))
        renamed=video.with_name('renamed.mp4');video.rename(renamed);db.rename_file(key,renamed.name)
        await self.pipeline.run_pipeline([])
        self.query.assert_not_awaited();self.hash.assert_not_called()
        self.pipeline.callbacks['file_scene_from_match'].assert_called_once_with(renamed,scene,'StashDB')
        self.assertEqual(db.file_identity(renamed.name)['id'],fid)
        db.processing_job_enqueue(renamed.name)
        await self.pipeline.run_pipeline([])
        self.assertEqual(self.pipeline.callbacks['file_scene_from_match'].call_count,2)
        self.hash.assert_not_called()

    async def test_failure_deferred_and_new_arrival_do_not_stop_list(self):
        self.add('a.mp4');self.add('b.mp4');self.add('c.mp4')
        seen=[]
        from download_safety import PartialDownloadPending
        async def process(video):
            seen.append(video.name)
            if video.name=='a.mp4':raise OSError('offline')
            if video.name=='b.mp4':raise PartialDownloadPending('still writing')
            if video.name=='c.mp4':self.add('d.mp4')
            return {'status':'unmatched'}
        self.pipeline.process_single=process
        await self.pipeline.run_pipeline([])
        self.assertEqual(seen,['a.mp4','b.mp4','c.mp4','d.mp4'])
        self.assertEqual(db.processing_work_ready(),[])
        with db.get_conn() as c:
            pending=c.execute("SELECT attempts,available_at FROM queue_changes WHERE state='pending' ORDER BY attempts").fetchall()
        self.assertEqual([r[0] for r in pending],[0,1])
        self.assertTrue(all(r[1]>0 for r in pending))

    async def test_manual_work_not_overwritten_and_restart_preserves_suggestion_step(self):
        _,key,fid=self.add('manual.mp4')
        with db.get_conn() as c:queue_changes.request(c,fid,'file_scene',{'scene':{'id':'manual'}})
        self.assertFalse(queue_changes.schedule_suggestions(key))
        self.assertEqual(db.processing_work_ready(),[])
        _,key,fid=self.add('suggest.mp4')
        with db.get_conn() as c:
            queue_changes.request(c,fid,'process',{'operation':'suggestions','include_iafd':False})
        db.processing_job_start(key)
        with db.get_conn() as c:queue_changes.recover(c)
        queue_changes.release_processing()
        await self.pipeline.run_pipeline([])
        self.hash.assert_not_called();self.query.assert_not_awaited()
        self.assertEqual(self.events,[('suggestions','suggest.mp4')])

    async def test_waits_for_shared_lock_and_registers_before_waiting(self):
        self.add('s.mp4');self.pipeline.lock.acquire()
        task=asyncio.create_task(self.pipeline.run_pipeline([]))
        await asyncio.sleep(.02);self.assertFalse(task.done())
        self.pipeline.lock.release()
        await asyncio.wait_for(task,3)
        self.assertEqual(db.processing_work_ready(),[])

    async def test_stale_claim_cannot_replace_new_manual_request(self):
        video,key,fid=self.add('s.mp4')
        ready=db.processing_work_ready()[0]
        queue_media.read(key,video)
        queue_media.save_match(key,video,{'scene':{'id':'old'},'source':'StashDB'}, expected_signature=db.media_file_signature(video))
        with db.get_conn() as c:queue_changes.request(c,fid,'file_scene',{'scene':{'id':'new'}})
        self.assertEqual(await self.pipeline.run_job(ready),{'status':'busy'})
        self.hash.assert_not_called()
        with db.get_conn() as c:
            self.assertIsNone(c.execute('SELECT pipeline_match_json FROM processed_files').fetchone()[0])

    async def test_delayed_suggestion_retry_retains_stage_and_options(self):
        _,key,fid=self.add('s.mp4')
        with db.get_conn() as c:queue_changes.request(c,fid,'process',{'operation':'suggestions','include_iafd':True})
        self.pipeline.callbacks['search_suggestions']=Mock(side_effect=OSError('offline'))
        await self.pipeline.run_pipeline([])
        with db.get_conn() as c:
            row=c.execute('SELECT state,desired_json,attempts,available_at FROM queue_changes').fetchone()
        self.assertEqual(row[0],'pending')
        self.assertEqual(json.loads(row[1]),{'operation':'suggestions','include_iafd':True})
        self.assertEqual(row[2],1);self.assertGreater(row[3],0)
        self.hash.assert_not_called()

    async def test_new_inbox_file_is_not_skipped_due_to_old_filed_status(self):
        video,key,fid=self.add('s.mp4')
        db.update_file(key,'filed')
        await self.pipeline.run_pipeline([])
        self.hash.assert_called_once()
        self.assertEqual(db.processing_work_result(fid)['status'],'unmatched')

    async def test_missing_previously_filed_video_does_not_turn_into_error(self):
        video,key,fid=self.add('s.mp4');video.unlink();db.update_file(key,'filed')
        await self.pipeline.run_pipeline([])
        self.hash.assert_not_called()
        self.assertEqual(db.processing_work_result(fid)['status'],'filed')

    async def test_lookup_miss_cannot_overwrite_newer_manual_match(self):
        _,key,fid=self.add('s.mp4')
        async def search(*args,**kwargs):
            with db.get_conn() as c:queue_changes.request(c,fid,'file_scene',{'scene':{'id':'manual'}})
            return [],''
        self.query.side_effect=search
        await self.pipeline.run_pipeline([])
        with db.get_conn() as c:
            row=c.execute('SELECT action,state,desired_json FROM queue_changes').fetchone()
        self.assertEqual((row[0],row[1]),('file_scene','pending'))
        self.assertEqual(json.loads(row[2])['scene']['id'],'manual')
        self.assertEqual(self.events,[])

    async def test_lookup_hit_cannot_override_newer_manual_choice(self):
        _,key,fid=self.add('s.mp4')
        scene={'id':'old-match','performers':[]}
        async def lookup(*a,**kw):
            with db.get_conn() as c:queue_changes.request(c,fid,'file_scene',{'scene':{'id':'manual'}})
            return [scene],'StashDB'
        self.query.side_effect=lookup
        with patch('app.core.fingerprint_selection.select_scene',return_value=scene):
            result=await self.pipeline.run_pipeline([])
        self.assertEqual(result[fid]['status'],'superseded')
        self.pipeline.callbacks['file_scene_from_match'].assert_not_called()
        with db.get_conn() as c:
            row=c.execute('SELECT action,state,desired_json FROM queue_changes').fetchone()
            self.assertIsNone(c.execute('SELECT pipeline_match_json FROM processed_files').fetchone()[0])
        self.assertEqual((row[0],row[1]),('file_scene','pending'))
        self.assertEqual(json.loads(row[2])['scene']['id'],'manual')

    async def test_filing_boundary_checks_revision_and_fences_started_effect(self):
        _,key,fid=self.add('s.mp4');db.processing_job_start(key)
        try:
            queue_changes.begin_effects()
            with db.get_conn() as c:
                with self.assertRaisesRegex(ValueError,'already started'):
                    queue_changes.request(c,fid,'file_scene',{'scene':{'id':'new'}})
            db.processing_job_finish(fid,'error','retryable')
            with db.get_conn() as c:
                row=c.execute('SELECT desired_json,state FROM queue_changes').fetchone()
            self.assertEqual(row[1],'pending')
            self.assertNotIn('_effects_started',json.loads(row[0]))
        finally:queue_changes.release_processing()

    async def test_progress_counts_entire_queue_beyond_read_window(self):
        for i in range(105):self.add(f'{i:03}.mp4')
        ready=db.processing_work_ready()
        self.assertEqual(len(ready),100);self.assertEqual(ready[0]['ready_count'],105)
        totals=[]
        async def process(video):
            totals.append(self.state['pipeline_total'])
            return {'status':'unmatched'}
        self.pipeline.process_single=process
        await self.pipeline.run_pipeline([])
        self.assertEqual(totals,[105]*105)

    async def test_restart_does_not_replay_effect_fence_before_annotation(self):
        _,key,fid=self.add('s.mp4');db.processing_job_start(key)
        try:
            queue_changes.begin_effects()
            with db.get_conn() as c:
                queue_changes.recover(c)
                row=c.execute('SELECT state,running_revision FROM queue_changes WHERE file_id=?',(fid,)).fetchone()
            self.assertEqual(tuple(row),('blocked',None))
            self.assertEqual(db.processing_work_ready(),[])
        finally:queue_changes.release_processing()
