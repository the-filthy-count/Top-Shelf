import unittest
import database as db
from app.core import queue_changes as changes
import test_suggestion_rule_versions as fixtures

class QueueChangesTests(unittest.TestCase):
    setUp = fixtures.SuggestionRuleVersionTests.setUp

    def asset(self):
        db.upsert_file('test.mp4')
        return db.file_identity('test.mp4')['id']

    def test_newer_edit_survives_old_worker_completion(self):
        fid=self.asset()
        with db.get_conn() as c:
            first=changes.request(c,fid,'file_scene',{'title':'old'})
            self.assertTrue(changes.claim(c,fid,first))
            second=changes.request(c,fid,'file_scene',{'title':'new'})
            self.assertFalse(changes.claim(c,fid,second))
            changes.finish(c,fid,first,'completed',{'destination':'old.mp4'})
            row=c.execute('SELECT * FROM queue_changes WHERE file_id=?',(fid,)).fetchone()
            self.assertEqual(row['state'],'pending');self.assertEqual(row['revision'],second)
            self.assertTrue(changes.claim(c,fid,second))

    def test_stale_edits_and_duplicate_claims_rejected(self):
        fid=self.asset()
        with db.get_conn() as c:
            rev=changes.request(c,fid,'recover',{})
            with self.assertRaises(ValueError):changes.request(c,fid,'recover',{},expected_revision=0)
            self.assertTrue(changes.claim(c,fid,rev));self.assertFalse(changes.claim(c,fid,rev))

    def test_failed_effect_remains_visible(self):
        fid=self.asset()
        def fail():raise OSError('Offline destination')
        from unittest.mock import patch
        with patch.dict(changes._handlers,{'test_failure':lambda fid,payload:fail()}):
            with self.assertRaises(OSError):changes.execute(fid,'test_failure',{},fail)
        row=changes.list_changes()[0]
        self.assertEqual(row['state'],'failed');self.assertEqual(row['error'],'Offline destination')

    def test_manual_jobs_supersede_pending_and_keep_completion(self):
        fid=self.asset()
        one=db.manual_job_create('test','test','',{'file_id':fid})
        two=db.manual_job_create('test','test','',{'file_id':fid})
        self.assertFalse(db.manual_job_mark_started(one))
        self.assertTrue(db.manual_job_mark_started(two))
        db.manual_job_delete(two)
        self.assertEqual(changes.list_changes()[0]['state'],'completed')
        with db.get_conn() as c:
            self.assertEqual(c.execute('SELECT status FROM manual_queue_jobs WHERE id=?',(two,)).fetchone()[0],'completed')

    def test_restart_blocks_interrupted_effect(self):
        fid=self.asset()
        with db.get_conn() as c:
            rev=changes.request(c,fid,'recover',{})
            changes.claim(c,fid,rev);changes.recover(c)
            self.assertFalse(changes.claim(c,fid,rev))
        self.assertEqual(changes.list_changes()[0]['state'],'blocked')

    def test_automatic_processing_and_nested_filing_share_record(self):
        fid=self.asset();db.processing_job_enqueue('test.mp4')
        self.assertEqual(db.processing_job_start('test.mp4'),fid)
        result=changes.execute(fid,'file_scene',{'scene':{'id':'123'}},lambda:{'status':'filed'})
        db.processing_job_finish(fid,result['status'])
        row=changes.list_changes()[0]
        self.assertEqual(row['revision'],1);self.assertEqual(row['desired']['scene']['id'],'123')
        self.assertEqual(row['state'],'completed')

    def test_async_worker_keeps_ownership_across_thread_hops(self):
        import asyncio
        fid=self.asset()
        async def run():
            await asyncio.to_thread(db.processing_job_start,'test.mp4')
            changes.adopt_processing(fid)
            result=await asyncio.to_thread(changes.execute,fid,'file_scene',{'scene':{'id':'threaded'}},lambda:{'status':'filed'})
            await asyncio.to_thread(db.processing_job_finish,fid,result['status'])
            changes.release_processing()
        asyncio.run(run())
        row=changes.list_changes()[0]
        self.assertEqual(row['state'],'completed')
        self.assertEqual(row['revision'],1)
        self.assertEqual(row['desired']['scene']['id'],'threaded')

    def test_relocation_failure_rolls_back_media_and_sidecars(self):
        from pathlib import Path
        from unittest.mock import patch
        root=Path(self.tmp.name)/'inbox';root.mkdir()
        video=root/'test.mp4';video.write_bytes(b'content')
        sidecar=root/'test.nfo';sidecar.write_text('metadata')
        target=root/'Actor'/'test.mp4'
        with patch.object(db,'rename_file',return_value=False):
            with self.assertRaises(ValueError):
                changes.relocate('test.mp4','Actor/test.mp4',[(video,target),(sidecar,target.with_suffix('.nfo'))])
        self.assertEqual(video.read_bytes(),b'content');self.assertTrue(sidecar.exists())
        self.assertFalse(target.exists());self.assertEqual(changes.list_changes()[0]['state'],'failed')

    def test_cancel_uses_revision_and_stops_pending_worker(self):
        import ast, asyncio
        from pathlib import Path
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        tree=ast.parse(Path('main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='queue_change_cancel')
        env={'app':FastAPI(),'db':db,'JSONResponse':JSONResponse,'json':__import__('json')}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'queue-api','exec'),env)
        fid=self.asset();job=db.manual_job_create('test','test','',{'file_id':fid})
        stale=asyncio.run(env['queue_change_cancel'](fid,{'revision':0}))
        self.assertEqual(stale.status_code,409)
        result=asyncio.run(env['queue_change_cancel'](fid,{'revision':1}))
        self.assertTrue(result['ok']);self.assertFalse(db.manual_job_mark_started(job))
        self.assertEqual(changes.list_changes()[0]['state'],'cancelled')

    def test_legacy_pending_job_migrates_to_stable_desired_state(self):
        import json
        fid=self.asset()
        with db.get_conn() as c:
            c.execute("INSERT INTO manual_queue_jobs(job_type,label,payload_json,status) VALUES('manual_scene','test',?,'pending')",(json.dumps({'filename':'test.mp4','scene':{'id':'123'}}),))
            changes.recover(c)
            row=c.execute('SELECT payload_json FROM manual_queue_jobs').fetchone()
            payload=json.loads(row[0])
            self.assertEqual(payload['file_id'],fid);self.assertEqual(payload['_change_revision'],1)
            self.assertTrue(changes.claim(c,fid,1))

    def test_unapproved_files_and_movies_share_list_and_hidden_files_stay_out(self):
        self.asset();db.upsert_movie('movie.mp4');db.upsert_file('.hidden.mp4')
        rows=changes.list_changes()
        self.assertEqual({r['filename'] for r in rows},{'test.mp4','movie.mp4'})
        self.assertTrue(all(r['state']=='draft' for r in rows))

    def test_completed_file_cannot_replay_against_reused_path(self):
        fid=self.asset()
        db.update_file('test.mp4',status='filed',destination='/library/test.mp4')
        with self.assertRaisesRegex(ValueError,'already filed'):
            changes.assert_actionable(fid)

    def test_manual_payload_is_loaded_from_canonical_record(self):
        import json
        fid=self.asset()
        job=db.manual_job_create('manual_scene','test','',{'file_id':fid,'scene':{'id':'right'}})
        with db.get_conn() as conn:
            payload=json.loads(conn.execute('SELECT payload_json FROM manual_queue_jobs WHERE id=?',(job,)).fetchone()[0])
            payload['scene']={'id':'wrong'}
            conn.execute("UPDATE manual_queue_jobs SET status='completed',payload_json=? WHERE id=?",(json.dumps(payload),job))
        self.assertEqual([r['id'] for r in db.manual_job_list_active()],[job])
        command=db.manual_job_claim(job)
        self.assertEqual(command['payload']['scene']['id'],'right')
        self.assertIsNone(db.manual_job_claim(job))

    def test_legacy_projection_cannot_override_cancellation_or_retry_delay(self):
        fid=self.asset()
        db.processing_job_start('test.mp4')
        db.processing_job_finish(fid,'error','offline')
        with db.get_conn() as conn:
            conn.execute("UPDATE processing_jobs SET state='pending',available_at=0")
        self.assertEqual(db.processing_jobs_ready(),[])
        self.assertIsNone(db.processing_job_start('test.mp4'))
        with db.get_conn() as conn:
            conn.execute("UPDATE queue_changes SET available_at=0")
            conn.execute("UPDATE processing_jobs SET state='failed'")
        self.assertEqual(db.processing_jobs_ready(),['test.mp4'])
        self.assertEqual(db.processing_job_start('test.mp4'),fid)
        changes.release_processing()

    def test_dispatch_uses_saved_input_and_never_the_callers_callback(self):
        from unittest.mock import patch
        fid=self.asset()
        seen=[]
        with patch.dict(changes._handlers,{'test_dispatch':lambda file_id,payload:seen.append(payload) or {'status':'completed'}}):
            result=changes.execute(fid,'test_dispatch',{'title':'saved'},lambda:self.fail('Caller callback executed'))
        self.assertEqual(seen,[{'title':'saved'}])
        self.assertEqual(result['status'],'completed')
        self.assertEqual(changes.list_changes()[0]['state'],'completed')

    def test_dispatch_survives_loss_of_original_caller(self):
        from unittest.mock import patch
        fid=self.asset()
        with db.get_conn() as conn:
            revision=changes.request(conn,fid,'test_dispatch',{'title':'saved'})
        with patch.dict(changes._handlers,{'test_dispatch':lambda file_id,payload: {'title':payload['title']}}):
            self.assertEqual(changes.dispatch(fid,revision),{'title':'saved'})
            with self.assertRaisesRegex(ValueError,'no longer ready'):
                changes.dispatch(fid,revision)

    def test_pending_command_rebuilds_missing_worker_projection(self):
        fid=self.asset()
        with db.get_conn() as conn:
            revision=changes.request(conn,fid,'file_scene',{'scene':{'id':'saved'}})
        rows=db.manual_job_list_active()
        self.assertEqual(len(rows),1)
        command=db.manual_job_claim(rows[0]['id'])
        self.assertEqual(command['payload']['_change_revision'],revision)
        self.assertEqual(command['payload']['scene']['id'],'saved')
        self.assertEqual(db.manual_job_list_active(),[])
