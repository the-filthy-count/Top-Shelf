"""Behavioral regression tests for the replacement engine; no live services."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
import database as db
import test_unified_matching_pipeline as fixtures
from app.core.video_matching.engine import MatchingEngine
from app.core.video_matching.journal import Journal
from app.core.video_matching.providers import fingerprints
from app.core.video_matching.decisions import movie, score_suggestion
from download_safety import PartialDownloadPending


class RebuiltMatchingTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.UnifiedPipelineTests.setUp
    add = fixtures.UnifiedPipelineTests.add

    def engine(self, **callbacks):
        return MatchingEngine(db,callbacks,complete=lambda *a:None,
            duration=lambda p:100,phash=lambda *a,**kw:'1234567890abcdef')

    async def test_provider_misses_reused_and_only_failed_provider_retried(self):
        _,key,fid = self.add('cache.mp4')
        journal = Journal(db,fid)
        query = AsyncMock(side_effect=[[],TimeoutError('offline'),[]])
        client = AsyncMock()
        with patch('app.integrations.stashbox.get_async_client',return_value=client),patch('app.integrations.stashbox.query_stashbox',query):
            with self.assertRaisesRegex(RuntimeError,'incomplete'):
                await fingerprints('1234567890abcdef',{'stashdb':'one','tpdb':'two'},journal=journal)
            self.assertEqual(query.await_count,2)
            self.assertEqual(await fingerprints('1234567890abcdef',{'stashdb':'one','tpdb':'two'},journal=journal),([], 'none'))
            self.assertEqual(query.await_count,3)
            self.assertEqual(query.await_args.args[2],'https://theporndb.net/graphql')
        with db.get_conn() as c:
            text = '\n'.join(str(tuple(row)) for row in c.execute('SELECT * FROM video_match_steps'))
        self.assertNotIn('"one"',text);self.assertNotIn('"two"',text)

    async def test_renaming_keeps_provider_cache_but_new_hash_invalidates_it(self):
        path,key,fid = self.add('old.mp4')
        journal = Journal(db,fid)
        query = AsyncMock(return_value=[])
        with patch('app.integrations.stashbox.get_async_client',return_value=AsyncMock()),patch('app.integrations.stashbox.query_stashbox',query):
            await fingerprints('1234567890abcdef',{'stashdb':'one'},journal=journal)
            path.rename(path.with_name('new.mp4'));db.rename_file(key,'new.mp4')
            await fingerprints('1234567890abcdef',{'stashdb':'one'},journal=Journal(db,db.file_identity('new.mp4')['id']))
            self.assertEqual(query.await_count,1)
            await fingerprints('0000000000000000',{'stashdb':'one'},journal=journal)
            self.assertEqual(query.await_count,2)

    async def test_expired_miss_is_searched_again(self):
        _,_,fid=self.add('expiry.mp4')
        journal=Journal(db,fid)
        journal.write('test',{'q':'same'},result=[],ttl=-1)
        self.assertIsNone(journal.cached('test',{'q':'same'}))

    async def test_new_credentials_invalidate_only_their_provider_cache(self):
        _,_,fid=self.add('keys.mp4');journal=Journal(db,fid)
        query=AsyncMock(return_value=[])
        with patch('app.integrations.stashbox.get_async_client',return_value=AsyncMock()),patch('app.integrations.stashbox.query_stashbox',query):
            await fingerprints('hash',{'stashdb':'one','fansdb':'two'},journal=journal)
            await fingerprints('hash',{'stashdb':'new','fansdb':'two'},journal=journal)
        self.assertEqual(query.await_count,3)

    async def test_grab_identity_files_without_provider_query(self):
        video,key,fid=self.add('grab.mp4')
        scene={'id':'correct','performers':[{'performer':{'id':'person','name':'Name'}}]}
        self.pipeline.callbacks['fetch_grab_scene'].return_value=scene
        self.pipeline.callbacks['file_scene_from_match'].return_value={'status':'filed'}
        with patch.object(db,'lookup_scene_grab_for_filename',return_value={'id':1,'source_id':'correct','source_db':'StashDB'}),patch.object(db,'mark_scene_grab_consumed') as consumed:
            result=await self.pipeline.run_pipeline([])
        self.assertEqual(result[fid]['status'],'filed')
        self.query.assert_not_awaited();consumed.assert_called_once_with(1)
        self.pipeline.callbacks['file_scene_from_match'].assert_called_once_with(video,scene,'StashDB')

    async def test_failed_grab_uses_fingerprint_but_filing_failure_does_not(self):
        video,key,fid=self.add('fallback.mp4')
        scene={'id':'fingerprint'}
        self.query.return_value=([scene],'FansDB')
        self.pipeline.callbacks['fetch_grab_scene'].side_effect=TimeoutError('offline')
        self.pipeline.callbacks['file_scene_from_match'].side_effect=OSError('move failed')
        with patch.object(db,'lookup_scene_grab_for_filename',return_value={'id':1,'source_id':'grab','source_db':'StashDB'}):
            result=await self.pipeline.run_pipeline([])
        self.assertEqual(result[fid]['status'],'error');self.query.assert_awaited_once()
        with db.get_conn() as c:
            saved=json.loads(c.execute('SELECT pipeline_match_json FROM processed_files').fetchone()[0])
        self.assertEqual(saved['scene']['id'],'fingerprint')

    async def test_movie_failure_retries_then_saved_match_skips_provider(self):
        video,key,fid=self.add('film.mp4','processed_movies','movies')
        db.save_settings({'api_key_tmdb':'test'})
        search=Mock(side_effect=[TimeoutError('offline'),{'movie':{'id':'m','title':'Film'},'source':'tmdb'}])
        filing=Mock(return_value={'status':'no_dir'})
        engine=self.engine(movie_query=lambda n:('Film',None),movie_providers=[{'key':'tmdb','search':search}],
            queue_resolve_phash=lambda *a,**kw:None,file_movie=filing,cleanup=Mock())
        with self.assertRaisesRegex(RuntimeError,'incomplete'):
            engine.feature(key,video.parent)
        self.assertEqual(engine.feature(key,video.parent)['status'],'no_dir')
        self.assertEqual(engine.feature(key,video.parent)['status'],'no_dir')
        self.assertEqual(search.call_count,2);self.assertEqual(filing.call_count,2)

    async def test_movie_mutation_during_lookup_cannot_file(self):
        video,key,fid=self.add('change.mp4','processed_movies','movies')
        db.save_settings({'api_key_tmdb':'test'})
        def search(*a):
            video.write_bytes(b'replacement contents')
            return {'movie':{'id':'m'},'source':'tmdb'}
        filing=Mock()
        engine=self.engine(movie_query=lambda n:('Film',None),movie_providers=[{'key':'tmdb','search':search}],
            queue_resolve_phash=lambda *a,**kw:None,file_movie=filing,cleanup=Mock())
        with self.assertRaises(PartialDownloadPending):engine.feature(key,video.parent)
        filing.assert_not_called()

    async def test_movie_and_jav_share_names_without_sharing_identity(self):
        first,key,fid=self.add('same.mp4','processed_movies','movies')
        second,jkey,jfid=self.add('same.mp4','processed_movies','jav')
        compare=Mock(return_value={'status':'removed'})
        engine=self.engine(queue_resolve_phash=compare)
        engine.feature('same.mp4',first.parent);engine.feature('same.mp4',second.parent)
        self.assertEqual([call.args[1] for call in compare.call_args_list],[key,jkey])
        self.assertNotEqual(fid,jfid)

    async def test_no_provider_is_an_error_not_a_genuine_miss(self):
        video,key,fid=self.add('no-provider.mp4','processed_movies','movies')
        engine=self.engine(queue_resolve_phash=lambda *a,**kw:None,movie_query=lambda n:('Film',None),movie_providers=[])
        with self.assertRaisesRegex(RuntimeError,'No movie databases'):engine.feature(key,video.parent)


class DecisionTests(unittest.TestCase):
    def test_movie_name_is_not_unique_without_disambiguation(self):
        rows=[{'id':'one','title':'Film','year':'2001'},{'id':'two','title':'Film','year':'2002'}]
        self.assertEqual(movie(rows,'Film').state,'review')
        self.assertEqual(movie(rows,'Film','2002').candidate['id'],'two')

    def test_name_only_suggestion_cannot_become_automatic_match(self):
        cast=[{'performer':{'name':name}} for name in ('One Person','Two Person')]
        reasons=[]
        score=score_suggestion('Different title',{'id':'x','title':'Other title','performers':cast},
            query_performers=['One Person','Two Person'],allow_joint_cast=True,reasons=reasons)
        self.assertEqual(score,.5)
        self.assertIn('title unconfirmed',reasons[0]['label'])

class SuggestionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.UnifiedPipelineTests.setUp
    add = fixtures.UnifiedPipelineTests.add

    def prepare(self):
        from app.core import queue_records
        self.add('search.mp4')
        record=queue_records.read('search.mp4')
        fields=dict(record['fields'],title='Example Title',performers=['Example Person'])
        record=queue_records.save('search.mp4',record['file_id'],fields,record['revision'])
        self.enterContext(patch.object(db,'queue_search_record',side_effect=queue_records.read))
        self.enterContext(patch('app.core.matching._build_file_meta',return_value={}))
        row={'id':'one','_source':'StashDB','title':'Example Title','performers':[{'performer':{'id':'p','name':'Example Person'}}]}
        search=Mock(return_value=[row])
        callbacks={'sources':{'StashDB'},'search':search,'cast':Mock(return_value=([],True)),
            'allowed':lambda r:True,'normalize':lambda r,f:{'source_db':r['_source'],'source_id':r['id'],'title':r['title'],'payload':r},'prewarm':Mock()}
        return record,callbacks

    async def test_repeat_uses_saved_response_and_new_fields_trigger_search(self):
        from app.core.video_matching.suggestions import run
        from app.core import queue_records
        record,callbacks=self.prepare()
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),1)
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),1)
        self.assertEqual(callbacks['search'].call_count,1)
        queue_records.save('search.mp4',record['file_id'],dict(record['fields'],title='Another Title'),record['revision'])
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),0)
        self.assertEqual(callbacks['search'].call_count,2)

    async def test_deliberate_rescan_uses_a_new_generation_but_retries_reuse_it(self):
        from app.core.video_matching.suggestions import run
        record,callbacks=self.prepare()
        run(db,'search.mp4',callbacks,include_iafd=False)
        run(db,'search.mp4',callbacks,include_iafd=False,search_generation='manual-1')
        self.assertEqual(callbacks['search'].call_count,2)
        run(db,'search.mp4',callbacks,include_iafd=False,search_generation='manual-1')
        self.assertEqual(callbacks['search'].call_count,2)

    async def test_outage_preserves_candidates_and_marks_attempt_incomplete(self):
        from app.core.video_matching.suggestions import run
        record,callbacks=self.prepare()
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),1)
        with db.get_conn() as c:c.execute('DELETE FROM video_match_steps')
        callbacks['search'].side_effect=TimeoutError('offline')
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),0)
        with db.get_conn() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM queue_suggestions').fetchone()[0],1)
            self.assertEqual(c.execute("SELECT state FROM video_match_steps WHERE stage='suggestions'").fetchone()[0],'failed')

    async def test_user_edit_during_search_cannot_publish_old_candidates(self):
        from app.core.video_matching.suggestions import run
        from app.core import queue_records
        record,callbacks=self.prepare()
        rows=callbacks['search'].return_value
        def change(**kw):
            queue_records.save('search.mp4',record['file_id'],dict(record['fields'],title='Changed'),record['revision'])
            return rows
        callbacks['search'].side_effect=change
        self.assertEqual(run(db,'search.mp4',callbacks,include_iafd=False),0)
        with db.get_conn() as c:self.assertEqual(c.execute('SELECT count(*) FROM queue_suggestions').fetchone()[0],0)

class MigrationTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.UnifiedPipelineTests.setUp
    add = fixtures.UnifiedPipelineTests.add

    async def test_repeated_upgrade_preserves_existing_analysis_and_journal(self):
        from app.core.queue_media import read
        video,key,fid=self.add('migration.mp4')
        media=read(key,video,duration_probe=lambda p:123,phash_probe=lambda p:'fingerprint')
        journal=Journal(db,fid)
        journal.write('fingerprint:test',{'hash':'fingerprint'},result=[])
        db.init_db();db.init_db()
        measured=read(key,video,duration_probe=Mock(side_effect=AssertionError('must reuse')),
            phash_probe=Mock(side_effect=AssertionError('must reuse')))
        self.assertEqual(measured['phash'],media['phash'])
        self.assertEqual(measured['media_duration'],123)
        self.assertEqual(journal.cached('fingerprint:test',{'hash':'fingerprint'}),[])

    async def test_deleting_file_identity_removes_its_observations(self):
        _,key,fid=self.add('remove.mp4')
        Journal(db,fid).write('test',{},result=[])
        with db.get_conn() as conn:
            conn.execute('DELETE FROM file_assets WHERE id=?',(fid,))
            self.assertEqual(conn.execute('SELECT count(*) FROM video_match_steps').fetchone()[0],0)

class ProviderCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_superseded_work_cancels_outstanding_providers(self):
        from app.core.queue_changes import SupersededWork
        waiting=asyncio.Event(); cancelled=asyncio.Event()
        async def query(client,phash,endpoint,*args):
            if 'stashdb' in endpoint:
                await waiting.wait()
                raise SupersededWork('New manual choice')
            waiting.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        with patch('app.integrations.stashbox.get_async_client',return_value=AsyncMock()),patch('app.integrations.stashbox.query_stashbox',side_effect=query):
            with self.assertRaises(SupersededWork):
                await asyncio.wait_for(fingerprints('hash',{'stashdb':'a','tpdb':'b'}),1)
        self.assertTrue(cancelled.is_set())
