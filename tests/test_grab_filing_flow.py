import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, AsyncMock, patch
import sys

ROOT = Path(__file__).resolve().parents[1]

class GrabFlowTests(unittest.IsolatedAsyncioTestCase):
    async def check_flow(self, fetched, status="filed"):
        tree=ast.parse((ROOT/'app/core/pipeline.py').read_text())
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef))
        method=next(n for n in cls.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='process_single')
        scene={'id':'scene-id','performers':[{'performer':{'id':'correct-id','name':'Person'}}]}
        with tempfile.TemporaryDirectory() as tmp:
            video=Path(tmp)/'video.mp4';video.touch()
            db=SimpleNamespace(queue_file_key=lambda p,s:p.name,get_settings=lambda:{'source_dir':tmp},upsert_file=Mock(),
                validate_media_cache=lambda *a:'signature', media_file_signature=lambda *a:'signature', get_media_duration=lambda _:60, get_phash=lambda _:'cached',update_file=Mock(),
                lookup_scene_grab_by_ts_uid=lambda _:None,
                lookup_scene_grab_for_filename=lambda _:{'id':10,'kind':'scene','source_id':'scene-id','source_title':'Title','source_db':'StashDB'},
                mark_scene_grab_consumed=Mock())
            query=AsyncMock(return_value=([scene],'StashDB'))
            env={'require_complete_queue_video':lambda *a:None,'Path':Path,'asyncio':asyncio,'db':db,'_strip_ts_uid':lambda s:(s,''),'query_with_fallback':query}
            exec(compile(ast.Module(body=[method],type_ignores=[]),'pipeline.py','exec'),env)
            file_scene=Mock(return_value={'status':status})
            owner=SimpleNamespace(state={},emit=Mock(),callbacks={
                'queue_resolve_phash':lambda *args:None,
                'fetch_grab_scene':lambda grab:scene if fetched else None,
                'file_scene_from_match':file_scene})
            # Import only the pure normalization module; never start the app.
            async def direct_thread(fn,*args,**kwargs):return fn(*args,**kwargs)
            with patch.object(asyncio,'to_thread',direct_thread):
                result=await env['process_single'](owner,video)
            self.assertEqual(result,{'status':status})
            file_scene.assert_called_once_with(video,scene,'StashDB')
            if fetched:
                query.assert_not_awaited()
                if status=='filed':db.mark_scene_grab_consumed.assert_called_once_with(10)
                else:db.mark_scene_grab_consumed.assert_not_called()
            else:
                query.assert_awaited_once()
                db.mark_scene_grab_consumed.assert_not_called()

    async def test_grab_uses_authoritative_scene(self):
        await self.check_flow(True)

    async def test_failed_grab_fetch_uses_phash(self):
        await self.check_flow(False)

    async def test_no_dir_and_error_do_not_replace_confirmed_match(self):
        for status in ('no_dir','error','deferred'):
            await self.check_flow(True,status)
