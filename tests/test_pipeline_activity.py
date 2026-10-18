import ast
import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

class PipelineActivityTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_runs_off_loop_and_progress_clears(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app/core/pipeline.py').read_text())
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef))
        method=next(n for n in cls.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='run_pipeline')
        with tempfile.TemporaryDirectory() as tmp:
            video=Path(tmp)/'clip.mp4';video.touch()
            state={};loop_thread=threading.get_ident();seen=[]
            def resolve(*args,**kwargs):
                seen.append((threading.get_ident(),state['pipeline_phase']))
                return video
            db=SimpleNamespace(get_settings=lambda:{'source_dir':tmp},resolve_source_video_path=resolve,notification_add=Mock())
            env=dict(asyncio=asyncio,Path=Path,db=db,VIDEO_EXTENSIONS={'.mp4'})
            exec(compile(ast.Module(body=[method],type_ignores=[]),'pipeline.py','exec'),env)
            async def process(path):
                self.assertEqual(state['pipeline_total'],1)
                self.assertEqual(state['current_file'],'clip.mp4')
                return {'status':'unmatched'}
            owner=SimpleNamespace(state=state,lock=threading.Lock(),emit=Mock(),flush_emit_buffer=Mock(),process_single=process)
            await env['run_pipeline'](owner,['clip.mp4'])
            self.assertNotEqual(seen[0][0],loop_thread)
            self.assertEqual(seen[0][1],'Scanning files')
            self.assertFalse(state['running']);self.assertIsNone(state['pipeline_phase'])
            self.assertFalse(owner.lock.locked())
