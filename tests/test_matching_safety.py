import ast
import asyncio
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from download_safety import require_complete_queue_video, PartialDownloadPending
ROOT=Path(__file__).resolve().parents[1]

class MatchingSafetyTests(unittest.TestCase):
    def test_partial_album_and_shared_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); settings={'source_dir':tmp}
            loose=root/'loose.mp4';loose.touch();(root/'other.part').touch()
            require_complete_queue_video(loose,settings)
            (root/'loose.mp4.part').touch()
            with self.assertRaises(PartialDownloadPending):require_complete_queue_video(loose,settings)
            album=root/'album';album.mkdir();video=album/'clip.mp4';video.touch();partial=album/'active.part';partial.touch()
            with self.assertRaises(PartialDownloadPending):require_complete_queue_video(video,settings)
            self.assertTrue(partial.exists());self.assertTrue(video.exists())

    def test_cache_invalidates_replaced_file(self):
        tree=ast.parse((ROOT/'database.py').read_text())
        conn=sqlite3.connect(':memory:');self.addCleanup(conn.close)
        conn.execute('CREATE TABLE processed_files(filename TEXT, phash TEXT, media_duration INTEGER, media_signature TEXT)')
        conn.execute("INSERT INTO processed_files VALUES ('clip.mp4','stale',60,NULL)")
        env={'Path':Path,'json':json,'get_conn':lambda:conn}
        for name in ('media_file_signature','validate_media_cache'):
            fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
            exec(compile(ast.Module(body=[fn],type_ignores=[]),'db','exec'),env)
        with tempfile.TemporaryDirectory() as tmp:
            video=Path(tmp)/'clip.mp4';video.write_bytes(b'old')
            validate=env['validate_media_cache'];validate('clip.mp4',video)
            self.assertEqual(conn.execute('SELECT phash,media_duration FROM processed_files').fetchone(),(None,None))
            conn.execute("UPDATE processed_files SET phash='valid',media_duration=60")
            validate('clip.mp4',video)
            self.assertEqual(conn.execute('SELECT phash FROM processed_files').fetchone()[0],'valid')
            replacement=video.with_suffix('.new');replacement.write_bytes(b'new');replacement.replace(video)
            validate('clip.mp4',video)
            self.assertEqual(conn.execute('SELECT phash,media_duration FROM processed_files').fetchone(),(None,None))

    def test_batch_continues_after_failure_and_deferral(self):
        tree=ast.parse((ROOT/'app/core/pipeline.py').read_text())
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef))
        fn=next(n for n in cls.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='run_pipeline')
        async def inline(fn,*args):return fn(*args)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name in ('a.mp4','b.mp4','c.mp4'):(root/name).touch()
            seen=[]
            async def process(p):
                seen.append(p.name)
                if p.name=='a.mp4':raise OSError('failed')
                if p.name=='b.mp4':raise PartialDownloadPending('waiting')
                return {'status':'filed'}
            notify=Mock()
            env={'Path':Path,'asyncio':SimpleNamespace(to_thread=inline),'PartialDownloadPending':PartialDownloadPending,
                 'db':SimpleNamespace(get_settings=lambda:{'source_dir':tmp},notification_add=notify),'VIDEO_EXTENSIONS':{'.mp4'}}
            exec(compile(ast.Module(body=[fn],type_ignores=[]),'pipeline','exec'),env)
            owner=SimpleNamespace(state={},lock=threading.Lock(),emit=Mock(),flush_emit_buffer=Mock(),process_single=process)
            asyncio.run(env['run_pipeline'](owner))
            self.assertEqual(seen,['a.mp4','b.mp4','c.mp4'])
            self.assertFalse(owner.state['running']);self.assertFalse(owner.lock.locked())
            self.assertIn('1 waiting for download',notify.call_args.args[1])
