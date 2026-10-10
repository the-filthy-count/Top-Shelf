"""Filmstrip cache regressions without ffmpeg, app startup or user files."""
import ast
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import shutil
import tempfile
import threading
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'main.py'

def load(root, generate):
    tree = ast.parse(SOURCE.read_text())
    names = {'_queue_thumbs_dir', '_generate_queue_thumb', '_carry_queue_thumbs'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    ns = dict(Path=Path, threading=threading, hashlib=hashlib, shutil=shutil,
              QUEUE_THUMB_COUNT=5, _queue_thumb_frame_locks=[threading.Lock() for _ in range(128)],
              _metadata_base_dir=lambda:root, _generate_queue_thumb_unlocked=generate)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), ns)
    return ns

class QueueFilmstripTests(unittest.TestCase):
    def test_parallel_generation_publishes_once_and_atomically(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);dest=root/'0.jpg';entered=threading.Event();release=threading.Event();calls=[]
            def generate(video, temporary, timestamp, **kwargs):
                calls.append(temporary)
                temporary.write_bytes(b'partial');entered.set();release.wait(3)
                temporary.write_bytes(b'complete');return True
            ns=load(root, generate)
            with ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(ns['_generate_queue_thumb'], root/'source.mp4', dest, 10)
                self.assertTrue(entered.wait(3));self.assertFalse(dest.exists())
                second=pool.submit(ns['_generate_queue_thumb'], root/'source.mp4', dest, 10)
                release.set();self.assertTrue(first.result());self.assertTrue(second.result())
            self.assertEqual(len(calls),1);self.assertEqual(dest.read_bytes(),b'complete')
            self.assertEqual(list(root.glob('.*.jpg')),[])

    def test_failed_frame_is_not_published(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            def generate(video, temporary, timestamp, **kwargs):
                temporary.write_bytes(b'partial');return False
            ns=load(root,generate)
            self.assertFalse(ns['_generate_queue_thumb'](root/'source.mp4',root/'0.jpg',10))
            self.assertEqual(list(root.iterdir()),[])

    def test_grouping_carries_only_complete_frames_without_overwriting(self):
        with tempfile.TemporaryDirectory() as d:
            ns=load(Path(d),None)
            old=ns['_queue_thumbs_dir']('source.mp4',ensure=True)
            (old/'0.jpg').write_bytes(b'cached');(old/'1.jpg').write_bytes(b'');(old/'.2-tmp.jpg').write_bytes(b'partial')
            ns['_carry_queue_thumbs']('source.mp4','Performer/source.mp4')
            new=ns['_queue_thumbs_dir']('Performer/source.mp4')
            self.assertEqual((new/'0.jpg').read_bytes(),b'cached');self.assertFalse((new/'1.jpg').exists())
            (new/'0.jpg').write_bytes(b'newer')
            ns['_carry_queue_thumbs']('source.mp4','Performer/source.mp4')
            self.assertEqual((new/'0.jpg').read_bytes(),b'newer')
            self.assertTrue((old/'0.jpg').exists())

if __name__ == '__main__': unittest.main()
