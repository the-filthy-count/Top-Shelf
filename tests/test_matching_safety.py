import ast
import asyncio
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
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
        conn.execute('CREATE TABLE processed_files(filename TEXT, pipeline_match_json TEXT, phash TEXT, media_duration INTEGER, media_signature TEXT, media_size_bytes INTEGER, media_codec TEXT, media_width INTEGER, media_height INTEGER)')
        conn.execute("INSERT INTO processed_files(filename,phash,media_duration,media_signature) VALUES ('clip.mp4','stale',60,NULL)")
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
            # Real pre-sort: rename the video and rekey its existing DB record.
            import os
            grouped = video.parent / 'Performer' / 'renamed.mp4'
            grouped.parent.mkdir()
            video.rename(grouped)
            video = grouped
            conn.execute("UPDATE processed_files SET filename='Performer/renamed.mp4',media_codec='h264',media_width=1920,media_height=1080,media_size_bytes=3")
            validate('Performer/renamed.mp4',video)
            self.assertEqual(conn.execute('SELECT phash,media_duration,media_codec,media_width,media_height,media_size_bytes FROM processed_files').fetchone(),
                             ('valid',60,'h264',1920,1080,3))
            video.chmod(0o600)
            validate('Performer/renamed.mp4',video)
            self.assertEqual(conn.execute('SELECT phash FROM processed_files').fetchone()[0],'valid')
            # Same-size edits still invalidate when modification time changes.
            before = video.stat()
            video.write_bytes(b'edit'[:3])
            os.utime(video, ns=(before.st_atime_ns,before.st_mtime_ns+1_000_000_000))
            validate('Performer/renamed.mp4',video)
            self.assertEqual(conn.execute('SELECT phash,media_duration FROM processed_files').fetchone(),(None,None))
            conn.execute("UPDATE processed_files SET phash='valid',media_duration=60,filename='clip.mp4'")
            replacement=video.with_suffix('.new');replacement.write_bytes(b'new');replacement.replace(video)
            validate('clip.mp4',video)
            self.assertEqual(conn.execute('SELECT phash,media_duration FROM processed_files').fetchone(),(None,None))
