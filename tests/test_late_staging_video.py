import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from download_safety import (VIDEO_EXTENSIONS, PartialDownloadPending,
                             cleanup_download_directory, require_complete_download)
from test_duplicate_filing_safety import load


class LateStagingVideoTests(unittest.TestCase):
    def test_video_arriving_during_import_is_preserved_then_imported_on_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            entry = root / 'release'
            entry.mkdir()
            queue = root / 'queue'
            queue.mkdir()
            (entry / 'first.mp4').write_bytes(b'first video')
            late = entry / 'second.MKV'
            injected = False

            def move(src, dst):
                nonlocal injected
                src.rename(dst)
                if not injected:
                    late.write_bytes(b'second video')
                    injected = True
                return dst

            env = {'Path': Path, 'require_complete_download': require_complete_download,
                   'cleanup_download_directory': cleanup_download_directory,
                   'emit': lambda message: None, '_strip_ts_uid': lambda name: (name, ''),
                   'db': SimpleNamespace(lookup_scene_grab_by_final_name=lambda name: None),
                   'VIDEO_EXTENSIONS': VIDEO_EXTENSIONS, '_looks_like_gibberish': lambda name: False,
                   '_clean_release_filename': lambda name: name, '_move_staged_video': move}
            process = load('main.py', '_process_download_entry', env)
            with self.assertRaises(PartialDownloadPending):
                process(entry, queue)
            self.assertEqual(late.read_bytes(), b'second video')
            self.assertEqual((queue / 'first.mp4').read_bytes(), b'first video')
            self.assertEqual(process(entry, queue), [queue / late.name])
            self.assertEqual((queue / late.name).read_bytes(), b'second video')
            self.assertFalse(entry.exists())

    def test_cleanup_preserves_nested_video(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / 'nested'
            folder.mkdir()
            video = folder / 'late.mp4'
            video.write_bytes(b'video')
            with self.assertRaises(PartialDownloadPending):
                cleanup_download_directory(root)
            self.assertEqual(video.read_bytes(), b'video')
