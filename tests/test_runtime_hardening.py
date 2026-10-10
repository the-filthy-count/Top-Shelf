import errno
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from runtime_jobs import ActivityBuffer, BoundedExecutor, bounded_results
from app.utils.fs import safe_move

class HardeningTests(unittest.TestCase):
    def test_logs_without_viewers_stay_bounded(self):
        activity = ActivityBuffer(limit=3)
        for i in range(10):
            activity.append(str(i))
        self.assertEqual(len(activity.pending), 3)
        self.assertEqual(activity.dropped, 7)
        one, two = activity.subscribe(), activity.subscribe()
        activity.append('shared')
        self.assertEqual(one.get_nowait(), 'shared')
        self.assertEqual(two.get_nowait(), 'shared')
        activity.unsubscribe(one)
        self.assertEqual(len(activity.subscribers), 1)

    def test_executor_rejects_backlog_and_releases_capacity(self):
        release = threading.Event()
        pool = BoundedExecutor(max_workers=1, pending=1)
        try:
            first = pool.submit(release.wait, 3)
            second = pool.submit(lambda: 2)
            with self.assertRaises(RuntimeError):
                pool.submit(lambda: 3)
            second.cancel()
            third = pool.submit(lambda: 3)
            release.set()
            first.result(3)
            self.assertEqual(third.result(3), 3)
        finally:
            release.set()
            pool.shutdown()

    def test_batched_results(self):
        with BoundedExecutor(max_workers=2, pending=2) as pool:
            self.assertEqual(sorted(bounded_results(pool, lambda x: x*x, range(20), 4)),
                             [(x,x*x) for x in range(20)])

    def test_copy_failure_keeps_existing_destination_and_source(self):
        with tempfile.TemporaryDirectory() as root:
            source, dest = Path(root)/'source', Path(root)/'dest'
            source.write_bytes(b'new'); dest.write_bytes(b'old')
            with patch('app.utils.fs.os.replace', side_effect=OSError(errno.EXDEV, 'cross device')), \
                 patch('app.utils.fs.shutil.copyfile', side_effect=OSError(errno.ENOSPC, 'full')):
                with self.assertRaises(OSError):
                    safe_move(source, dest)
            self.assertEqual(dest.read_bytes(), b'old')
            self.assertEqual(source.read_bytes(), b'new')
            self.assertFalse(list(Path(root).glob('*.partial')))

    def test_cross_device_move_publishes_only_after_copy(self):
        import os, shutil
        with tempfile.TemporaryDirectory() as root:
            source, dest = Path(root)/'source', Path(root)/'dest'
            source.write_bytes(b'new'); dest.write_bytes(b'old')
            replace, copy = os.replace, shutil.copyfile
            def cross_device(src, dst):
                if Path(src) == source:
                    raise OSError(errno.EXDEV, 'cross device')
                replace(src, dst)
            def copying(src, dst):
                self.assertEqual(dest.read_bytes(), b'old')
                return copy(src, dst)
            with patch('app.utils.fs.os.replace', side_effect=cross_device), \
                 patch('app.utils.fs.shutil.copyfile', side_effect=copying):
                safe_move(source, dest)
            self.assertFalse(source.exists())
            self.assertEqual(dest.read_bytes(), b'new')

    def test_changing_source_is_retained(self):
        import shutil
        with tempfile.TemporaryDirectory() as root:
            source, dest = Path(root)/'source', Path(root)/'dest'
            source.write_bytes(b'new'); dest.write_bytes(b'old')
            copy = shutil.copyfile
            def changing(src, dst):
                copy(src, dst)
                source.write_bytes(b'changed')
            with patch('app.utils.fs.os.replace', side_effect=OSError(errno.EXDEV, 'cross device')), \
                 patch('app.utils.fs.shutil.copyfile', side_effect=changing):
                with self.assertRaises(OSError):
                    safe_move(source, dest)
            self.assertEqual(source.read_bytes(), b'changed')
            self.assertEqual(dest.read_bytes(), b'old')

    def test_media_timeout_reaps_child_and_releases_slot(self):
        import subprocess, sys
        import media_budget
        with self.assertRaises(subprocess.TimeoutExpired):
            with media_budget.media_slot('test timeout'):
                media_budget.run_process([sys.executable, '-c', 'import time; time.sleep(10)'], timeout=.1)
        self.assertFalse(media_budget._processes)
        self.assertEqual(media_budget._active, 0)
        self.assertEqual(media_budget.run_process([sys.executable, '-c', 'print(42)'], timeout=2, text=True).stdout.strip(), '42')

    def test_media_output_is_bounded(self):
        import sys
        import media_budget
        with self.assertRaises(ValueError):
            media_budget.run_process([sys.executable, '-c', 'import sys; sys.stdout.write("x" * (17 * 1024 * 1024))'], timeout=5)
        self.assertFalse(media_budget._processes)

    def test_image_transfer_limit_without_content_length(self):
        from unittest.mock import MagicMock
        import bounded_images
        response = MagicMock()
        response.__enter__.return_value = response
        response.headers = {}
        response.iter_content.return_value = iter([b'1234', b'5678'])
        with patch.object(bounded_images, 'MAX_BYTES', 5), patch.object(bounded_images.requests, 'get', return_value=response):
            with self.assertRaisesRegex(ValueError, 'too large'):
                bounded_images.download('https://example.test/image')
        response.__exit__.assert_called_once()

    def test_source_ownership_rejects_second_process(self):
        import subprocess, sys
        from runtime_jobs import claim_sources
        with tempfile.TemporaryDirectory() as root:
            claim_sources([root])
            child = subprocess.run([sys.executable, '-c', 'from runtime_jobs import claim_sources; import sys; claim_sources([sys.argv[1]])', root], capture_output=True, timeout=5)
            self.assertNotEqual(child.returncode, 0)
            self.assertIn(b'Another Top-Shelf instance owns', child.stderr)
            from runtime_jobs import _source_handles
            _source_handles.pop(str(Path(root).resolve())).close()
