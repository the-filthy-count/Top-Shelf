import tempfile
import unittest
from pathlib import Path
from test_duplicate_filing_safety import load


class MissingDuplicateTests(unittest.TestCase):
    def test_missing_copy_is_not_a_duplicate_but_can_reappear(self):
        with tempfile.TemporaryDirectory() as tmp:
            existing = Path(tmp) / 'scene.mkv'
            missing = Path(tmp) / 'scene.mp4'
            existing.touch()
            rows = [{'destination': str(p), 'phash_1': 'abc'} for p in (existing, missing)]
            scan = load('database.py', 'library_files_duplicate_groups', {
                'library_files_list_active_rows': lambda: rows,
            })
            self.assertEqual(scan(), [])
            missing.touch()
            self.assertEqual(len(scan()[0]['files']), 2)
            self.assertEqual(len(rows), 2)

    def test_missing_high_resolution_copy_never_recommended(self):
        env = {'Path': Path}
        for name in ('_dup_resolution_pixels', '_dup_int_size', '_duplicate_recommend'):
            load('main.py', name, env)
        existing = {'destination': 'scene.mkv', 'on_disk': True, 'media_width': 720}
        missing = {'destination': 'scene.mp4', 'on_disk': False, 'media_width': 3840}
        recommend = env['_duplicate_recommend']
        self.assertEqual(recommend([missing, existing])['keeper'], existing)
        self.assertIsNone(recommend([missing])['keeper'])

    def test_file_disappearing_after_scan_hides_group(self):
        env = {'db': type('DB', (), {'library_files_duplicate_groups': staticmethod(lambda: [
            {'phash': 'abc', 'files': [{'on_disk': True}, {'on_disk': False}]}])}),
            '_health_dup_file_payload': lambda r: r,
            '_duplicate_recommend': lambda rows: self.fail('Must skip incomplete group')}
        scan = load('main.py', '_health_duplicate_groups_payload', env)
        self.assertEqual(scan(), [])
