import errno
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_duplicate_filing_safety import load


class LibraryNfoAtomicTests(unittest.TestCase):
    def test_existing_metadata_survives_write_and_publish_failures(self):
        plan = dict(status='ok', nfo_title='title', show_title='show', studio='studio',
                    perf_names=[], date='', images=[])
        scans = Mock()
        env = {'Path': Path, 'os': os, 'db': SimpleNamespace(get_settings=lambda: {}),
               '_scene_filing_plan': lambda *a: plan,
               '_expand_group_performers_for_nfo': lambda names: names,
               'build_nfo': lambda *a, **kw: '<episodedetails>new</episodedetails>',
               '_ensure_library_filed_permissions': lambda path: None,
               'best_image_url': lambda images: '', 'emit': lambda message: None,
               'trigger_media_scans': scans}
        write = load('main.py', 'write_library_scene_nfo_and_thumb', env)
        for case in ('partial_write', 'publish_failure', 'success'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                scans.reset_mock()
                video = Path(tmp) / 'scene.mp4'
                video.write_bytes(b'video')
                nfo = video.with_suffix('.nfo')
                nfo.write_text('original metadata')
                nfo.chmod(0o640)
                thumb = video.with_name('scene-thumb.jpg')
                thumb.write_bytes(b'original thumb')

                def partial_write(path, data, *args, **kwargs):
                    with path.open('w', encoding='utf-8') as output:
                        output.write(data[:8])
                        output.flush()
                        raise OSError(errno.ENOSPC, 'simulated disk full')

                if case == 'partial_write':
                    with patch.object(Path, 'write_text', partial_write):
                        with self.assertRaises(OSError):
                            write(video, {}, 'StashDB')
                elif case == 'publish_failure':
                    with patch.object(os, 'replace', side_effect=OSError('publish failed')):
                        with self.assertRaises(OSError):
                            write(video, {}, 'StashDB')
                else:
                    self.assertEqual(write(video, {}, 'StashDB')['status'], 'ok')
                self.assertEqual(nfo.read_text(), '<episodedetails>new</episodedetails>'
                                 if case == 'success' else 'original metadata')
                self.assertEqual(nfo.stat().st_mode & 0o777, 0o640)
                self.assertEqual(video.read_bytes(), b'video')
                self.assertEqual(thumb.read_bytes(), b'original thumb')
                self.assertEqual(list(Path(tmp).glob('.scene-nfo-*')), [])
                self.assertEqual(scans.call_count, int(case == 'success'))
