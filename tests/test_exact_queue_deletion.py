import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class ExactQueueDeletionTests(unittest.TestCase):
    def test_missing_file_never_deletes_another_extension(self):
        for prefix,setting in [('', 'source_dir'),('Vices/Category/','vice_download_watch_dir'),('JAV/','jav_download_watch_dir')]:
            with self.subTest(prefix=prefix),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);folder=root/'Category' if prefix.startswith('Vices') else root;folder.mkdir(exist_ok=True)
                for ext in ('.mkv','.avi'):(folder/('clip'+ext)).write_bytes(ext.encode())
                (folder/'clip.nfo').write_text('keep metadata')
                lookup=Mock(side_effect=AssertionError('alternate resolver must not run'))
                db=SimpleNamespace(get_settings=lambda:{setting:str(root)},update_file=Mock(),update_movie=Mock())
                delete=load('main.py','_queue_delete_filename_sync',{'Path':Path,'db':db,'emit':Mock(),'_resolve_queue_file_path':lookup})
                result=delete(prefix+'clip.mp4')
                self.assertTrue(result['already_missing']);self.assertFalse(result['deleted']);self.assertEqual(result['freed_bytes'],0)
                for ext in ('.mkv','.avi'):self.assertEqual((folder/('clip'+ext)).read_bytes(),ext.encode())
                self.assertEqual((folder/'clip.nfo').read_text(),'keep metadata')
                lookup.assert_not_called()
                exact=folder/'clip.mp4';exact.write_bytes(b'video')
                result=delete(prefix+'clip.mp4')
                self.assertTrue(result['deleted']);self.assertFalse(exact.exists())
                self.assertTrue((folder/'clip.mkv').exists());self.assertTrue((folder/'clip.avi').exists())
