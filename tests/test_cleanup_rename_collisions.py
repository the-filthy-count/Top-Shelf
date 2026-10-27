import tempfile
import threading
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class CleanupRenameCollisionTests(unittest.TestCase):
    def test_all_existing_suffixes_and_symlinks_are_preserved(self):
        for nested in (False,True):
            with self.subTest(nested=nested),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);folder=root/'release' if nested else root;folder.mkdir(exist_ok=True)
                for name in ('clip.mp4','clip_1.mp4','clip_2.mp4'):(folder/name).write_bytes(name.encode())
                (folder/'clip_3.mp4').symlink_to(folder/'missing-target')
                src=folder/'clip.1080p.mp4';src.write_bytes(b'incoming');rekey=Mock()
                env={'Path':Path,'_staging_move_lock':threading.Lock(),'safe_move':lambda a,b:a.rename(b),
                     '_clean_release_filename':lambda n:'clip.mp4','emit':Mock()}
                load('main.py','_move_staged_video',env)
                clean=load('main.py','_clean_and_rename_on_disk',env)
                old=src.relative_to(root).as_posix();expected=(folder/'clip_4.mp4').relative_to(root).as_posix()
                self.assertEqual(clean(root,old,rekey_fn=rekey),expected)
                self.assertEqual((root/expected).read_bytes(),b'incoming')
                for name in ('clip.mp4','clip_1.mp4','clip_2.mp4'):self.assertEqual((folder/name).read_bytes(),name.encode())
                self.assertTrue((folder/'clip_3.mp4').is_symlink())
                rekey.assert_called_once_with(old,expected)

    def test_failed_move_keeps_source_and_does_not_rekey(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);src=root/'clip.1080p.mp4';src.write_bytes(b'incoming');rekey=Mock()
            clean=load('main.py','_clean_and_rename_on_disk',{'Path':Path,'_clean_release_filename':lambda n:'clip.mp4',
                '_move_staged_video':Mock(side_effect=OSError('denied')),'emit':Mock()})
            self.assertEqual(clean(root,src.name,rekey_fn=rekey),src.name)
            self.assertEqual(src.read_bytes(),b'incoming');rekey.assert_not_called()

class FolderTagCleanupTests(unittest.TestCase):
    def test_matching_folder_label_survives_cleanup_without_repeat_move(self):
        import re
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'Sybil';folder.mkdir()
            video=folder/'[Sybil] Olga.Sybil.1080p.mp4';video.write_bytes(b'video')
            def cleaner(name):
                return re.sub(r'\[[^]]+\]\s*','',name).replace('.1080p','').replace('Olga.Sybil','Olga Sybil')
            def move(src,dst):
                src.rename(dst);return dst
            moved=Mock(side_effect=move)
            env={'Path':Path,'_clean_release_filename':cleaner,'_move_staged_video':moved,
                 'db':SimpleNamespace(rename_file=Mock()),'emit':Mock()}
            clean=load('main.py','_clean_and_rename_on_disk',env)
            result=clean(root,'Sybil/'+video.name)
            self.assertEqual(result,'Sybil/[Sybil] Olga Sybil.mp4')
            self.assertEqual(clean(root,result),result)
            self.assertEqual(moved.call_count,1)
            self.assertEqual((root/result).read_bytes(),b'video')
