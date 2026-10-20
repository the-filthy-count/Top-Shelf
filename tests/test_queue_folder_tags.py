import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_filename_identity import load_function
from download_safety import require_complete_download, PartialDownloadPending

class FolderTagTests(unittest.TestCase):
    def test_tags_once_preserves_partials_and_sidecars(self):
        db=SimpleNamespace(upsert_file=Mock(),rename_file=Mock(return_value=True))
        env=dict(Path=Path,os=os,db=db,VIDEO_EXTENSIONS={'.mp4'},
                 require_complete_download=require_complete_download,PartialDownloadPending=PartialDownloadPending,
                 _queue_group_hold_ready=lambda p:True,safe_move=lambda a,b:a.rename(b),
                 _carry_queue_thumbs=Mock(),emit=Mock())
        tag=load_function('main.py','_tag_queue_folder_files',env)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'Leana Lovings';folder.mkdir()
            (folder/'POV.mp4').write_bytes(b'video');(folder/'POV.nfo').write_text('metadata')
            (root/'loose.mp4').touch()
            partial=root/'unfinished';partial.mkdir();(partial/'clip.mp4').touch();(partial/'clip.part').touch()
            tag(root);tag(root)
            self.assertEqual((folder/'[Leana Lovings] POV.mp4').read_bytes(),b'video')
            self.assertTrue((folder/'[Leana Lovings] POV.nfo').exists())
            self.assertTrue((partial/'clip.mp4').exists());self.assertTrue((partial/'clip.part').exists())
            self.assertTrue((root/'loose.mp4').exists());db.rename_file.assert_called_once()
            db.rename_file.return_value=False
            (folder/'other.mp4').touch();tag(root)
            self.assertTrue((folder/'other.mp4').exists())
