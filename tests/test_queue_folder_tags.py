import os
import re
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
        env=dict(Path=Path,os=os,re=re,db=db,VIDEO_EXTENSIONS={'.mp4'},
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
            old='[Wrong Name] [Another Name] scene [1080p].mp4'
            (folder/old).write_bytes(b'correct me')
            (folder/Path(old).with_suffix('.nfo')).write_text('sidecar')
            (folder/'[ts-1234abcd] tracked.mp4').touch()
            tag(root)
            corrected=folder/'[Leana Lovings] scene [1080p].mp4'
            self.assertEqual(corrected.read_bytes(),b'correct me')
            self.assertTrue(corrected.with_suffix('.nfo').exists())
            self.assertTrue((folder/'[Leana Lovings] [ts-1234abcd] tracked.mp4').exists())
            calls=db.rename_file.call_count
            tag(root)
            self.assertEqual(db.rename_file.call_count,calls)
            # A correction must not overwrite a pre-existing target.
            conflict=folder/'[Wrong] scene [1080p].mp4'
            conflict.write_bytes(b'keep me')
            tag(root)
            self.assertEqual(conflict.read_bytes(),b'keep me')
            db.rename_file.return_value=False
            (folder/'other.mp4').touch();tag(root)
            self.assertTrue((folder/'other.mp4').exists())
