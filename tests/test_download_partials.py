import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from download_safety import require_complete_download, cleanup_download_directory, PartialDownloadPending

class PartialDownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.folder=Path(self.tmp.name)/'download';self.folder.mkdir()
        self.video=self.folder/'finished.mp4';self.video.write_bytes(b'video')
    def partial(self):
        nested=self.folder/'nested';nested.mkdir()
        path=nested/'unfinished.MP4.PART';path.write_bytes(b'partial');return path
    def test_nested_partial_blocks_directory_and_direct_video(self):
        part=self.partial()
        for entry in (self.folder,self.video,part):
            with self.assertRaises(PartialDownloadPending):require_complete_download(entry)
        self.assertEqual(part.read_bytes(),b'partial')
        self.assertTrue(self.video.exists())
    def test_cleanup_preserves_entire_directory_with_partial(self):
        part=self.partial()
        with self.assertRaises(PartialDownloadPending):cleanup_download_directory(self.folder)
        self.assertTrue(part.exists());self.assertTrue(self.video.exists())
    def test_processing_resumes_after_partial_renamed(self):
        part=self.partial();part.rename(part.with_suffix(''))
        require_complete_download(self.folder)
    def test_cleanup_never_unlinks_partial_created_after_precheck(self):
        part=self.partial()
        with patch('download_safety.require_complete_download'):
            cleanup_download_directory(self.folder)
        self.assertEqual(part.read_bytes(),b'partial');self.assertTrue(self.folder.exists())
    def test_finished_cleanup_removes_junk_and_empty_folders(self):
        cleanup_download_directory(self.folder)
        self.assertFalse(self.folder.exists())
    def test_all_download_entrypoints_guard_before_any_processing(self):
        names={'_process_download_entry','_process_stage_entry_to_queue','_process_vice_download_entry','_process_jav_download_entry'}
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        funcs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        self.assertEqual(len(funcs),4)
        for f in funcs:
            call=f.body[1].value
            self.assertIsInstance(call,ast.Call);self.assertEqual(call.func.id,'require_complete_download')

if __name__=='__main__':unittest.main()
