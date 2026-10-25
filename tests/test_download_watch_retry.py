"""Exercise scheduler functions without booting application jobs."""
import ast
import threading
import time
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from download_safety import require_complete_download, PartialDownloadPending

class WatchRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.settings = {'download_watch_enabled':'true', 'download_watch_hold_secs':'30'}
        self.env = dict(Path=Path, time=time, _download_lock=threading.Lock(),
                        db=SimpleNamespace(get_settings=lambda:self.settings),
                        PartialDownloadPending=PartialDownloadPending, emit=Mock())
        for name in ('downloads','movie_downloads','vice_downloads','jav_downloads','vice_stage_downloads','jav_stage_downloads'):
            self.env['_pending_'+name] = {}
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        names={'_seed_pending_downloads','_retry_partial_download','_check_pending_downloads'}
        selected=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[])
        exec(compile(selected,'main.py','exec'),self.env)

    def test_startup_scan_preserves_newer_hold_and_respects_disabled(self):
        file=self.root/'existing.mp4';file.touch()
        directory=self.root/'album';directory.mkdir()
        pending=self.env['_pending_downloads'];pending[str(file)]=time.time()+900
        deadline=pending[str(file)]
        self.env['_seed_pending_downloads'](self.root,pending)
        self.assertIn(str(directory),pending)
        self.assertEqual(pending[str(file)],deadline)
        self.settings['download_watch_enabled']='false';pending.clear()
        self.env['_seed_pending_downloads'](self.root,pending)
        self.assertFalse(pending)

    def test_partial_retries_then_processes_without_another_event(self):
        folder=self.root/'album';folder.mkdir()
        part=folder/'clip.mp4.part';part.write_bytes(b'incomplete')
        pending=self.env['_pending_downloads'];pending[str(folder)]=0
        processed=[]
        def process(entry,dest):
            require_complete_download(entry)
            processed.append(entry)
        cleanup=Mock()
        self.env.update(_dl_resolve_import_dest_dir=lambda *args:(self.root,None),
                        _process_download_entry=process,
                        _remove_client_jobs_matching_watch_folder=cleanup)
        self.env['_check_pending_downloads']()
        self.assertGreater(pending[str(folder)],time.time())
        self.assertEqual(part.read_bytes(),b'incomplete');cleanup.assert_not_called()
        part.rename(folder/'clip.mp4');pending[str(folder)]=0
        self.env['_check_pending_downloads']()
        self.assertEqual(processed,[folder]);self.assertFalse(pending)
        cleanup.assert_called_once()

    def test_shared_root_does_not_block_unrelated_files_or_directories(self):
        part=self.root/'active.mp4.part';part.write_bytes(b'partial')
        complete=self.root/'complete.mp4';complete.touch()
        folder=self.root/'finished';folder.mkdir();(folder/'video.mp4').touch()
        require_complete_download(complete,shared_root=True)
        require_complete_download(folder)
        own=self.root/'complete.mp4.part';own.touch()
        with self.assertRaises(PartialDownloadPending):
            require_complete_download(complete,shared_root=True)
        self.assertEqual(part.read_bytes(),b'partial')

if __name__=='__main__':unittest.main()
