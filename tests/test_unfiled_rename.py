import ast
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from download_safety import require_complete_download, PartialDownloadPending

class UnfiledRenameTests(unittest.TestCase):
    def test_rename_collision_partial_and_database_rollback(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_rename_unfiled_match')
        db=SimpleNamespace(rename_file=Mock(return_value=True))
        env=dict(Path=Path,db=db,require_complete_download=require_complete_download,
                 PartialDownloadPending=PartialDownloadPending,_sanitize_fs_component=lambda x:x,
                 _strip_ts_uid=lambda x:(x,''),safe_move=lambda a,b:a.rename(b),
                 _carry_queue_thumbs=Mock(),emit=Mock())
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),env)
        rename=env['_rename_unfiled_match']
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);video=root/'random.mp4';video.write_bytes(b'video')
            settings={'source_dir':temp};plan={'studio':'Studio','date':'2026-10-18','perf_names':['Alice'],'title':'Scene'}
            part=root/'random.mp4.part';part.touch()
            self.assertEqual(rename(video,video.name,settings,plan)[0],video)
            part.unlink()
            db.rename_file.return_value=False
            self.assertEqual(rename(video,video.name,settings,plan)[0],video)
            self.assertTrue(video.exists())
            db.rename_file.return_value=True
            target,key=rename(video,video.name,settings,plan)
            self.assertEqual(key,'Studio - 2026-10-18 - Alice - Scene.mp4')
            self.assertEqual(target.read_bytes(),b'video');self.assertFalse(video.exists())
            video.write_bytes(b'another')
            self.assertEqual(rename(video,video.name,settings,plan)[0],video)
            self.assertEqual(target.read_bytes(),b'video')
