import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from test_filename_identity import load_function
import database as db
from download_safety import require_complete_queue_video, PartialDownloadPending
from app.core.movie_jobs import run

class HiddenQueueTests(unittest.TestCase):
    def test_walk_omits_dotfiles_and_entire_hidden_subtrees(self):
        env={'Path':Path,'os':os,'_library_walk_prune_basenames':lambda s:set(),
             '_is_video_filename':lambda s: s.endswith('.mp4')}
        walk=load_function('main.py','_movie_source_video_relpaths_impl',env)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            names=['normal.mp4','Actor/scene.mp4','.hidden.mp4','Actor/.hidden.mp4',
                   '.cache/scene.mp4','Actor/.cache/nested/scene.mp4']
            for name in names:
                file=root/name;file.parent.mkdir(parents=True,exist_ok=True);file.touch()
            self.assertEqual({rel for rel,_ in walk(root)}, {'normal.mp4','Actor/scene.mp4'})
            for name in names[2:]:
                self.assertIsNone(db.resolve_source_video_path(root,name))
                with self.assertRaises(PartialDownloadPending):
                    require_complete_queue_video(root/name,{'source_dir':str(root)})
            require_complete_queue_video(root/'normal.mp4',{'source_dir':str(root)})
            require_complete_queue_video(root/'Actor/scene.mp4',{'source_dir':str(root)})

    def test_direct_movie_processing_cannot_process_hidden_paths(self):
        process=Mock()
        with patch.object(db,'get_settings',side_effect=AssertionError('must skip before touching DB')):
            for name in ['.hidden.mp4','Folder/.cache/file.mp4']:
                self.assertEqual(run(name,None,None,process)['status'],'skipped')
        process.assert_not_called()

    def test_empty_hidden_directories_are_preserved(self):
        sweep=load_function('main.py','_sweep_empty_queue_subdirs',{'Path':Path,'os':os})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'.hidden/nested').mkdir(parents=True)
            (root/'visible/empty').mkdir(parents=True)
            self.assertEqual(sweep(root),2)
            self.assertTrue((root/'.hidden/nested').is_dir())

    def test_cleaner_leaves_hidden_names_and_folders_untouched(self):
        clean=load_function('main.py','_clean_and_rename_on_disk',{'Path':Path})
        for name in ['.hidden.1080p.mp4','Actor/.cache/file.1080p.mp4']:
            self.assertEqual(clean(Path('/unused'),name),name)

    def test_filesystem_events_do_not_enqueue_hidden_videos(self):
        import ast
        import threading
        import time
        from types import SimpleNamespace
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        nodes=[n for n in tree.body if isinstance(n,ast.ClassDef) and n.name in {'NewVideoHandler','NewMovieVideoHandler'}]
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            settings={'source_dir':tmp,'movies_source_dir':tmp}
            pending={};movies={}
            env=dict(Path=Path,time=time,FileSystemEventHandler=object,
                     db=SimpleNamespace(get_settings=lambda:settings),VIDEO_EXTENSIONS={'.mp4'},
                     _pending_lock=threading.Lock(),_pending_files=pending,_pending_movie_files=movies,
                     _queue_warm=Mock(),_movies_queue_warm=Mock(),emit=Mock())
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'watchers','exec'),env)
            for name in ['.hidden.mp4','Folder/.cache/file.mp4','visible.mp4']:
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.touch()
                for cls in ['NewVideoHandler','NewMovieVideoHandler']:
                    env[cls]().on_created(SimpleNamespace(src_path=str(path),is_directory=False))
            self.assertEqual(list(pending),['visible.mp4'])
            self.assertEqual(list(movies),['visible.mp4'])
