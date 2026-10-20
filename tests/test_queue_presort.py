import re
import tempfile
import threading
import unicodedata
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_filename_identity import load_function
from download_safety import require_complete_download, PartialDownloadPending

class PresortTests(unittest.TestCase):
    def test_fresh_exact_names_move_without_scene_match(self):
        env={'re':re,'unicodedata':unicodedata,
             '_TOKEN_SPLIT_RE':re.compile(r'[^A-Za-z0-9]+|(?<=[a-z])(?=[A-Z])|(?<=[A-Za-z])(?=\d)'),
             '_RELEASE_TAG_RE':re.compile(r'\[ts-[a-z0-9]+\]')}
        tokenize=load_function('database.py','_tokenize_for_match',env)
        env['_normalize_release_for_match']=lambda s:re.sub('[^a-z0-9]','',s.lower())
        guess=load_function('database.py','_library_guess_from_release_name',env)
        names=['Sasha Grey','Leana Lovings','Lillith Lodge','Lea Pam','Dare Taylor']
        indexes=([],[(tuple(tokenize(name,release=False)),name,i) for i,name in enumerate(names)])
        db=SimpleNamespace(processed_files_map_by_filename=lambda **kw:{},
            _build_library_match_indexes=Mock(return_value=indexes),
            _library_guess_from_release_name=guess,rename_file=Mock())
        env.update(Path=Path,db=db,VIDEO_EXTENSIONS={'.mp4'},emit=Mock(),
            _queue_group_hold_ready=lambda p:True,_sanitize_fs_component=lambda x:x,
            _resolve_library_folder_name=lambda n,k:n if n in names else '',
            safe_move=lambda a,b:a.rename(b),_queue_group_seen_lock=threading.Lock(),
            _queue_group_seen={},require_complete_download=require_complete_download,
            PartialDownloadPending=PartialDownloadPending)
        load_function('main.py','_pick_queue_group_name_from_row',env)
        sort=load_function('main.py','_group_queue_root_files_by_hint_locked',env)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cases={'001 - Sasha Grey Scene.mp4':'Sasha Grey','Leana Lovings POV.mp4':'Leana Lovings',
                   'Lillith-Lodge- +(6).mp4':'Lillith Lodge','Léa Pam & Guest.mp4':'Lea Pam',
                   'daretaylor scene.mp4':'Dare Taylor'}
            for file in [*cases,'Taylor Dare scene.mp4','unknown.mp4','Sasha Grey pending.mp4']:
                (root/file).write_bytes(b'video')
            (root/'Sasha Grey pending.mp4.part').write_bytes(b'partial')
            self.assertEqual(sort(root),len(cases))
            for file,folder in cases.items():self.assertEqual((root/folder/file).read_bytes(),b'video')
            for file in ['Taylor Dare scene.mp4','unknown.mp4','Sasha Grey pending.mp4']:
                self.assertTrue((root/file).exists())
            self.assertEqual(sort(root),0)
            self.assertEqual((root/'Sasha Grey pending.mp4.part').read_bytes(),b'partial')
