import ast
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load, ROOT

class YearAndDeleteTests(unittest.TestCase):
    def test_cleanup_preserves_years_but_removes_release_tags(self):
        env={'re':re,'Path':Path,'_strip_ts_uid':lambda s:(s,None)}
        names={'_FILENAME_JUNK_RE','_FILENAME_BRACKET_JUNK_RE','_FILENAME_URL_RE','_FILENAME_GROUP_TAIL_RE'}
        for node in ast.parse((ROOT/'main.py').read_text()).body:
            if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in names for t in node.targets):
                exec(compile(ast.Module(body=[node],type_ignores=[]),'<regex>','exec'),env)
        clean=load('main.py','_clean_release_filename',env)
        for brackets in ('(2020)','[2020]'):
            self.assertEqual(clean(f'The Movie {brackets} [RARBG] (1080p).mp4'),f'The Movie {brackets}.mp4')

    def test_jav_delete_updates_movie_history_and_blocks_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);jav=root/'jav';jav.mkdir();video=jav/'clip.mp4';video.write_bytes(b'video')
            outside=root/'outside.mp4';outside.touch();(jav/'escape.mp4').symlink_to(outside)
            db=SimpleNamespace(get_settings=lambda:{'jav_download_watch_dir':str(jav)},update_movie=Mock(),update_file=Mock())
            env={'Path':Path,'db':db,'emit':Mock(),'_resolve_queue_file_path':lambda fn,s:jav/fn.split('/',1)[1]}
            delete=load('main.py','_queue_delete_filename_sync',env)
            self.assertFalse(delete('JAV/escape.mp4')['ok']);self.assertTrue(outside.exists())
            self.assertFalse(delete('JAV/../outside.mp4')['ok'])
            self.assertTrue(delete('JAV/clip.mp4')['deleted']);self.assertFalse(video.exists())
            db.update_movie.assert_called_once_with('jav://clip.mp4',status='removed');db.update_file.assert_not_called()
            self.assertTrue(delete('JAV/clip.mp4')['already_missing'])
            self.assertFalse(delete('clip.mp4')['ok'])
