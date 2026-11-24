import ast
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

class ViceFolderSyncTests(unittest.TestCase):
    def test_all_folders_created_despite_art_failure(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        names = {'_sync_vice_folders','_sync_vice_download_folders','_ensure_vice_folder'}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = {'vices_dir':str(root/'library'), 'vice_download_watch_dir':str(root/'inbox')}
            art = Mock(side_effect=RuntimeError('Artwork unavailable'))
            ns = {'Path':Path, 'db':SimpleNamespace(get_settings=lambda:settings),
                  '_load_vices':lambda:[{'name':'First'},{'name':'New Vice'}],
                  '_vice_folder_name':lambda name:name, '_log':Mock(),
                  '_ensure_vice_nfo_and_art':art, '_write_vice_metadata_info':Mock()}
            for node in tree.body:
                if isinstance(node,ast.FunctionDef) and node.name in names:
                    exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
            for _ in range(2):
                ns['_sync_vice_folders']()
            for area in ('library','inbox'):
                for vice in ('First','New Vice'):
                    self.assertTrue((root/area/vice).is_dir())
            self.assertEqual(art.call_count,4)
        loop = next(n for n in tree.body if isinstance(n,ast.For) and isinstance(n.target,ast.Name) and n.target.id=='_action_name')
        wrapped = ast.literal_eval(loop.iter)
        self.assertNotIn('_sync_vice_folders',wrapped)
        self.assertNotIn('_sync_vice_download_folders',wrapped)
