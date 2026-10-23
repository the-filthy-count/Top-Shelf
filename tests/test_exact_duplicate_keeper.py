import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load

class ExactKeeperTests(unittest.TestCase):
    def test_missing_keeper_cannot_be_replaced_by_other_extension(self):
        for scope,key in [('', 'source_dir'),('Vices/Category/','vice_download_watch_dir'),('JAV/','jav_download_watch_dir')]:
            with self.subTest(scope=scope),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);folder=root/'Category' if scope.startswith('Vices') else root;folder.mkdir(exist_ok=True)
                alternate=folder/'keeper.mkv';alternate.touch();loser=folder/'other.mp4';loser.touch()
                delete=Mock(return_value={'ok':True,'deleted':True,'freed_bytes':1})
                env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{key:str(root)},media_file_signature=lambda p:p.stat().st_size),
                     '_queue_delete_filename_sync':delete}
                load('main.py','_exact_queue_file_path',env)
                resolve=load('main.py','_queue_duplicate_resolve_one_sync',env)
                self.assertFalse(resolve(scope+'keeper.mp4',[scope+'other.mp4'])['ok'])
                delete.assert_not_called();self.assertTrue(loser.exists());self.assertTrue(alternate.exists())
                keeper=folder/'keeper.mp4';keeper.touch()
                result=resolve(scope+'keeper.mp4',[scope+'./keeper.mp4',scope+'other.mp4'])
                self.assertTrue(result['ok']);delete.assert_called_once_with(scope+'other.mp4')

    def test_outside_keeper_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inbox=root/'inbox';inbox.mkdir();outside=root/'outside.mp4';outside.touch()
            (inbox/'escape.mp4').symlink_to(outside)
            exact=load('main.py','_exact_queue_file_path',{'Path':Path})
            for name in ('../outside.mp4','escape.mp4'):
                self.assertIsNone(exact(name,{'source_dir':str(inbox)}))
