import tempfile
import unittest
from pathlib import Path
from test_duplicate_filing_safety import load

class QueuePathTests(unittest.TestCase):
    def test_nested_history_does_not_borrow_root_identity(self):
        lookup=load('database.py','queue_cached_row',{'Path':Path})
        rows={'scene.mp4':{'status':'filed'},'scene':{'performers':'wrong'}}
        self.assertEqual(lookup(rows,'Person/scene.mp4'),{})
        rows['Person/scene']={'status':'unmatched'}
        self.assertEqual(lookup(rows,'Person/scene.mp4'),{'status':'unmatched'})
        rows['Person/scene.mp4']={'status':'pending'}
        self.assertEqual(lookup(rows,'Person/scene.mp4'),{'status':'pending'})

    def test_vice_keys_round_trip_and_do_not_collide(self):
        key=load('database.py','queue_file_key',{'Path':Path})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);src=root/'queue';vice=root/'vice';src.mkdir();vice.mkdir()
            settings={'source_dir':str(src),'vice_download_watch_dir':str(vice)}
            for name in ('One','Two'):
                (vice/name).mkdir();video=vice/name/'scene.mp4';video.touch()
                self.assertEqual(key(video,settings),f'Vices/{name}/scene.mp4')
            video=src/'scene.mp4';video.touch()
            self.assertEqual(key(video,settings),'scene.mp4')
