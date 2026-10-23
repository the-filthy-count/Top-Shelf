import tempfile
import unittest
from pathlib import Path
from test_duplicate_filing_safety import load

class LibraryCollisionNamesTests(unittest.TestCase):
    def test_keeps_searching_beyond_99(self):
        choose=load('main.py','_next_free_dest_slot',{'Path':Path})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);dst=root/'scene.mp4';dst.write_bytes(b'original')
            for n in range(2,105):(root/f'scene - {n}.mp4').write_bytes(b'existing')
            (root/'scene - 105.mp4').symlink_to(root/'missing')
            self.assertEqual(choose(dst),root/'scene - 106.mp4')
            self.assertEqual(dst.read_bytes(),b'original')
            for n in range(2,105):self.assertEqual((root/f'scene - {n}.mp4').read_bytes(),b'existing')
            self.assertTrue((root/'scene - 105.mp4').is_symlink())

    def test_free_path_and_dangling_primary_symlink(self):
        choose=load('main.py','_next_free_dest_slot',{'Path':Path})
        with tempfile.TemporaryDirectory() as tmp:
            dst=Path(tmp)/'scene.mp4'
            self.assertEqual(choose(dst),dst)
            dst.symlink_to(Path(tmp)/'missing')
            self.assertEqual(choose(dst),Path(tmp)/'scene - 2.mp4')
            self.assertTrue(dst.is_symlink())

    def test_filing_preserves_unavailable_symlink_and_occupied_slots(self):
        from app.utils.fs import safe_move

        def fingerprint(path):
            # Exercise the real missing-file error handling without video tools.
            return path.read_bytes().hex()

        env = {'Path': Path, 'safe_move': safe_move,
               'compute_phash': fingerprint, 'emit': lambda message: None}
        load('main.py', '_phash_for_library_destination', env)
        load('main.py', '_next_free_dest_slot', env)
        move = load('main.py', '_quality_gate_move', env)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / 'incoming.mp4'
            src.write_bytes(b'incoming video')
            dst = root / 'scene.mp4'
            target = root / 'offline-volume' / 'original.mp4'
            dst.symlink_to(target)
            occupied = root / 'scene - 2.mp4'
            occupied.write_bytes(b'existing video')
            result = move(src, dst)
            self.assertEqual(result, root / 'scene - 3.mp4')
            self.assertEqual(result.read_bytes(), b'incoming video')
            self.assertFalse(src.exists())
            self.assertTrue(dst.is_symlink())
            self.assertEqual(dst.readlink(), target)
            self.assertEqual(occupied.read_bytes(), b'existing video')
            self.assertFalse(target.exists())
