import os
from pathlib import Path
import tempfile
import unittest
from download_safety import sweep_empty_download_dirs

class EmptyDownloadTests(unittest.TestCase):
    def test_empty_nested_removed_files_and_roots_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'empty'/'nested').mkdir(parents=True)
            (root/'video').mkdir();(root/'video'/'clip.mp4').write_bytes(b'video')
            (root/'metadata').mkdir();(root/'metadata'/'info.nfo').write_text('keep')
            protected=root/'configured';protected.mkdir()
            self.assertEqual(sweep_empty_download_dirs(root,min_age_seconds=0,protected_roots=[protected]),2)
            self.assertTrue(root.is_dir());self.assertTrue(protected.is_dir())
            self.assertTrue((root/'video'/'clip.mp4').exists())
            self.assertTrue((root/'metadata'/'info.nfo').exists())

    def test_partial_release_and_vice_category_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'category'/'empty').mkdir(parents=True)
            (root/'partial'/'empty').mkdir(parents=True)
            (root/'partial'/'clip.part').touch()
            (root/'unrelated.part').touch()
            self.assertEqual(sweep_empty_download_dirs(root,preserve_top_level=True,min_age_seconds=0),1)
            self.assertTrue((root/'category').exists())
            self.assertTrue((root/'partial'/'empty').exists())
            self.assertTrue((root/'unrelated.part').exists())

    def test_recent_and_symlink_directories_preserved(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            root=Path(tmp);(root/'recent').mkdir();(root/'link').symlink_to(outside)
            self.assertEqual(sweep_empty_download_dirs(root),0)
            self.assertTrue((root/'recent').exists());self.assertTrue((root/'link').is_symlink())
            os.utime(root/'recent',(1,1))
            self.assertEqual(sweep_empty_download_dirs(root),1)
            self.assertTrue(Path(outside).exists())
