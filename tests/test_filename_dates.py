import unittest
from filename_dates import extract_filename_date

class FilenameDateTests(unittest.TestCase):
    def test_formats(self):
        for token in ['20261014','2026.10.14','2026-10-14','2026_10_14','2026 10 14','26.10.14','14-10-2026','10-14-2026']:
            with self.subTest(token=token):
                self.assertEqual(extract_filename_date('Studio.'+token+'.Title.mp4')[0],'2026-10-14')
    def test_invalid_ambiguous_and_numeric_ids(self):
        for token in ['20260230','20261314','03-04-2026','1232026101456','no date 1080p']:
            self.assertEqual(extract_filename_date(token),('',''))
    def test_leap_date(self):
        self.assertEqual(extract_filename_date('20240229')[0],'2024-02-29')
