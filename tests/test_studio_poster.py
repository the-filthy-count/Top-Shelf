import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
from PIL import Image, ImageDraw
from app.utils.studio_poster import ensure_studio_poster, render_poster
from test_duplicate_filing_safety import load


class StudioPosterTests(unittest.TestCase):
    def logo(self):
        image = Image.new('RGBA', (400, 100))
        ImageDraw.Draw(image).rectangle((20, 20, 380, 80), fill='white')
        return image

    def test_render_dimensions_dark_edges_and_randomness(self):
        first = render_poster(self.logo(), 10)
        second = render_poster(self.logo(), 11)
        self.assertEqual(first.size, (1000, 1500))
        self.assertLess(max(first.getpixel((0, 0))), 20)
        self.assertGreater(max(first.getpixel((500, 500))), 40)
        self.assertEqual(first.getpixel((500, 750)), (255, 255, 255))
        self.assertFalse(np.array_equal(first, second))

    def test_existing_art_preserved_and_failed_save_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logo = root / 'logo.png'
            self.logo().save(logo)
            old = root / 'Poster.JPG'
            old.write_bytes(b'original')
            self.assertFalse(ensure_studio_poster(root, logo))
            self.assertEqual(old.read_bytes(), b'original')
            old.unlink()
            with patch.object(Image.Image, 'save', side_effect=OSError('disk full')):
                with self.assertRaises(OSError):
                    ensure_studio_poster(root, logo)
            self.assertFalse((root / 'poster.webp').exists())
            self.assertEqual(list(root.glob('.studio-poster-*')), [])
            self.assertTrue(ensure_studio_poster(root, logo, 10))
            with Image.open(root / 'poster.webp') as poster:
                self.assertEqual(poster.size, (1000, 1500))
                self.assertEqual(poster.format, 'WEBP')
            saved = (root / 'poster.webp').read_bytes()
            self.assertFalse(ensure_studio_poster(root, logo, 20))
            self.assertEqual((root / 'poster.webp').read_bytes(), saved)

    def test_backfill_uses_studio_index_and_series_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            series = root / 'series'
            series.mkdir()
            studio = series / 'Studio'
            studio.mkdir()
            indexed = root / 'Indexed studio'
            indexed.mkdir()
            create = Mock(return_value=True)
            listing = Mock(return_value=[{'path': str(indexed), 'folder_name': 'Indexed studio'}])
            env = {'Path': Path, 'db': SimpleNamespace(favourite_list=listing,
                   get_settings=lambda: {'series_dir': str(series)}),
                   '_ensure_studio_folder_poster': create}
            scan = load('main.py', '_backfill_studio_posters', env)
            self.assertEqual(scan(), 2)
            listing.assert_called_once_with(kind='studio')
            self.assertEqual({call.args[0] for call in create.call_args_list}, {studio, indexed})
