"""Studio logo conversion without importing or starting the live application."""
import ast
import base64
import asyncio
from io import BytesIO
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, MagicMock, patch
from urllib.parse import quote
import xml.etree.ElementTree as ET

from PIL import Image
from starlette.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[1]
SVG = b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 100"><path fill="red" d="M0 0h100v100H0z"/></svg>'
PNG_MAGIC = b'\x89PNG\r\n\x1a\n'


async def run_in_threadpool(func, *args, **kwargs):
    # Exercise the handler without starting application workers.
    return func(*args, **kwargs)


def load():
    names = {'_prepare_studio_svg', '_image_bytes_are_svg', '_normalise_studio_logo_to_png',
             'save_image_bytes_optimized', '_pil_normalize_for_output',
             '_pil_resize_max_edge', '_favourites_download_image_bytes',
             'api_favourites_apply_image'}
    nodes = []
    for node in ast.parse((ROOT / 'main.py').read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            node.decorator_list = []
            nodes.append(node)
    ns = dict(base64=base64, Path=Path, Image=Image, BytesIO=BytesIO, ET=ET, time=time,
              quote=quote, _PNG_MAGIC=PNG_MAGIC, _PNG_FILE_MAGIC=PNG_MAGIC,
              IMAGE_CACHE_JPEG_QUALITY=85, IMAGE_CACHE_WEBP_QUALITY=85,
              IMAGE_CACHE_WEBP_METHOD=4, Body=lambda value: value,
              run_in_threadpool=run_in_threadpool, JSONResponse=JSONResponse,
              FAVOURITES_SAVED_POSTER_MAX_EDGE=3200,
              FAVOURITES_FAV_IMAGE_MAX_EDGE=300)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), ns)
    return ns


class StudioSVGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ns = load()

    def render_fixture(self, cmd, **kwargs):
        self.assertEqual(cmd[0], 'rsvg-convert')
        self.assertEqual(Path(cmd[-1]).read_bytes(), SVG)
        image = Image.new('RGBA', (200, 100))
        image.paste((255, 0, 0, 255), (0, 0, 100, 100))
        image.save(cmd[cmd.index('--output') + 1], 'PNG')
        return SimpleNamespace(returncode=0)

    def test_detects_xml_variants_without_accepting_html(self):
        detect = self.ns['_image_bytes_are_svg']
        for data in (SVG, b'<!--' + b'x' * 5000 + b'-->' + SVG,
                     SVG.decode().encode('utf-16'),
                     b'<s:svg xmlns:s="http://www.w3.org/2000/svg"/>'):
            self.assertTrue(detect(data))
        self.assertFalse(detect(b'<html><svg/></html>'))
        self.assertFalse(detect(b'not an image'))

    def test_dedicated_svg_renderer_preserves_alpha_and_aspect(self):
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'logo.png'
            with patch('subprocess.run', side_effect=self.render_fixture) as run:
                self.assertTrue(self.ns['_normalise_studio_logo_to_png'](SVG, dest, 100))
            self.assertEqual(run.call_count, 1)
            with Image.open(dest) as image:
                self.assertEqual(image.format, 'PNG')
                self.assertEqual(image.size, (100, 50))
                self.assertEqual(image.getpixel((99, 25))[3], 0)
                self.assertEqual(image.getpixel((0, 25)), (255, 0, 0, 255))

    @unittest.skipUnless(shutil.which('magick') or shutil.which('convert'), 'ImageMagick unavailable')
    def test_real_svg_conversion_when_rsvg_binary_is_missing(self):
        run = subprocess.run
        def without_rsvg(cmd, **kwargs):
            if cmd[0] == 'rsvg-convert':
                raise FileNotFoundError(cmd[0])
            return run(cmd, **kwargs)
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'logo.png'
            with patch('subprocess.run', side_effect=without_rsvg):
                self.assertTrue(self.ns['save_image_bytes_optimized'](SVG, dest, max_edge=100))
            with Image.open(dest) as image:
                self.assertEqual(image.size, (100, 50))
                self.assertEqual(image.getpixel((99, 25))[3], 0)

    def test_raster_logos_still_convert_without_external_tools(self):
        for fmt in ('PNG', 'JPEG', 'WEBP'):
            with self.subTest(format=fmt), tempfile.TemporaryDirectory() as directory:
                source = BytesIO()
                Image.new('RGB', (200, 100), 'red').save(source, format=fmt)
                dest = Path(directory) / 'logo.png'
                with patch('subprocess.run') as run:
                    self.assertTrue(self.ns['_normalise_studio_logo_to_png'](source.getvalue(), dest, 100))
                run.assert_not_called()
                with Image.open(dest) as image:
                    self.assertEqual(image.format, 'PNG')
                    self.assertEqual(image.size, (100, 50))

    def test_embedded_png_uses_standard_mime_and_bounded_dimensions(self):
        buffer = BytesIO()
        Image.new('RGBA', (5000, 2500), 'red').save(buffer, format='PNG')
        svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10000 5000">'
               '<image x="120" y="60" width="9800" height="4800" href="data:img/png;base64,'
               + base64.b64encode(buffer.getvalue()).decode() + '"/></svg>').encode()
        result = ET.fromstring(self.ns['_prepare_studio_svg'](svg, 300))
        embedded = result[0]
        self.assertEqual(embedded.get('x'), '120')
        self.assertEqual(embedded.get('width'), '9800')
        self.assertTrue(embedded.get('href').startswith('data:image/png;base64,'))
        with Image.open(BytesIO(base64.b64decode(embedded.get('href').split(',')[1]))) as im:
            self.assertEqual(im.size, (2048, 1024))
            self.assertEqual(im.getpixel((100, 100)), (255, 0, 0, 255))

    def test_blank_renderer_result_tries_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'logo.png'
            def render(cmd, **kwargs):
                if cmd[0] == 'rsvg-convert':
                    Image.new('RGBA', (200, 100)).save(cmd[cmd.index('--output') + 1])
                else:
                    Image.new('RGBA', (200, 100), 'red').save(cmd[-1][6:])
                return SimpleNamespace(returncode=0)
            with patch('subprocess.run', side_effect=render) as run:
                self.assertTrue(self.ns['_normalise_studio_logo_to_png'](SVG, dest))
            self.assertEqual(run.call_count, 2)
            with Image.open(dest) as im:
                self.assertEqual(im.getpixel((0, 0)), (255, 0, 0, 255))

    def test_failed_conversion_keeps_existing_logo(self):
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'logo.png'
            Image.new('RGBA', (10, 10), 'blue').save(dest)
            original = dest.read_bytes()
            def broken_renderer(cmd, **kwargs):
                output = cmd[cmd.index('--output') + 1] if cmd[0] == 'rsvg-convert' else cmd[-1][6:]
                Path(output).write_bytes(b'broken output')
                return SimpleNamespace(returncode=0)
            with patch('subprocess.run', side_effect=broken_renderer):
                self.assertFalse(self.ns['_normalise_studio_logo_to_png'](SVG, dest))
            self.assertEqual(dest.read_bytes(), original)
            self.assertEqual(list(Path(directory).iterdir()), [dest])

    def test_studio_picker_downloads_and_saves_png_and_thumbnail(self):
        for save_local in (True, False):
            with self.subTest(save_local=save_local), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                folder = root / 'studio'
                folder.mkdir()
                def metadata(kind, row_id):
                    path = root / 'metadata' / kind / str(row_id)
                    path.mkdir(parents=True, exist_ok=True)
                    return path
                response = MagicMock(content=SVG)
                response.headers = {}
                response.iter_content.return_value = [SVG]
                response.__enter__.return_value = response
                requests = SimpleNamespace(get=Mock(return_value=response), RequestException=RuntimeError)
                db = SimpleNamespace(
                    favourite_get=lambda _: {'kind': 'studio', 'path': str(folder), 'folder_name': 'Fixture'},
                    favourite_update_matches=Mock(), studio_logo_slug=lambda _: 'fixture',
                    studio_logo_upsert=Mock())
                ns = self.ns
                overrides = dict(requests=requests, db=db,
                    _studio_logos_dir=lambda: root / 'logos', _studio_logo_filename=lambda _: 'fixture.png',
                    _metadata_dir_for=metadata, _sync_entity_library_images_safe=Mock(),
                    favourites_clear_image_disk_cache=Mock(), emit=Mock())
                with patch.dict(ns, overrides), patch('bounded_images.requests.get', requests.get), patch('subprocess.run', side_effect=self.render_fixture) as run:
                    result = asyncio.run(ns['api_favourites_apply_image']({
                        'row_id': 1, 'image_url': 'https://example.test/logo.svg', 'save_local': save_local}))
                self.assertEqual(result['ok'], True)
                requests.get.assert_called_once()
                response.raise_for_status.assert_called_once()
                self.assertEqual(run.call_count, 1)
                with Image.open(metadata('studio', 1) / '1_logo.webp') as thumb:
                    self.assertEqual(thumb.format, 'WEBP')
                    self.assertEqual(thumb.getpixel((199, 50))[3], 0)
                if save_local:
                    self.assertTrue((folder / 'logo.png').read_bytes().startswith(PNG_MAGIC))
                    self.assertTrue((root / 'logos' / 'fixture.png').read_bytes().startswith(PNG_MAGIC))
                    self.assertIn('/folder-logo?', result['image_url'])
                else:
                    self.assertFalse((folder / 'logo.png').exists())


if __name__ == '__main__':
    unittest.main()
