"""Isolated regression tests: no app startup, database or network required."""
import ast
import hashlib
import json
import logging
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock
from PIL import Image

SOURCE = Path(__file__).resolve().parents[1] / 'main.py'

def load_functions(*names, **environment):
    tree = ast.parse(SOURCE.read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert len(nodes) == len(names)
    ns = dict(Path=Path, Image=Image, hashlib=hashlib, json=json, threading=threading,
              _grid_thumb_locks=[threading.Lock() for _ in range(32)],
              _downloads_encoding_lock=threading.Lock(), _downloads_encoding=(None,None,None),
              _pil_normalize_for_output=lambda im: im.copy(), _log=logging.getLogger(__name__))
    ns.update(environment)
    exec(compile(ast.Module(body=nodes,type_ignores=[]), str(SOURCE),'exec'), ns)
    return ns

class PerformanceTests(unittest.TestCase):
    def test_thumbnail_reuses_revision_and_preserves_original(self):
        ns=load_functions('_performer_grid_thumbnail')
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'headshot.jpg'
            Image.new('RGB',(1200,1600),'red').save(source)
            original=source.read_bytes()
            result=ns['_performer_grid_thumbnail'](source,1)
            with Image.open(result) as im: self.assertEqual(im.size,(360,480))
            self.assertEqual(source.read_bytes(),original)
            self.assertEqual(ns['_performer_grid_thumbnail'](source,1),result)
            Image.new('RGB',(1200,1600),'blue').save(source)
            self.assertNotEqual(ns['_performer_grid_thumbnail'](source,1),result)
            self.assertEqual(len(list(Path(d).glob('*grid*'))),1)

    def test_download_encoding_shared_until_snapshot_changes(self):
        warm=Mock();warm.get.return_value={'items':[1]}
        encoder=Mock();encoder.dumps.side_effect=json.dumps
        ns=load_functions('_encoded_downloads_snapshot',_downloads_warm=warm,json=encoder)
        first=ns['_encoded_downloads_snapshot']()
        self.assertEqual(ns['_encoded_downloads_snapshot'](),first)
        self.assertEqual(encoder.dumps.call_count,1)
        warm.get.return_value={'items':[2]}
        self.assertNotEqual(ns['_encoded_downloads_snapshot'](),first)
        self.assertEqual(encoder.dumps.call_count,2)

    def test_backfill_completion_and_retry(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'headshot.jpg').write_bytes(b'image')
            db=Mock();db.favourite_list.return_value=[{'id':1}]
            thumb=Mock()
            ns=load_functions('_backfill_performer_grid_thumbnails',db=db,time=Mock(),
                _metadata_base_dir=lambda:root,_metadata_dir_for=lambda *args:root,
                _performers_iafd_read_meta=lambda rid:{'image_files':['headshot.jpg']},
                _performer_grid_thumbnail=thumb)
            thumb.side_effect=OSError('test failure')
            with self.assertLogs(level='ERROR'):ns['_backfill_performer_grid_thumbnails']()
            self.assertFalse((root/'.grid-thumbnails-480-v1.done').exists())
            thumb.side_effect=None
            ns['_backfill_performer_grid_thumbnails']()
            self.assertTrue((root/'.grid-thumbnails-480-v1.done').exists())
            count=thumb.call_count
            ns['_backfill_performer_grid_thumbnails']()
            self.assertEqual(thumb.call_count,count)

    def test_shared_empty_metadata_does_not_read_again(self):
        reader=Mock(side_effect=AssertionError('duplicate metadata read'))
        ns=load_functions('_favourite_row_tile_country','_favourite_row_tile_gender',
            _performers_iafd_read_meta=reader,_bio_country_from_popup_cache=lambda k:'',
            _bio_gender_from_popup_cache=lambda k:'')
        self.assertEqual(ns['_favourite_row_tile_country']({'id':1},{}),'')
        self.assertEqual(ns['_favourite_row_tile_gender']({'id':1},{}),'')
        reader.assert_not_called()

if __name__=='__main__':unittest.main()
