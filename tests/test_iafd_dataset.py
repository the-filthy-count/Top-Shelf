import json
import lzma
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock
from app.core import iafd_dataset as dataset

class DatasetTests(unittest.TestCase):
    def test_exact_alias_search_fallback_and_failed_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)/'data.xz'
            target = Path(directory)/'data.sqlite'
            item = {'source_reference':'abc','name':'Jane Doe','tattoos':['unused'],'image_url':'https://example.com/headshot.jpg','aliases':['Other Alias'],
                    'urls':[{'site':'IAFD','url':'https://www.iafd.com/person.rme/id=abc'}]}
            with lzma.open(archive,'wt') as f: f.write(json.dumps(item)+'\n')
            self.assertEqual(dataset.import_file(archive,target),1)
            with patch.object(dataset,'path',return_value=target):
                self.assertTrue(dataset.current_index())
                for name in ['Jane Doe','janedoe','Other_Alias']:
                    self.assertEqual(dataset.search(name)[0]['id'],'abc')
                    self.assertEqual(dataset.search(name)[0]['thumb_url'],'https://example.com/headshot.jpg')
                self.assertEqual(dataset.search('Doe Jane'),[])
                self.assertEqual(dataset.search('Jane'),[])
                fallback = Mock(return_value=[{'name':'Live'}])
                dataset.search_with_fallback('Jane Doe',fallback)
                fallback.assert_not_called()
                self.assertEqual(dataset.search_with_fallback('Unknown',fallback),[{'name':'Live'}])
                archive.write_bytes(b'broken')
                with self.assertRaises(lzma.LZMAError): dataset.import_file(archive,target)
                self.assertEqual(dataset.search('Jane Doe')[0]['id'],'abc')
