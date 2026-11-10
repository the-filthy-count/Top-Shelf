import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import xml.etree.ElementTree as ET
from app.core import performer_creation as creation

class PerformerCreationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.conn=sqlite3.connect(':memory:')
        self.conn.execute('CREATE TABLE favourite_entities(id INTEGER PRIMARY KEY,kind TEXT,folder_name TEXT,library_name TEXT)')
        self.patch=patch.object(creation.db,'get_conn',return_value=self.conn)
        self.patch.start()
    def tearDown(self):
        self.patch.stop();self.conn.close();self.tmp.cleanup()
    def test_namesake_requires_confirmation_and_preserves_existing_files(self):
        original=creation.reserve_folder(self.root,'Jane Doe')
        (original/'tvshow.nfo').write_text('original')
        with self.assertRaises(creation.NameInUse):creation.reserve_folder(self.root,'Jane Doe')
        self.assertEqual(creation.reserve_folder(self.root,'Jane Doe',True).name,'Jane Doe (2)')
        self.assertEqual(creation.reserve_folder(self.root,'Jane Doe',True).name,'Jane Doe (3)')
        self.assertEqual((original/'tvshow.nfo').read_text(),'original')
    def test_name_collision_in_another_root_is_case_insensitive(self):
        self.conn.execute("INSERT INTO favourite_entities VALUES(1,'performer','JANE DOE',NULL)")
        with self.assertRaises(creation.NameInUse):creation.reserve_folder(self.root,'Jane Doe')
        self.assertFalse((self.root/'Jane Doe').exists())
        self.assertEqual(creation.reserve_folder(self.root,'Jane Doe',True).name,'Jane Doe (2)')
    def test_distinct_show_ids_keep_title_and_provider_ids(self):
        nfo='<tvshow><title>Jane Doe</title><uniqueid type="stashdb" default="true">abc</uniqueid></tvshow>'
        a,b=[ET.fromstring(creation.identify_show(nfo)) for _ in range(2)]
        self.assertNotEqual(a.find("uniqueid[@type='topshelf']").text,b.find("uniqueid[@type='topshelf']").text)
        self.assertEqual(a.findtext('title'),'Jane Doe')
        self.assertEqual(a.find("uniqueid[@type='stashdb']").text,'abc')
        self.assertEqual(len(a.findall("uniqueid[@default='true']")),1)
    def test_display_name_separate_from_folder_counter(self):
        self.conn.execute("INSERT INTO favourite_entities VALUES(1,'performer','Jane Doe (2)',NULL)")
        creation.preserve_name(1,'Jane Doe')
        self.assertEqual(self.conn.execute('SELECT folder_name,library_name FROM favourite_entities').fetchone(),('Jane Doe','Jane Doe'))
