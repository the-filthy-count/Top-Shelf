import sqlite3
import unittest
from unittest.mock import patch
import database as db

class CustomLibraryLinkTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.execute('CREATE TABLE favourite_entities(id INTEGER PRIMARY KEY,custom_url TEXT)')
        self.conn.execute('INSERT INTO favourite_entities(id) VALUES(1)')
        self.conn.commit()
        self.patch=patch.object(db,'get_conn',return_value=self.conn);self.patch.start()
    def tearDown(self):
        self.patch.stop();self.conn.close()
    def test_save_replace_and_remove(self):
        for url in ('https://example.com/profile?a=1&b=2','https://example.org/other',''):
            db.favourite_set_custom_url(1,url)
            self.assertEqual(self.conn.execute('SELECT custom_url FROM favourite_entities').fetchone()[0],url or None)
    def test_invalid_links_leave_saved_value_intact(self):
        db.favourite_set_custom_url(1,'https://example.com')
        for url in ('javascript:alert(1)','file:///etc/passwd','https://','https://example.com/\nbad','https://user:pass@example.com',123):
            with self.assertRaises(ValueError):db.favourite_set_custom_url(1,url)
        self.assertEqual(self.conn.execute('SELECT custom_url FROM favourite_entities').fetchone()[0],'https://example.com')
    def test_missing_entry(self):
        with self.assertRaises(ValueError):db.favourite_set_custom_url(99,'https://example.com')
