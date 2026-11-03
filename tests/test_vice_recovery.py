import json
import unittest
from unittest.mock import patch
import database as db
import test_library_recovery as fixtures
from app.core import vice_recovery as recovery
from app.core.queue_recovery import restore, store


class ViceRecoveryTests(unittest.TestCase):
    def setUp(self):
        fixtures.LibraryRecoveryTests.setUp(self)
        self.vice = {'id': '1', 'name': 'Example', 'locked': True, 'tags': [{'id': '7', 'name': 'Tag'}]}
        db.save_settings({'vices_dir': str(self.root), 'vices_json': json.dumps([self.vice]),
                          'monitored_tags': json.dumps([{'id': '7', 'name': 'Tag', 'stashdb_id': 'enriched'}])})
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def test_restore_preserves_identity_tags_and_new_vices(self):
        result = recovery.archive('1', self.folder)
        self.assertFalse(self.folder.exists())
        self.assertEqual(json.loads(db.get_settings()['vices_json']), [])
        other = {'id': '2', 'name': 'New', 'tags': []}
        db.save_settings({'vices_json': json.dumps([other])})
        restore(result['recovery_id'])
        self.assertEqual(json.loads(db.get_settings()['vices_json']), [self.vice, other])
        self.assertEqual(json.loads(db.get_settings()['monitored_tags'])[0]['stashdb_id'], 'enriched')
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertTrue(self.empty.is_dir())

    def test_reused_identity_blocks_restore(self):
        result = recovery.archive('1', self.folder)
        db.save_settings({'vices_json': json.dumps([{'id': '1', 'name': 'Other'}])})
        with self.assertRaises(ValueError): restore(result['recovery_id'])
        self.assertFalse(self.folder.exists())
        self.assertEqual(store().list()[0]['state'], 'archived')

    def test_failed_settings_write_rolls_back_files_and_index(self):
        with patch.object(recovery, '_write', side_effect=OSError('settings failed')):
            with self.assertRaises(OSError): recovery.archive('1', self.folder)
        self.assertEqual(self.video.read_bytes(), b'video')
        self.assertEqual(json.loads(db.get_settings()['vices_json']), [self.vice])
        with db.get_conn() as conn:
            self.assertIsNotNone(conn.execute('SELECT id FROM library_files WHERE id=?', (self.file_id,)).fetchone())

    def test_partial_blocks_before_settings_change(self):
        (self.empty/'unfinished.part').touch()
        with self.assertRaises(ValueError): recovery.archive('1', self.folder)
        self.assertEqual(json.loads(db.get_settings()['vices_json']), [self.vice])
        self.assertTrue(self.video.exists())
