import unittest
from unittest.mock import patch
import database as db
import test_library_recovery as fixtures
from app.core import filing_recovery
from app.core.queue_recovery import archive_pipeline, restore, store
from app.utils.fs import safe_move


class FilingRecoveryTests(unittest.TestCase):
    def setUp(self):
        fixtures.LibraryRecoveryTests.setUp(self)
        self.inbox = self.base/'inbox'; self.inbox.mkdir()
        self.source = self.inbox/'new.mp4'; self.source.write_bytes(b'better')
        db.save_settings({'source_dir': str(self.inbox)})
        self.nfo = self.video.with_suffix('.nfo'); self.nfo.write_bytes(b'old nfo')
    def tearDown(self):
        filing_recovery.finish(self.video)
        fixtures.LibraryRecoveryTests.tearDown(self)

    def test_upgrade_retains_incumbent_and_metadata(self):
        filing_recovery.replace(self.source, self.video)
        filing_recovery.finish(self.video)
        self.assertEqual(self.video.read_bytes(), b'better'); self.assertFalse(self.source.exists())
        self.assertFalse(self.nfo.exists())
        record = store().list()[0]
        self.assertEqual(record['state'], 'archived')
        with self.assertRaises(FileExistsError): restore(record['id'])
        self.assertEqual(self.video.read_bytes(), b'better')

    def test_move_failure_restores_incumbent_and_index(self):
        with patch.object(filing_recovery, 'safe_move', side_effect=OSError('full')):
            with self.assertRaises(OSError): filing_recovery.replace(self.source, self.video)
        self.assertEqual(self.source.read_bytes(), b'better')
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertEqual(self.nfo.read_bytes(), b'old nfo')
        with db.get_conn() as conn:
            self.assertIsNotNone(conn.execute('SELECT id FROM library_files WHERE id=?', (self.file_id,)).fetchone())

    def test_nfo_failure_can_restore_inbox_then_incumbent(self):
        filing_recovery.replace(self.source, self.video)
        safe_move(self.video, self.source, replace_existing=False)
        filing_recovery.rollback(self.video)
        self.assertEqual(self.source.read_bytes(), b'better')
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertEqual(self.nfo.read_bytes(), b'old nfo')

    def test_discarded_incoming_restores_pending_after_caller_files_duplicate(self):
        db.upsert_file('new.mp4'); db.update_file('new.mp4', status='processing')
        result = archive_pipeline(self.source, 'new.mp4', duplicate_destination=str(self.video))
        db.update_file('new.mp4', status='filed', destination=str(self.video))
        restore(result['recovery_id'])
        self.assertEqual(self.source.read_bytes(), b'better'); self.assertEqual(self.video.read_bytes(), b'video')
        with db.get_conn() as conn:
            row = conn.execute("SELECT status,destination FROM processed_files WHERE filename='new.mp4'").fetchone()
            self.assertEqual(row[0], 'pending'); self.assertFalse(row[1])

    def test_exclusive_move_preserves_competing_destination(self):
        with self.assertRaises(FileExistsError): safe_move(self.source, self.video, replace_existing=False)
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertEqual(self.source.read_bytes(), b'better')
