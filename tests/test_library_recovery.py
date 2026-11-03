import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database as db
from app.core import library_recovery as recovery
from app.core.queue_recovery import restore, store


class LibraryRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.base = Path(self.tmp.name)
        self.old = db.DB_PATH; db.close_thread_connection(); db.DB_PATH = self.base/'state.db'
        self.cache = patch.object(db, '_settings_cache', None); self.cache.start()
        db.init_db()
        self.root = self.base/'library'; self.root.mkdir()
        db.save_settings({'series_dir': str(self.root)})
        self.folder = self.root/'Studio_%'; self.folder.mkdir()
        self.empty = self.folder/'empty'; self.empty.mkdir()
        self.video = self.folder/'episode.mp4'; self.video.write_bytes(b'video')
        (self.folder/'poster.webp').write_bytes(b'poster')
        with db.get_conn() as conn:
            self.row_id = conn.execute("INSERT INTO favourite_entities(kind,folder_name,path,match_tpdb_id) VALUES('studio','Example',?,'studio-id')", (str(self.folder),)).lastrowid
            self.file_id = conn.execute("INSERT INTO library_files(filename_stem,current_filename,destination,library_root) VALUES('episode','episode.mp4',?,?)", (str(self.video), str(self.root))).lastrowid
            conn.execute("INSERT INTO performer_bio_fields(row_id,field,value,edited) VALUES(?,'country','United Kingdom',1)", (self.row_id,))
            conn.commit()

    def tearDown(self):
        db.close_thread_connection(); db.DB_PATH = self.old; self.cache.stop(); self.tmp.cleanup()

    def test_round_trip_preserves_records_files_and_empty_directories(self):
        with db.get_conn() as conn:
            original = dict(conn.execute('SELECT * FROM favourite_entities WHERE id=?', (self.row_id,)).fetchone())
            indexed = dict(conn.execute('SELECT * FROM library_files WHERE id=?', (self.file_id,)).fetchone())
        result = recovery.archive(self.row_id)
        self.assertFalse(self.folder.exists()); self.assertIsNone(db.favourite_get(self.row_id))
        with db.get_conn() as conn:
            self.assertIsNone(conn.execute('SELECT * FROM performer_bio_fields WHERE row_id=?', (self.row_id,)).fetchone())
        self.assertEqual(result['retained_bytes'], 11)
        self.assertEqual(store().list()[0]['filename'], 'Example')
        restore(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertTrue(self.empty.is_dir())
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT value FROM performer_bio_fields WHERE row_id=?', (self.row_id,)).fetchone()[0], 'United Kingdom')
            self.assertEqual(dict(conn.execute('SELECT * FROM favourite_entities WHERE id=?', (self.row_id,)).fetchone()), original)
            self.assertEqual(dict(conn.execute('SELECT * FROM library_files WHERE id=?', (self.file_id,)).fetchone()), indexed)

    def test_occupied_restore_preserves_new_files(self):
        result = recovery.archive(self.row_id)
        self.folder.mkdir(); self.video.write_bytes(b'new')
        with self.assertRaises(ValueError): restore(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'new')
        self.assertEqual(store().list()[0]['state'], 'archived')

    def test_part_blocks_before_any_changes(self):
        (self.empty/'unfinished.part').touch()
        with self.assertRaises(ValueError): recovery.archive(self.row_id)
        self.assertTrue(self.video.exists()); self.assertIsNotNone(db.favourite_get(self.row_id))
        self.assertEqual(store().list(), [])

    def test_new_arrival_rolls_bundle_back_without_removing_arrival(self):
        real = Path.rmdir
        def remove(path):
            if path == self.folder:
                (path/'new.mp4').write_bytes(b'new')
            return real(path)
        with patch.object(Path, 'rmdir', remove), self.assertRaises(OSError):
            recovery.archive(self.row_id)
        self.assertEqual(self.video.read_bytes(), b'video')
        self.assertEqual((self.folder/'new.mp4').read_bytes(), b'new')
        self.assertTrue(self.empty.exists()); self.assertIsNotNone(db.favourite_get(self.row_id))

    def test_empty_folder_can_be_restored(self):
        self.video.unlink(); (self.folder/'poster.webp').unlink()
        result = recovery.archive(self.row_id)
        restore(result['recovery_id'])
        self.assertTrue(self.empty.is_dir()); self.assertEqual(result['retained_bytes'], 0)

    def test_literal_prefix_does_not_remove_sibling_index(self):
        sibling = self.root/'Studio_XX'; sibling.mkdir()
        with db.get_conn() as conn:
            other = conn.execute("INSERT INTO library_files(filename_stem,current_filename,destination) VALUES('x','x.mp4',?)", (str(sibling/'x.mp4'),)).lastrowid
            conn.commit()
        recovery.archive(self.row_id)
        with db.get_conn() as conn:
            self.assertIsNotNone(conn.execute('SELECT id FROM library_files WHERE id=?', (other,)).fetchone())

    def test_purge_retains_new_original_folder(self):
        result = recovery.archive(self.row_id)
        self.folder.mkdir(); self.video.write_bytes(b'new')
        store().purge(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'new')
        self.assertEqual(store().list(), [])

    def test_failed_restore_rolls_files_back_and_can_retry(self):
        result = recovery.archive(self.row_id)
        with patch.object(recovery, '_insert', side_effect=OSError('database failed')):
            with self.assertRaises(OSError): restore(result['recovery_id'])
        self.assertFalse(self.folder.exists())
        self.assertIsNone(db.favourite_get(self.row_id))
        self.assertEqual(store().list()[0]['state'], 'archived')
        restore(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'video')

    def test_conflicting_record_blocks_restore_before_moving(self):
        result = recovery.archive(self.row_id)
        with db.get_conn() as conn:
            conn.execute("INSERT INTO favourite_entities(id,kind,folder_name,path) VALUES(?,'studio','New',?)", (self.row_id, str(self.root/'new')))
            conn.commit()
        with self.assertRaises(ValueError): restore(result['recovery_id'])
        self.assertFalse(self.folder.exists())
        self.assertEqual(store().list()[0]['state'], 'archived')

    def test_nested_entity_is_not_silently_removed(self):
        with db.get_conn() as conn:
            conn.execute("INSERT INTO favourite_entities(kind,folder_name,path) VALUES('studio','Child',?)", (str(self.empty),))
            conn.commit()
        with self.assertRaises(ValueError): recovery.archive(self.row_id)
        self.assertEqual(self.video.read_bytes(), b'video')
        self.assertIsNotNone(db.favourite_get(self.row_id))
        self.assertEqual(store().list(), [])
