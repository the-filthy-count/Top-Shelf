import unittest
from unittest.mock import patch

import database as db
import test_library_recovery as fixtures
from app.core import video_recovery
from app.core.queue_recovery import store, restore


class VideoRecoveryTests(unittest.TestCase):
    setUp = fixtures.LibraryRecoveryTests.setUp
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def test_video_sidecar_and_index_round_trip(self):
        db.upsert_file('original.mp4')
        db.update_file('original.mp4', status='filed', destination=str(self.video))
        identity = db.file_identity('original.mp4')
        sidecar = self.video.with_suffix('.nfo'); sidecar.write_bytes(b'nfo')
        result = video_recovery.archive(self.video)
        self.assertFalse(self.video.exists()); self.assertFalse(sidecar.exists())
        self.assertTrue((self.folder/'poster.webp').exists())
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT status FROM processed_files WHERE filename=?', ('original.mp4',)).fetchone()[0], 'removed')
            self.assertIsNone(conn.execute('SELECT id FROM library_files WHERE id=?', (self.file_id,)).fetchone())
        restore(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'video'); self.assertEqual(sidecar.read_bytes(), b'nfo')
        self.assertEqual(db.file_identity('original.mp4')['id'], identity['id'])
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT status FROM processed_files WHERE filename=?', ('original.mp4',)).fetchone()[0], 'filed')
            self.assertIsNotNone(conn.execute('SELECT id FROM library_files WHERE id=?', (self.file_id,)).fetchone())

    def test_shared_sidecars_are_retained_beside_other_video(self):
        sibling = self.video.with_suffix('.mkv'); sibling.write_bytes(b'other')
        sidecar = self.video.with_suffix('.nfo'); sidecar.write_bytes(b'shared')
        result = video_recovery.archive(self.video)
        self.assertEqual(sidecar.read_bytes(), b'shared'); self.assertEqual(sibling.read_bytes(), b'other')
        restore(result['recovery_id']); self.assertEqual(self.video.read_bytes(), b'video')

    def test_reused_queue_record_prevents_unsafe_restore(self):
        db.upsert_file('original.mp4'); db.update_file('original.mp4', status='filed', destination=str(self.video))
        result = video_recovery.archive(self.video)
        db.upsert_file('original.mp4')
        with self.assertRaises(ValueError): restore(result['recovery_id'])
        self.assertFalse(self.video.exists()); self.assertEqual(store().list()[0]['state'], 'archived')

    def test_failed_index_restore_rolls_video_back(self):
        result = video_recovery.archive(self.video)
        with patch.object(video_recovery, '_insert', side_effect=OSError('database failed')):
            with self.assertRaises(OSError): restore(result['recovery_id'])
        self.assertFalse(self.video.exists())
        restore(result['recovery_id']); self.assertEqual(self.video.read_bytes(), b'video')

    def test_missing_video_does_not_delete_index(self):
        self.video.unlink()
        with self.assertRaises(ValueError): video_recovery.archive(self.video)
        with db.get_conn() as conn:
            self.assertIsNotNone(conn.execute('SELECT id FROM library_files WHERE id=?', (self.file_id,)).fetchone())

    def test_partial_download_blocks_removal(self):
        (self.folder/'other.part').touch()
        with self.assertRaises(ValueError): video_recovery.archive(self.video)
        self.assertTrue(self.video.exists())

    def test_occupied_destination_preserves_both_copies(self):
        result = video_recovery.archive(self.video)
        self.video.write_bytes(b'new')
        with self.assertRaises(FileExistsError): restore(result['recovery_id'])
        self.assertEqual(self.video.read_bytes(), b'new')
        self.assertEqual(store().list()[0]['state'], 'archived')

    def test_movie_record_status_and_identity_survive_restore(self):
        db.upsert_movie('movie.mp4')
        db.update_movie('movie.mp4', status='filed', destination=str(self.video))
        identity = db.file_identity('movie.mp4', 'processed_movies')
        result = video_recovery.archive(self.video)
        restore(result['recovery_id'])
        self.assertEqual(db.file_identity('movie.mp4', 'processed_movies')['id'], identity['id'])
        with db.get_conn() as conn:
            self.assertEqual(conn.execute("SELECT status FROM processed_movies WHERE filename='movie.mp4'").fetchone()[0], 'filed')

    def test_shared_root_partial_does_not_block_unrelated_video(self):
        loose = self.root/'loose.mp4'; loose.write_bytes(b'loose')
        (self.root/'other.part').touch()
        result = video_recovery.archive(loose)
        self.assertTrue((self.root/'other.part').exists())
        restore(result['recovery_id']); self.assertEqual(loose.read_bytes(), b'loose')

    def test_symlink_is_rejected_before_touching_target(self):
        link = self.folder/'alias.mp4'; link.symlink_to(self.video)
        with self.assertRaises(ValueError): video_recovery.archive(link)
        self.assertTrue(link.is_symlink()); self.assertEqual(self.video.read_bytes(), b'video')

    def test_library_identity_resolves_renamed_video_not_new_occupant(self):
        original = self.video
        renamed = self.folder/'renamed.mp4'; original.rename(renamed); original.write_bytes(b'new occupant')
        with db.get_conn() as conn:
            conn.execute('UPDATE library_files SET destination=? WHERE id=?', (str(renamed), self.file_id)); conn.commit()
        result = video_recovery.archive(original, library_file_id=self.file_id)
        self.assertEqual(original.read_bytes(), b'new occupant'); self.assertFalse(renamed.exists())
        restore(result['recovery_id']); self.assertEqual(renamed.read_bytes(), b'video')

    def test_missing_library_identity_never_falls_back_to_path(self):
        with self.assertRaises(ValueError): video_recovery.archive(self.video, library_file_id=999999)
        self.assertEqual(self.video.read_bytes(), b'video')
