import errno
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database as db
from app.core import queue_recovery as queue
from app.core.recovery import RecoveryStore, recover_interrupted
from app.utils.fs import move_exclusive


class QueueRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.inbox = self.root / 'inbox'
        self.inbox.mkdir()
        cache = patch.object(db, "_settings_cache", None)
        cache.start(); self.addCleanup(cache.stop)
        db.close_thread_connection()
        old = db.DB_PATH
        db.DB_PATH = self.root / 'state.sqlite'
        self.addCleanup(setattr, db, 'DB_PATH', old)
        self.addCleanup(db.close_thread_connection)
        db.init_db()
        with db.get_conn() as conn:
            for key in ('source_dir', 'movies_source_dir', 'jav_download_watch_dir'):
                conn.execute('UPDATE settings SET value=? WHERE key=?', (str(self.inbox), key))
            conn.commit()

    def video(self, name='clip.mp4'):
        path = self.inbox / name
        path.write_bytes(b'original video')
        path.with_suffix('.nfo').write_text('original metadata')
        return path

    def archive(self, video, filename=None, table='processed_files', key=None, kind='scene'):
        return queue.archive(filename or video.name, video, self.inbox, table, key or video.name, kind)

    def test_scene_restore_retains_identity_original_status_sidecars_and_history(self):
        video = self.video()
        db.upsert_file(video.name)
        db.update_file(video.name, status='unmatched')
        before = db.file_identity(video.name)
        result = self.archive(video)
        self.assertFalse(video.exists())
        self.assertFalse(video.with_suffix('.nfo').exists())
        self.assertEqual(queue.store().list()[0]['state'], 'archived')
        restored = queue.restore(result['recovery_id'])
        self.assertEqual(restored['file_id'], before['id'])
        self.assertEqual(video.read_bytes(), b'original video')
        self.assertEqual(video.with_suffix('.nfo').read_text(), 'original metadata')
        self.assertEqual(db.file_identity(video.name)['id'], before['id'])
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT status FROM processed_files').fetchone()[0], 'unmatched')
        self.assertEqual(queue.store().list(), [])
        with self.assertRaises(ValueError):
            queue.restore(result['recovery_id'])

    def test_movie_and_jav_restore_after_history_purge_keep_original_id(self):
        for kind, filename, key in [('movie','clip.mp4','clip.mp4'), ('scene','JAV/clip.mp4','jav://clip.mp4')]:
            with self.subTest(kind=kind):
                video = self.video()
                db.upsert_movie(key)
                original = db.file_identity(key, 'processed_movies')
                result = self.archive(video, filename, 'processed_movies', key, kind)
                with db.get_conn() as conn:
                    conn.execute('DELETE FROM processed_movies WHERE filename=?', (key,))
                    conn.commit()
                queue.restore(result['recovery_id'])
                self.assertEqual(db.file_identity(key, 'processed_movies')['id'], original['id'])
                self.assertTrue(video.is_file())
                video.unlink(); video.with_suffix('.nfo').unlink()

    def test_restore_conflict_preserves_new_file_and_retained_original(self):
        video = self.video()
        result = self.archive(video)
        video.write_bytes(b'new occupant')
        with self.assertRaises(FileExistsError):
            queue.restore(result['recovery_id'])
        self.assertEqual(video.read_bytes(), b'new occupant')
        self.assertEqual(queue.store().list()[0]['state'], 'archived')
        video.unlink()
        queue.restore(result['recovery_id'])
        self.assertEqual(video.read_bytes(), b'original video')

    def test_partial_directory_is_preserved_but_unrelated_root_part_does_not_block(self):
        video = self.video()
        (self.inbox / 'other.part').touch()
        result = self.archive(video)
        self.assertTrue((self.inbox / 'other.part').exists())
        queue.restore(result['recovery_id'])
        nested = self.inbox / 'album'; nested.mkdir()
        child = nested / 'clip.mp4'; child.touch()
        (nested / 'active.part').touch()
        with self.assertRaises(ValueError):
            self.archive(child, 'album/clip.mp4', key='album/clip.mp4')
        self.assertTrue(child.exists())
        self.assertTrue((nested / 'active.part').exists())

    def test_symlink_parent_and_changed_inbox_block_restore(self):
        folder = self.inbox / 'album'; folder.mkdir()
        video = self.video('album/clip.mp4')
        result = self.archive(video, 'album/clip.mp4', key='album/clip.mp4')
        folder.rmdir()
        elsewhere = self.root / 'elsewhere'; elsewhere.mkdir()
        folder.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(ValueError):
            queue.restore(result['recovery_id'])
        self.assertEqual(list(elsewhere.iterdir()), [])

    def test_stable_id_does_not_follow_old_filename_to_a_different_file(self):
        db.upsert_file('old.mp4')
        asset = db.file_identity('old.mp4')
        db.rename_file('old.mp4', 'renamed.mp4')
        db.upsert_file('old.mp4')
        self.assertEqual(queue.resolve_target(asset['id'], 'old.mp4'), ('renamed.mp4', 'scene'))
        with db.get_conn() as conn:
            conn.execute("DELETE FROM processed_files WHERE filename='renamed.mp4'"); conn.commit()
        with self.assertRaises(ValueError):
            queue.resolve_target(asset['id'], 'old.mp4')

    def test_reused_removed_path_gets_a_new_identity_and_job(self):
        video = self.video()
        db.upsert_file(video.name)
        old = db.file_identity(video.name)
        db.processing_job_enqueue(video.name)
        result = self.archive(video)
        video.write_bytes(b'new download')
        db.upsert_file(video.name)
        fresh = db.file_identity(video.name)
        self.assertNotEqual(fresh['id'], old['id'])
        db.processing_job_enqueue(video.name)
        self.assertEqual(db.processing_job_start(video.name), fresh['id'])
        with self.assertRaises(ValueError):
            queue.resolve_target(old['id'], video.name)
        with self.assertRaises(ValueError):
            queue.restore(result['recovery_id'])
        self.assertEqual(video.read_bytes(), b'new download')
        self.assertEqual(queue.store().list()[0]['file_id'], old['id'])

    def test_sidecar_failure_rolls_back_all_completed_moves(self):
        video = self.video()
        calls = []
        def move(src, dst, **kwargs):
            calls.append((src, dst))
            if len(calls) == 2:
                raise OSError('simulated full disk')
            move_exclusive(src, dst, **kwargs)
        store = RecoveryStore(db.file_operation_connection, self.root / 'recover', move)
        with self.assertRaises(OSError):
            store.archive([video, video.with_suffix('.nfo')], 'asset', {'filename':video.name}, lambda *a:None)
        self.assertEqual(video.read_bytes(), b'original video')
        self.assertEqual(video.with_suffix('.nfo').read_text(), 'original metadata')
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT state FROM file_recovery').fetchone()[0], 'rolled_back')

    def test_replaced_source_is_not_removed_after_identity_validation(self):
        video = self.video()
        def replace_before_move(src, dst, **kwargs):
            replacement = src.with_suffix('.new')
            replacement.write_bytes(b'new occupant')
            replacement.replace(src)
            move_exclusive(src, dst, **kwargs)
        store = RecoveryStore(db.file_operation_connection, self.root / 'recover', replace_before_move)
        with self.assertRaises(OSError):
            store.archive([video], 'asset', {'filename':video.name}, lambda *a:None)
        self.assertEqual(video.read_bytes(), b'new occupant')

    def test_commit_failure_restores_files_and_rolls_back_queue_status(self):
        video = self.video()
        db.upsert_file(video.name)
        def fail(conn, context):
            conn.execute("UPDATE processed_files SET status='removed'")
            raise sqlite3.OperationalError('simulated commit preparation failure')
        with self.assertRaises(sqlite3.OperationalError):
            queue.store().archive([video, video.with_suffix('.nfo')], 'asset', {'filename':video.name}, fail)
        self.assertEqual(video.read_bytes(), b'original video')
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT status FROM processed_files').fetchone()[0], 'pending')

    def test_cursor_stays_valid_when_an_earlier_page_is_restored(self):
        first = self.archive(self.video('first.mp4'))
        second = self.archive(self.video('second.mp4'))
        page = queue.store().list(limit=1)
        cursor = page[0]['created_at'] + '|' + page[0]['id']
        queue.restore(page[0]['id'])
        next_page = queue.store().list(limit=1, before=cursor)
        self.assertEqual(len(next_page), 1)
        self.assertNotEqual(next_page[0]['id'], page[0]['id'])

    def test_shared_sidecar_stays_with_remaining_video(self):
        video = self.video()
        (self.inbox / 'clip.mkv').write_bytes(b'remaining video')
        result = self.archive(video)
        self.assertEqual(video.with_suffix('.nfo').read_text(), 'original metadata')
        queue.restore(result['recovery_id'])
        self.assertEqual(video.read_bytes(), b'original video')

    def test_purge_only_removes_retained_files_not_new_source_occupant(self):
        video = self.video()
        result = self.archive(video)
        video.write_bytes(b'new occupant')
        response = queue.store().purge(result['recovery_id'])
        self.assertGreater(response['freed_bytes'], 0)
        self.assertEqual(video.read_bytes(), b'new occupant')
        self.assertEqual(queue.store().list(), [])
        with self.assertRaises(ValueError):
            queue.restore(result['recovery_id'])

    def test_failed_purge_can_retry_but_cannot_restore_incomplete_bundle(self):
        video = self.video()
        result = self.archive(video)
        original_unlink = Path.unlink
        failed = []
        def unlink(path, *args, **kwargs):
            if path.name == '0001' and not failed:
                failed.append(path)
                raise PermissionError('simulated permission failure')
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', unlink):
            with self.assertRaises(PermissionError):
                queue.store().purge(result['recovery_id'])
        self.assertEqual(queue.store().list()[0]['state'], 'purge_failed')
        with self.assertRaises(ValueError):
            queue.restore(result['recovery_id'])
        self.assertTrue(queue.store().purge(result['recovery_id'])['ok'])
        self.assertEqual(queue.store().list(), [])

    def test_interrupted_operations_are_never_replayed(self):
        with db.get_conn() as conn:
            conn.execute("INSERT INTO file_recovery VALUES('id','asset','removing','{}',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
            recover_interrupted(conn); conn.commit()
            self.assertEqual(conn.execute('SELECT state FROM file_recovery').fetchone()[0], 'blocked')


class ExclusiveMoveTests(unittest.TestCase):
    def test_publish_race_never_overwrites_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, destination = Path(tmp)/'source', Path(tmp)/'destination'
            source.write_bytes(b'original'); destination.write_bytes(b'incumbent')
            with self.assertRaises(FileExistsError):
                move_exclusive(source, destination)
            self.assertEqual(source.read_bytes(), b'original')
            self.assertEqual(destination.read_bytes(), b'incumbent')

    def test_cross_device_copy_is_complete_before_source_removed(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            source, destination = Path(tmp)/'source', Path(tmp)/'destination'
            source.write_bytes(b'original')
            link = os.link
            calls = []
            def cross_device(src, dst, **kwargs):
                calls.append(src)
                if len(calls) == 1:
                    raise OSError(errno.EXDEV, 'cross device')
                self.assertEqual(source.read_bytes(), b'original')
                return link(src, dst, **kwargs)
            with patch('app.utils.fs.os.link', side_effect=cross_device):
                move_exclusive(source, destination)
            self.assertEqual(destination.read_bytes(), b'original')
            self.assertFalse(source.exists())
