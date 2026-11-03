import unittest
from pathlib import Path
import database as db
import test_library_recovery as fixtures
from app.core.movie_jobs import run
from app.core.jobs import recover as recover_interrupted


class MovieJobsTests(unittest.TestCase):
    def setUp(self):
        fixtures.LibraryRecoveryTests.setUp(self)
        self.inbox = self.base/'inbox'; self.inbox.mkdir()
        self.video = self.inbox/'movie.mp4'; self.video.write_bytes(b'video')
        db.save_settings({'movies_source_dir': str(self.inbox)})
    tearDown = fixtures.LibraryRecoveryTests.tearDown

    def job(self, key='movie.mp4'):
        with db.get_conn() as conn:
            return dict(conn.execute('SELECT j.* FROM processing_jobs j JOIN file_assets a ON a.id=j.file_id JOIN processed_movies p ON p.id=a.queue_id WHERE a.queue_table=? AND p.filename=?', ('processed_movies', key)).fetchone())

    def test_success_and_durable_ready_query(self):
        db.upsert_movie('movie.mp4'); db.processing_job_enqueue('movie.mp4', 'processed_movies')
        self.assertEqual(db.processing_jobs_ready(table='processed_movies'), ['movie.mp4'])
        self.assertEqual(run('movie.mp4', None, None, lambda *a,**kw:{'status':'filed'})['status'], 'filed')
        self.assertEqual(self.job()['state'], 'complete')
        self.assertEqual(db.processing_jobs_ready(table='processed_movies'), [])

    def test_deferred_does_not_consume_attempt_and_is_not_reclaimed_early(self):
        run('movie.mp4', None, None, lambda *a,**kw:{'status':'deferred'})
        self.assertEqual(self.job()['attempts'], 0); self.assertEqual(self.job()['state'], 'pending')
        self.assertEqual(run('movie.mp4', None, None, lambda *a,**kw:self.fail('early retry'))['status'], 'busy')

    def test_restart_recovers_claim_and_bounds_failures(self):
        db.upsert_movie('movie.mp4')
        job_id = db.processing_job_start('movie.mp4', 'processed_movies')
        self.assertTrue(job_id)
        with db.get_conn() as conn:
            recover_interrupted(conn); conn.execute('UPDATE processing_jobs SET available_at=0'); conn.commit()
        def failed(*a, **kw): raise OSError('offline')
        for _ in range(2):
            with self.assertRaises(OSError): run('movie.mp4', None, None, failed)
            with db.get_conn() as conn:
                conn.execute('UPDATE processing_jobs SET available_at=0'); conn.commit()
        self.assertEqual(self.job()['attempts'], 3); self.assertEqual(self.job()['state'], 'failed')
        self.assertEqual(db.processing_jobs_ready(table='processed_movies'), [])

    def test_jav_identity_stays_distinct(self):
        db.save_settings({'jav_download_watch_dir': str(self.inbox)})
        run('movie.mp4', self.inbox, 'jav', lambda *a,**kw:{'status':'unmatched'})
        self.assertEqual(self.job('jav://movie.mp4')['state'], 'complete')
        self.assertIsNone(db.file_identity('movie.mp4', 'processed_movies'))
