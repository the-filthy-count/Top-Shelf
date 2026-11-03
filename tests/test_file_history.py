import sqlite3
import unittest
from app.core.file_history import install,record_operation,finish_operation
from app.core import jobs

class FileHistoryTests(unittest.TestCase):
    def setUp(self):
        self.c=sqlite3.connect(':memory:');self.c.row_factory=sqlite3.Row;self.addCleanup(self.c.close)
        for table in ('processed_files','processed_movies'):
            self.c.execute(f'CREATE TABLE {table}(id INTEGER PRIMARY KEY AUTOINCREMENT,filename TEXT,status TEXT)')
        self.c.execute("INSERT INTO processed_files(filename,status) VALUES('legacy.mp4','unmatched')")
        install(self.c);jobs.install(self.c)

    def test_identity_and_original_survive_rename_removal_and_migration(self):
        self.assertEqual(self.c.execute('SELECT original_known FROM file_assets').fetchone()[0],0)
        self.c.execute("INSERT INTO processed_files(filename,status) VALUES('arrival.mp4','pending')")
        before=dict(self.c.execute("SELECT * FROM file_assets WHERE original_path='arrival.mp4'").fetchone())
        self.c.execute("UPDATE processed_files SET filename='Folder/clean.mp4',status='unmatched' WHERE filename='arrival.mp4'")
        self.c.execute("DELETE FROM processed_files WHERE filename='Folder/clean.mp4'")
        install(self.c)
        after=dict(self.c.execute('SELECT * FROM file_assets WHERE id=?',(before['id'],)).fetchone())
        self.assertEqual(after['original_path'],'arrival.mp4');self.assertEqual(after['current_path'],'Folder/clean.mp4')
        events=[r[0] for r in self.c.execute('SELECT kind FROM file_events WHERE file_id=?',(before['id'],))]
        self.assertEqual(events,['registered','queue.status','queue.rename','queue.record_removed'])
        self.c.execute("INSERT INTO processed_files(filename,status) VALUES('arrival.mp4','pending')")
        self.assertEqual(self.c.execute("SELECT count(DISTINCT id) FROM file_assets WHERE original_path='arrival.mp4'").fetchone()[0],2)

    def test_movie_identity_is_separate_and_transactional(self):
        self.c.commit()
        self.c.execute("INSERT INTO processed_movies(filename,status) VALUES('legacy.mp4','pending')")
        self.assertEqual(self.c.execute('SELECT count(*) FROM file_assets').fetchone()[0],2)
        self.c.rollback()
        self.assertEqual(self.c.execute('SELECT count(*) FROM file_assets').fetchone()[0],1)

    def test_operation_failure_is_recorded(self):
        op=record_operation(lambda:self.c,'source','target',[1,2,3,4]);finish_operation(lambda:self.c,op,'disk full')
        row=self.c.execute('SELECT state,error FROM file_operations').fetchone()
        self.assertEqual(tuple(row),('failed','disk full'))

class PersistentJobsTests(unittest.TestCase):
    def setUp(self):
        self.c=sqlite3.connect(':memory:');self.addCleanup(self.c.close);jobs.install(self.c)
    def test_recovery_bounded_errors_and_partial_downloads(self):
        jobs.enqueue(self.c,'file');self.assertTrue(jobs.claim(self.c,'file'))
        self.assertFalse(jobs.claim(self.c,'file'))
        jobs.checkpoint(self.c,'file','Fingerprinting');jobs.recover(self.c)
        self.assertEqual(self.c.execute('SELECT state FROM processing_jobs').fetchone()[0],'pending')
        self.c.execute('UPDATE processing_jobs SET available_at=0');self.assertTrue(jobs.claim(self.c,'file'))
        jobs.finish(self.c,'file','deferred')
        self.assertEqual(self.c.execute('SELECT attempts FROM processing_jobs').fetchone()[0],1)
        for _ in range(2):
            self.c.execute('UPDATE processing_jobs SET available_at=0');self.assertTrue(jobs.claim(self.c,'file'));jobs.finish(self.c,'file','error','network')
        self.assertEqual(self.c.execute('SELECT state FROM processing_jobs').fetchone()[0],'failed')

    def test_late_worker_cannot_resurrect_cancelled_job(self):
        jobs.enqueue(self.c, 'file'); self.assertTrue(jobs.claim(self.c, 'file'))
        self.c.execute("UPDATE processing_jobs SET state='cancelled',phase='Removed'")
        jobs.finish(self.c, 'file', 'error', 'late worker failure')
        self.assertEqual(self.c.execute('SELECT state,phase FROM processing_jobs').fetchone(), ('cancelled', 'Removed'))

    def test_duplicate_completion_does_not_change_retry_budget(self):
        jobs.enqueue(self.c, 'file'); self.assertTrue(jobs.claim(self.c, 'file'))
        jobs.finish(self.c, 'file', 'deferred')
        before = self.c.execute('SELECT * FROM processing_jobs').fetchone()
        jobs.finish(self.c, 'file', 'error', 'duplicate callback')
        self.assertEqual(self.c.execute('SELECT * FROM processing_jobs').fetchone(), before)
