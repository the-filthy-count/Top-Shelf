import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import database as db
from app.core import suggestion_rules

class SuggestionRuleVersionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        db.close_thread_connection();old=db.DB_PATH
        db.DB_PATH=Path(self.tmp.name)/'state.db'
        self.addCleanup(setattr,db,'DB_PATH',old);self.addCleanup(db.close_thread_connection)
        with sqlite3.connect(db.DB_PATH) as conn:
            conn.execute('CREATE TABLE queue_suggestion_attempts(filename TEXT PRIMARY KEY,attempted_at REAL NOT NULL)')
            conn.execute("INSERT INTO queue_suggestion_attempts VALUES('legacy.mp4',9999999999)")
        db.init_db()

    def test_legacy_attempt_migrates_and_retries_without_waiting(self):
        db.upsert_file('legacy.mp4');db.update_file('legacy.mp4',status='unmatched')
        self.assertIn('legacy.mp4',db.queue_suggestion_pending())
        db.queue_suggestion_mark_attempt('legacy.mp4')
        self.assertEqual(db.queue_suggestion_pending(),[])
        with patch.object(suggestion_rules,'RULES_VERSION',suggestion_rules.RULES_VERSION+1):
            self.assertIn('legacy.mp4',db.queue_suggestion_pending())
            db.queue_suggestion_mark_attempt('legacy.mp4')
            self.assertEqual(db.queue_suggestion_pending(),[])

    def test_existing_suggestions_and_nonunmatched_files_are_not_requeued(self):
        for name,status in [('saved.mp4','unmatched'),('done.mp4','filed'),('empty.mp4','unmatched')]:
            db.upsert_file(name);db.update_file(name,status=status);db.queue_suggestion_mark_attempt(name)
        db.queue_suggestion_finish_attempt('saved.mp4', True)
        db.queue_suggestions_add('saved.mp4',[{'source_db':'TPDB','source_id':'1','title':'Scene','score':.9,'payload':{'id':'1'}}])
        with patch.object(suggestion_rules,'RULES_VERSION',suggestion_rules.RULES_VERSION+1):
            self.assertEqual(db.queue_suggestion_pending(),['empty.mp4'])
        self.assertEqual(len(db.queue_suggestions_for_file('saved.mp4')),1)

    def test_partial_results_retry_without_losing_saved_candidates(self):
        db.upsert_file('partial.mp4')
        db.update_file('partial.mp4', status='unmatched')
        db.queue_suggestions_add('partial.mp4', [{'source_db':'TPDB','source_id':'1','score':.8}])
        with patch('time.time', return_value=1000):
            db.queue_suggestion_mark_attempt('partial.mp4')
            db.queue_suggestion_finish_attempt('partial.mp4', False)
        with patch('time.time', return_value=1299):
            self.assertNotIn('partial.mp4', db.queue_suggestion_pending())
        with patch('time.time', return_value=1300):
            self.assertIn('partial.mp4', db.queue_suggestion_pending())
        self.assertEqual(len(db.queue_suggestions_for_file('partial.mp4')), 1)
