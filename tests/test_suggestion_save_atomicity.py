import sqlite3
import unittest
import database as db
import test_suggestion_rule_versions as fixtures


class SuggestionSaveTests(unittest.TestCase):
    setUp = fixtures.SuggestionRuleVersionTests.setUp

    def test_rejected_after_search_snapshot_cannot_reappear(self):
        candidate = {'source_db':'TPDB', 'source_id':'one', 'score':.9}
        db.queue_suggestions_add('file.mp4', [candidate])
        self.assertEqual(db.queue_suggestion_rejects_for_file('file.mp4'), set())
        db.queue_suggestions_reject('file.mp4', 'TPDB', 'one')
        for replace in (False, True):
            self.assertEqual(db.queue_suggestions_add('file.mp4', [candidate], replace=replace), 0)
            self.assertEqual(db.queue_suggestions_for_file('file.mp4'), [])

    def test_failed_replacement_rolls_back_delete_and_inserts(self):
        db.queue_suggestions_add('file.mp4', [{'source_db':'TPDB','source_id':'old'}])
        with db.get_conn() as conn:
            conn.execute("""CREATE TRIGGER fail_suggestion BEFORE INSERT ON queue_suggestions
                WHEN NEW.source_id='fail' BEGIN SELECT RAISE(ABORT, 'test failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            db.queue_suggestions_add('file.mp4', [
                {'source_db':'TPDB','source_id':'new'},
                {'source_db':'TPDB','source_id':'fail'}], replace=True)
        self.assertEqual([r['source_id'] for r in db.queue_suggestions_for_file('file.mp4')], ['old'])
        db.queue_suggestions_add('file.mp4', [], replace=True)
        self.assertEqual(db.queue_suggestions_for_file('file.mp4'), [])
