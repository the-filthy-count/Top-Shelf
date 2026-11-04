import unittest
from pathlib import Path
from unittest.mock import patch
import database as db
from app.core import queue_records as records
import test_suggestion_rule_versions as fixtures


class QueueDetailsTests(unittest.TestCase):
    setUp = fixtures.SuggestionRuleVersionTests.setUp

    def create(self):
        db.upsert_file('Jane Doe Private Lesson.mp4')
        db.update_file('Jane Doe Private Lesson.mp4', status='unmatched', phash='abc123')
        with db.get_conn() as conn:
            conn.execute("INSERT INTO favourite_entities(kind,folder_name,path,match_stashdb_id) VALUES('performer','Jane Doe','/library/Jane Doe','person-id')")
        return records.read('Jane Doe Private Lesson.mp4')

    def test_corrections_survive_rename_reparse_and_preserve_fingerprint(self):
        original = self.create()
        fields = dict(original['fields'], title='Correct title', date='', scene_number=2, performers=['Jane Doe'], studio='')
        edited = records.save(original['filename'], original['file_id'], fields, original['revision'])
        self.assertEqual(edited['performer_links'][0]['source_ids'], {'StashDB':'person-id'})
        self.assertEqual(edited['technical']['phash'], 'abc123')
        db.rename_file(original['filename'], 'Different Name.mp4')
        current = records.read(file_id=original['file_id'])
        self.assertEqual(current['fields']['title'], 'Correct title')
        self.assertEqual(current['fields']['date'], '')
        self.assertEqual(current['fields']['scene_number'], 2)
        self.assertEqual(current['filename'], 'Different Name.mp4')
        self.assertEqual(current['technical']['status'], 'unmatched')
        self.assertEqual(records.saved_ids(current)['Jane Doe']['StashDB'], {'person-id'})

    def test_conflicting_edits_and_stale_search_results_are_rejected(self):
        original = self.create()
        edited = records.save(original['filename'], original['file_id'], dict(original['fields'], title='New'), 0)
        with self.assertRaises(ValueError):
            records.save(original['filename'], original['file_id'], original['fields'], 0)
        count = db.queue_suggestions_add(original['filename'], [{'source_db':'TPDB','source_id':'stale'}],
                                         replace=True, expected_record=original)
        self.assertEqual(count, 0)
        self.assertEqual(db.queue_suggestions_for_file(original['filename']), [])
        self.assertEqual(edited['revision'], 1)

    def test_reset_reparses_and_rejections_survive_edits(self):
        original = self.create()
        db.queue_suggestions_reject(original['filename'], 'TPDB', 'reject')
        changed = records.save(original['filename'], original['file_id'], dict(original['fields'], title='Correction'), 0)
        reset = records.save(changed['filename'], changed['file_id'], {}, changed['revision'], reset=True)
        self.assertFalse(reset['edited'])
        self.assertEqual(reset['fields'], original['fields'])
        self.assertIn(('TPDB','reject'), db.queue_suggestion_rejects_for_file(original['filename']))

    def test_media_observations_join_same_record_without_overwriting_fields(self):
        original = self.create()
        media = Path(self.tmp.name) / 'fixture.mp4'
        media.write_bytes(b'123456')
        records.observe_media(original['filename'], media, 'h264', 1920, 1080, 120)
        current = records.read(file_id=original['file_id'])
        self.assertEqual(current['technical']['media_size_bytes'], 6)
        self.assertEqual(current['technical']['media_width'], 1920)
        self.assertEqual(current['technical']['phash'], 'abc123')
        self.assertEqual(current['fields'], original['fields'])

    def test_invalid_dates_and_scene_numbers_are_not_saved(self):
        original = self.create()
        for fields in [dict(original['fields'], date='2026-02-30'), dict(original['fields'], scene_number=-1)]:
            with self.assertRaises(ValueError):
                records.save(original['filename'], original['file_id'], fields, 0)
        self.assertEqual(records.read(file_id=original['file_id'])['revision'], 0)

    def test_studio_identity_uses_saved_id_despite_name_difference(self):
        from app.core.suggestion_guard import bind_saved_identities
        from app.core.matching import _suggestion_identity_matches
        link = {'name':'Local studio', 'source_ids': {'StashDB':'studio-id'}}
        scene = {'_source':'StashDB', 'studio':{'id':'studio-id','name':'Canonical name'}, 'performers':[]}
        bound = bind_saved_identities(scene, [], {}, link)
        self.assertTrue(_suggestion_identity_matches([], 'Local studio', bound))
        scene['studio']['id'] = 'wrong'
        self.assertIsNone(bind_saved_identities(scene, [], {}, link))
