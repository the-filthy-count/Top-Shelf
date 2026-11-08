import unittest
from unittest.mock import Mock
from app.core.fingerprint_selection import distance, select_scene, save_review

HASH = '0000000000000000'
def scene(sid, hash, algorithm='PHASH'):
    return {'id':sid, 'title':sid, 'performers':[{'performer':{'id':'p1','name':'Person'}}],
            'fingerprints':[{'algorithm':algorithm,'hash':hash}]}

class FingerprintSelectionTests(unittest.TestCase):
    def test_closest_not_first_and_near_still_eligible(self):
        far=scene('far','00000000000000ff');near=scene('near','0000000000000003')
        self.assertIs(select_scene([far,near],HASH),near)
        self.assertIs(select_scene([near,far],HASH),near)
        self.assertIs(select_scene([far],HASH),far)
    def test_tie_between_distinct_scenes_requires_review(self):
        self.assertIsNone(select_scene([scene('a','0000000000000001'),scene('b','0000000000000002')],HASH))
        self.assertIsNone(select_scene([scene('a',HASH),scene('b',HASH)],HASH))
    def test_repeated_scene_is_not_ambiguity(self):
        a=scene('a',HASH)
        self.assertIs(select_scene([a,a],HASH),a)
    def test_closest_of_all_phash_values_only(self):
        a=scene('a','ffffffffffffffff');a['fingerprints'] += [{'algorithm':'PHASH','hash':HASH}]
        self.assertEqual(distance(a,HASH),0)
        self.assertIsNone(distance(scene('a',HASH,'OSHASH'),HASH))
        self.assertIsNone(distance(scene('a','bad'),HASH))
    def test_missing_comparison_evidence_with_multiple_results(self):
        a=scene('a',HASH);b={'id':'b'}
        self.assertIsNone(select_scene([a,b],HASH))
        self.assertIs(select_scene([b],HASH),b)  # single source hit remains trusted
    def test_review_retains_all_cast_ids_and_source(self):
        db=Mock();rows=[scene('a',HASH),scene('b',HASH)]
        save_review(db,'clip.mp4',rows,'TPDB',HASH)
        db.update_file.assert_called_once_with('clip.mp4',status='unmatched')
        saved=db.queue_suggestions_add.call_args.args[1]
        self.assertEqual(len(saved),2)
        self.assertEqual(saved[0]['payload']['performers'][0]['performer']['id'],'p1')
        self.assertEqual(saved[0]['payload']['_source'],'TPDB')
        self.assertTrue(saved[0]['payload']['_fingerprint_review'])
