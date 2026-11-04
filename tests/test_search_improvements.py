import unittest
from unittest.mock import Mock, patch
import database as db
from app.core import matching
from app.core.filename_parser import parse_filename
from app.integrations.scene_search import search_joint_cast
import test_suggestion_rule_versions as fixtures


class SearchDatabaseTests(unittest.TestCase):
    setUp=fixtures.SuggestionRuleVersionTests.setUp

    def test_saved_library_ids_are_source_scoped_and_ambiguous_ids_stay_ambiguous(self):
        with db.get_conn() as conn:
            conn.execute("INSERT INTO favourite_entities(kind,folder_name,path,match_tpdb_id,match_stashdb_id) VALUES('performer','Demi Hawks','/one','tpdb1','stash1')")
            conn.commit()
        saved=db.suggestion_performer_ids(['Demi Hawks'])
        self.assertEqual(saved['Demi Hawks'],{'TPDB':{'tpdb1'},'StashDB':{'stash1'}})
        post=Mock(return_value={'queryScenes':{'scenes':[{'id':'scene','title':'Summer Fling','performers':[{'performer':{'id':'stash1','name':'Different database spelling'}}]}]}})
        rows,complete=search_joint_cast(['Demi Hawks'],[('StashDB','ep','key')],post,'names','scenes',None,Mock(),saved_ids=saved)
        self.assertTrue(complete);self.assertEqual(post.call_args.args[2],'scenes')
        self.assertEqual(post.call_args.args[3]['input']['performers']['value'],['stash1'])
        self.assertTrue(matching._required_performers_in_cast(['Demi Hawks'],rows[0]))
        rows[0]['performers'][0]['performer']['id']='wrong'
        self.assertFalse(matching._required_performers_in_cast(['Demi Hawks'],rows[0]))
        with db.get_conn() as conn:
            conn.execute("INSERT INTO favourite_entities(kind,folder_name,path,match_stashdb_id) VALUES('performer','Demi Hawks','/two','stash2')");conn.commit()
        post.reset_mock()
        rows,_=search_joint_cast(['Demi Hawks'],[('StashDB','ep','key')],post,'names','scenes',None,Mock(),saved_ids=db.suggestion_performer_ids(['Demi Hawks']))
        self.assertEqual(rows,[]);post.assert_not_called()

    def test_failure_backoff_is_short_bounded_and_reset_by_success(self):
        db.upsert_file('retry.mp4');db.update_file('retry.mp4',status='unmatched')
        for now,delay in [(1000,300),(1300,900),(2200,3600),(5800,86400),(92200,86400)]:
            with patch('time.time',return_value=now):
                db.queue_suggestion_mark_attempt('retry.mp4');db.queue_suggestion_finish_attempt('retry.mp4',False)
            with patch('time.time',return_value=now+delay-1):self.assertEqual(db.queue_suggestion_pending(),[])
            with patch('time.time',return_value=now+delay):self.assertEqual(db.queue_suggestion_pending(),['retry.mp4'])
        with patch('time.time',return_value=200000):
            db.queue_suggestion_mark_attempt('retry.mp4');db.queue_suggestion_finish_attempt('retry.mp4',True)
        with patch('time.time',return_value=200300):self.assertEqual(db.queue_suggestion_pending(),[])
        with db.get_conn() as conn:
            row=conn.execute("SELECT failure_count,next_retry,outcome FROM queue_suggestion_attempts WHERE filename='retry.mp4'").fetchone()
        self.assertEqual(tuple(row),(0,None,'complete'))


class SharedParserTests(unittest.TestCase):
    def test_manual_and_automatic_share_formats_settings_and_dates(self):
        settings=dict(db.DEFAULTS,filename_strip_words='UNWANTED',site_abbreviations='{"ST":"Example Studio"}')
        with patch.object(db,'_library_guess_from_release_name',return_value={'performers':['Jane Doe'],'studio':'Example Studio'}),patch.object(db,'studio_logo_list_all',return_value=[]),patch.object(db,'get_settings',return_value=settings),patch('app.core.matching.Path.is_file',return_value=True):
            for name in ['Jane Doe/[Jane Doe] Jane Doe ST 20261014 Private Lesson 2.mp4',
                         'ST|2026-10-14|Jane Doe|Private Lesson 2.mp4',
                         'ST.2026.10.14.Jane.Doe.Private.Lesson.2.mp4',
                         'Jane Doe UNWANTED PrivateLesson2.mp4']:
                with self.subTest(name=name):
                    parsed=parse_filename(name,settings)
                    full,title,first,studio,cast=matching._split_query_for_search(name)
                    self.assertEqual((title,studio,cast),(parsed['title'],parsed['site'],parsed['performer_names']))
                    self.assertNotIn('UNWANTED',title)
                    self.assertIn('2',title)
            self.assertEqual(parse_filename('ST|2026-10-14|Jane Doe|Private Lesson 2.mp4',settings)['title'],'Private Lesson 2')
