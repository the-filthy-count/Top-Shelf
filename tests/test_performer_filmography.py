import unittest
from unittest.mock import Mock, patch
from app.integrations import performer_filmography as f

class FilmographyTests(unittest.TestCase):
    def row(self, source, sid='1', title='Scene', date='2024-01-01', studio='Studio'):
        return dict(source=source,id=sid,title=title,date=date,year=date[:4],studio=studio,url='https://example.com/'+sid)

    def test_cross_source_dedupe_retains_links_and_distinct_scenes(self):
        rows = [self.row('TPDB'),self.row('StashDB'),self.row('TPDB','2'),self.row('FansDB',date='2024-02-01')]
        result=f.dedupe(rows)
        self.assertEqual(len(result),3)
        self.assertTrue(any(len(row['sources'])==2 for row in result))
        self.assertEqual(len(f.dedupe([self.row('TPDB',date=''),self.row('FansDB',date='')])),2)

    def test_tpdb_follows_all_pages(self):
        session=Mock()
        session.get.side_effect=[Mock(json=Mock(return_value={'data':[{'id':str(i),'title':'Scene','date':'2024-01-01'}], 'meta':{'last_page':3}})) for i in range(3)]
        self.assertEqual(len(f.fetch_source('tpdb','p','key',session)),3)
        self.assertEqual(session.get.call_count,3)

    def test_stashbox_follows_pages_and_rejects_repeated_page(self):
        session=Mock()
        rows=[{'id':str(i),'title':'Scene'} for i in range(50)]
        session.post.side_effect=[Mock(json=Mock(return_value={'data':{'queryScenes':{'scenes':rows}}})),Mock(json=Mock(return_value={'data':{'queryScenes':{'scenes':[]}}}))]
        self.assertEqual(len(f.fetch_source('fansdb','p','key',session)),50)
        session.post.side_effect=None
        session.post.return_value=Mock(json=Mock(return_value={'data':{'queryScenes':{'scenes':rows}}}))
        with self.assertRaises(ValueError): f.fetch_source('fansdb','p','key',session)

    def test_failed_refresh_keeps_saved_source_results(self):
        ids={source:['p'] if source=='tpdb' else [] for source in f.SOURCES}
        key=f.cache_key(ids)
        store={key+':tpdb:p':{'scenes':[self.row('TPDB')]}}
        with patch.object(f.db,'get_settings',return_value={'api_key_tpdb':'key'}), patch.object(f.db,'app_json_cache_get',side_effect=store.get), patch.object(f.db,'app_json_cache_put',side_effect=lambda k,v:store.__setitem__(k,v)), patch.object(f,'fetch_source',side_effect=RuntimeError('offline')):
            f.refresh(ids)
        self.assertEqual(len(store[key]['films']),1)
        self.assertIn('failed',store[key]['sources'][0]['state'])

    def test_group_ids_are_source_scoped(self):
        ids=f.identity({'match_tpdb_id':'one','group_ids_json':'{"tpdb":["two", {"id":"one"}], "fansdb":["three"]}'})
        self.assertEqual(ids['tpdb'],['one','two'])
        self.assertEqual(ids['fansdb'],['three'])
