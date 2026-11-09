from contextlib import closing
import json
import lzma
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from app.core import iafd_catalogue as c

class CatalogueTests(unittest.TestCase):
    def test_joined_search_identity_filters_and_detail(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);files=[]
            data=[[
                {'source_reference':'p1','name':'Jane Doe','aliases':['JaneD'],'tattoos':['unused'],'image_url':'https://example.com/photo.jpg','urls':[{'site':'IAFD','url':'https://www.iafd.com/person.rme/id=p1'}]},
                {'source_reference':'p2','name':'Doe Jane','aliases':[]},
                {'source_reference':'p3','name':'Rare Performer','aliases':[]}],
                [{'source_reference':'s1','name':'Studio One'}],
                [{'source_reference':'t1','title':'A Sample Movie','unused_export_field':'discard','studio':{'source_reference':'s1'},'performers':[{'source_reference':'p1'}],
                  'urls':[{'url':'https://www.iafd.com/title.rme/id=t1'}], 'release_date':'2020-01-02'},
                 {'source_reference':'t2','title':'A Sample Movie Two','studio':{'source_reference':'s1'},'performers':[{'source_reference':'p1'},{'source_reference':'p2'}],
                  'urls':[{'url':'https://www.iafd.com/title.rme/id=t2'}], 'release_date':''},
                 {'source_reference':'support','title':'Support Movie','performers':[{'source_reference':'p2'}],
                  'urls':[{'url':'https://www.iafd.com/title.rme/id=support'}]},
                 {'source_reference':'rare','title':'Rare Movie','performers':[{'source_reference':'p3'},{'source_reference':'p1'}],
                  'urls':[{'url':'https://www.iafd.com/title.rme/id=rare'}]},
                 {'source_reference':'old','title':'Old Title','performers':[{'source_reference':'p2'}],
                  'release_date':'1999-12-31'}]]
            for i,records in enumerate(data):
                file=root/f'{i}.xz';files.append(file)
                with lzma.open(file,'wt') as out:
                    for record in records:out.write(json.dumps(record)+'\n')
            target=root/'index.sqlite';self.assertEqual(c.import_files(*files,target),3)
            with patch.object(c,'path',return_value=target):
                rows=c.search('Sample Movie',performer='JaneD',studio='Studio One')
                self.assertEqual(len(rows),2)
                self.assertEqual(rows[0]['performers'][0]['performer']['id'],'p1')
                self.assertEqual(rows[0]['studio']['name'],'Studio One')
                self.assertEqual(c.search('Sample',performer='Jane'),[])
                self.assertEqual(c.search('Sample',date_from='2021-01-01'),[])
                self.assertEqual(len(c.search(performer_id='p1')),2)
                self.assertEqual(c.filmography('p1')[0]['studio'],'Studio One')
                self.assertEqual(c.detail(rows[0]['_iafd_url'])['title'],'A Sample Movie')
                self.assertEqual(c.detail('https://www.iafd.com/title.rme/id=t2')['release_date'],'')
                self.assertNotIn('phash',rows[0])
                self.assertEqual(c.search('Old Title'),[])
                self.assertEqual(c.search('Rare Movie'),[])
                self.assertIsNone(c.detail('https://www.iafd.com/title.rme/id=rare'))
                self.assertEqual(c.filmography('p3'),[])
                import sqlite3
                with closing(sqlite3.connect(target)) as conn:
                    self.assertEqual(conn.execute('SELECT count(*) FROM people').fetchone()[0],2)
                    person=json.loads(conn.execute("SELECT data FROM people WHERE id='p1'").fetchone()[0])
                    self.assertNotIn('tattoos',person)
                    self.assertNotIn('image_url',person)
                    title=json.loads(conn.execute("SELECT data FROM titles WHERE id='t1'").fetchone()[0])
                    self.assertNotIn('performers',title)
                    self.assertNotIn('unused_export_field',title)
                    self.assertEqual(conn.execute('SELECT typeof(person),typeof(title) FROM cast LIMIT 1').fetchone(),('integer','integer'))
                    self.assertEqual(conn.execute("SELECT count(*) FROM cast WHERE title='old'").fetchone()[0],0)
                    self.assertEqual(conn.execute("SELECT count(*) FROM title_search WHERE title='Old Title'").fetchone()[0],0)
                self.assertTrue(c.current_index())
                from app.core import iafd_dataset as people
                performer_index=root/'performers.sqlite'
                people.import_file(files[0],performer_index)
                with patch.object(people,'path',return_value=performer_index):
                    people.retain_catalogue_performers()
                    self.assertEqual(people.search('Rare Performer'),[])
                    self.assertEqual(people.search('Jane Doe')[0]['id'],'p1')

    def test_movie_nfo_preserves_iafd_identity(self):
        import ast
        import io
        import xml.etree.ElementTree as ET
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='build_movie_nfo')
        namespace={'ET':ET,'io':io}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),namespace)
        xml=namespace['build_movie_nfo']({'title':'Movie','iafd_id':'https://www.iafd.com/title.rme/id=abc'},'movie.mp4')
        root=ET.fromstring(xml)
        self.assertIsNone(root.find('tmdbid'))
        self.assertEqual(root.find('uniqueid').get('type'),'iafd')
        self.assertEqual(root.find('uniqueid').text,'https://www.iafd.com/title.rme/id=abc')

    def test_cutoff_keeps_2000_and_unknown_dates(self):
        self.assertFalse(c.keep_title({'release_date':'1999-12-31'}))
        self.assertFalse(c.keep_title({'production_date':'1998'}))
        self.assertTrue(c.keep_title({'release_date':'2000-01-01'}))
        self.assertTrue(c.keep_title({'release_date':''}))
        self.assertTrue(c.keep_title({'release_date':'2001-01-01','production_date':'1999-01-01'}))

    def test_credit_filter_does_not_cascade(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); files=[]
            data=[[
                {'source_reference':'regular','name':'Regular'},
                {'source_reference':'rare','name':'Rare'},
                {'source_reference':'unused','name':'Unused'}], [], [
                {'source_reference':'a','title':'A','performers':[{'source_reference':'regular'},{'source_reference':'rare'}]},
                {'source_reference':'b','title':'B','performers':[{'source_reference':'regular'}]}]]
            for i, records in enumerate(data):
                file=root/f'{i}.xz';files.append(file)
                with lzma.open(file,'wt') as out:
                    for record in records: out.write(json.dumps(record)+'\n')
            target=root/'index.sqlite'
            self.assertEqual(c.import_files(*files,target),1)
            import sqlite3
            with closing(sqlite3.connect(target)) as conn:
                self.assertEqual(conn.execute('SELECT id FROM people').fetchall(), [('regular',)])
                self.assertEqual(conn.execute('SELECT id FROM titles').fetchall(), [('b',)])
