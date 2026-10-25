import re
import sqlite3
import unittest
from datetime import datetime
from test_duplicate_filing_safety import load

class MovieValidationTests(unittest.TestCase):
    def test_candidate_requires_title_year_and_unique_identity(self):
        choose=load('main.py','_verified_movie_candidate',{'re':re})
        right={'id':'right','title':'The Movie!','year':'2020'}
        wrong={'id':'wrong','title':'Unrelated','year':'1994'}
        self.assertEqual(choose([wrong,right],'the movie','2020'),right)
        self.assertIsNone(choose([wrong],'The Movie','2020'))
        self.assertIsNone(choose([right],'The Movie','2021'))
        self.assertIsNone(choose([dict(right,year='')],'The Movie','2020'))
        self.assertIsNone(choose([right,dict(right,id='other')],'The Movie','2020'))
        self.assertEqual(choose([right,right],'The Movie',None),right)
        self.assertEqual(choose([{'_id':'tpdb','name':'The Movie','date':'2020-03-01'}],'The Movie','2020')['_id'],'tpdb')

    def test_success_clears_old_error_and_foreign_source_ids(self):
        conn=sqlite3.connect(':memory:');self.addCleanup(conn.close)
        conn.execute('CREATE TABLE processed_movies(filename TEXT,status TEXT,processed_at TEXT,tmdb_id TEXT,tpdb_id TEXT,javstash_id TEXT,match_source TEXT,error TEXT)')
        conn.execute("INSERT INTO processed_movies VALUES('movie.mp4','error',NULL,'old',NULL,NULL,'tmdb','failed')")
        update=load('database.py','update_movie',{'datetime':datetime,'get_conn':lambda:conn})
        update('movie.mp4',status='processing')
        self.assertEqual(conn.execute('SELECT tmdb_id,error FROM processed_movies').fetchone(),('old',None))
        update('movie.mp4',status='filed',match_source='tpdb',tpdb_id='new')
        self.assertEqual(conn.execute('SELECT tmdb_id,tpdb_id,javstash_id,error FROM processed_movies').fetchone(),(None,'new',None,None))
        update('movie.mp4',status='error',error='new failure')
        self.assertEqual(conn.execute('SELECT tpdb_id,error FROM processed_movies').fetchone(),('new','new failure'))
