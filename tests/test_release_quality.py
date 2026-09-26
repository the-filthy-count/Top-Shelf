import ast
import re
import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

class ReleaseQuality(unittest.TestCase):
 def test_only_unique_exact_matches_get_resolution_comparison(self):
  conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row;self.addCleanup(conn.close)
  conn.executescript('CREATE TABLE processed_files(id INTEGER,match_title TEXT,match_studio TEXT); CREATE TABLE library_files(source_record_id INTEGER,media_height INTEGER,is_removed INTEGER); INSERT INTO processed_files VALUES(1,"Exact title","Studio"); INSERT INTO library_files VALUES(1,720,0);')
  @contextmanager
  def get_conn():yield conn
  tree=ast.parse(Path('main.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='release_quality_comparison');fn.decorator_list=[]
  ns={'db':SimpleNamespace(get_conn=get_conn),'re':re,'HTTPException':RuntimeError}
  exec(compile(ast.Module(body=[fn],type_ignores=[]),'test','exec'),ns)
  compare=ns['release_quality_comparison']
  item={'title':'Exact title','studio':'Studio','releases':[{'title':'Exact title 1080p'}]}
  self.assertTrue(compare({'items':[item]})['matches']['0']['higher_resolution'])
  self.assertEqual(compare({'items':[{**item,'studio':'Other'}]})['matches'],{})
  conn.execute('INSERT INTO library_files VALUES(1,1080,0)')
  self.assertEqual(compare({'items':[item]})['matches'],{})
if __name__=='__main__':unittest.main()
