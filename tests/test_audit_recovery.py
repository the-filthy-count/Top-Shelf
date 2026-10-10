import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import tempfile,unittest,json,ast,sqlite3
from pathlib import Path
from unittest.mock import patch
import merge_recovery as merge
class MergeTests(unittest.TestCase):
 def test_copy_failure_preserves_sources_and_same_request_resumes(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);a=root/'A';b=root/'B';a.mkdir();b.mkdir()
   (a/'same.mp4').write_bytes(b'a');(b/'same.mp4').write_bytes(b'b');(b/'poster.jpg').write_bytes(b'art')
   rows=[{'id':1,'path':str(a)},{'id':2,'path':str(b)}];dest=root/'Group'
   prepare=lambda:dict(rows=rows,primary_id=1,destination=str(dest),group_name='Group')
   commits=[]
   def commit(plan):commits.append(plan['operation_id'])
   original=merge.shutil.copyfile
   def fail(source,target):
    if Path(source).parent==b:raise OSError('injected disk failure')
    return original(source,target)
   with patch.object(merge.shutil,'copyfile',side_effect=fail):
    with self.assertRaises(RuntimeError):merge.execute(root/'jobs',{'id':1},prepare,commit,{'.mp4'})
   self.assertTrue((a/'same.mp4').exists());self.assertTrue((b/'same.mp4').exists());self.assertEqual(commits,[])
   result=merge.execute(root/'jobs',{'id':1},lambda: self.fail('Must use durable plan'),commit,{'.mp4'})
   self.assertTrue(result['ok']);self.assertEqual((dest/'same.mp4').read_bytes(),b'a');self.assertEqual((dest/'same (2).mp4').read_bytes(),b'b')
   self.assertFalse(a.exists());self.assertFalse(b.exists())
   merge.execute(root/'jobs',{'id':1},prepare,commit,{'.mp4'});self.assertEqual(len(commits),1)
 def test_commit_failure_preserves_sources(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);a=root/'A';a.mkdir();(a/'v.mp4').write_bytes(b'video')
   plan=lambda:dict(rows=[{'id':1,'path':str(a)}],primary_id=1,destination=str(root/'G'))
   with self.assertRaises(RuntimeError):merge.execute(root/'jobs',{},plan,lambda _:(_ for _ in ()).throw(RuntimeError('DB failure')),{'.mp4'})
   self.assertTrue((a/'v.mp4').exists())
 def test_database_commit_rolls_back_and_is_idempotent(self):
  with tempfile.TemporaryDirectory() as d:
   conn=sqlite3.connect(str(Path(d)/'db'));conn.row_factory=sqlite3.Row
   conn.executescript("CREATE TABLE favourite_entities(id INTEGER PRIMARY KEY,folder_name TEXT,path TEXT,is_group INT,group_ids_json TEXT,path_missing INT,updated_at TEXT);CREATE TABLE library_files(destination TEXT UNIQUE,current_filename TEXT,filename_stem TEXT,library_root TEXT,updated_at TEXT);INSERT INTO favourite_entities VALUES(1,'A','/A',0,'{}',0,'');INSERT INTO favourite_entities VALUES(2,'B','/B',0,'{}',0,'');")
   tree=ast.parse((Path(merge.__file__).parent/'database.py').read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='favourite_merge_commit')
   ns=dict(get_conn=lambda:conn,json=json,Path=Path,_iso_now=lambda:'now');exec(compile(ast.Module(body=[node],type_ignores=[]),'merge-db','exec'),ns)
   plan=dict(operation_id='one',rows=[{'id':1,'path':'/A'},{'id':2,'path':'wrong'}],primary_id=1,destination='/G',group_name='G',group_ids={},files=[],library_root='/')
   with self.assertRaises(ValueError):ns['favourite_merge_commit'](plan)
   self.assertEqual(conn.execute('SELECT path FROM favourite_entities WHERE id=1').fetchone()[0],'/A')
   plan['rows'][1]['path']='/B';ns['favourite_merge_commit'](plan);ns['favourite_merge_commit'](plan)
   self.assertEqual(conn.execute('SELECT COUNT(*) FROM favourite_entities').fetchone()[0],1)
   conn.close()
if __name__=='__main__':unittest.main()
