"""Library cache integration without starting the app or touching user data."""
import ast
import logging
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import Mock

SOURCE=Path(__file__).resolve().parents[1]/'main.py'

def load():
    tree=ast.parse(SOURCE.read_text())
    names={'_WarmCache','_LibrarySnapshots','_build_favourites_snapshot','api_favourites_list'}
    nodes=[n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in names]
    for n in nodes:n.decorator_list=[]
    rows=[{'id':1,'kind':'performer','folder_name':'A'},{'id':2,'kind':'studio','folder_name':'B'},{'id':3,'kind':'movie','folder_name':'C'},{'id':4,'kind':'jav','folder_name':'D'}]
    db=Mock();db.favourite_list.side_effect=lambda kind=None:[dict(r) for r in rows if kind is None or r['kind']==kind]
    db.get_settings.return_value={}
    vices=Mock(return_value=[])
    ns=dict(threading=threading,time=time,_log=logging.getLogger(),Path=Path,db=db,
            _favourites_row_api=lambda r:r,_load_vices=vices,_movie_library_sort_key=lambda s:s)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),ns)
    ns['_favourites_warm']=ns['_LibrarySnapshots']()
    return ns,rows,db,vices

class LibraryCacheTests(unittest.TestCase):
    def test_route_category_isolation_reuse_and_settings(self):
        ns,rows,db,vices=load();route=ns['api_favourites_list']
        result=route(kind='performer')
        self.assertEqual(result['performers'][0]['id'],1)
        self.assertEqual(result['studios'],[])
        db.favourite_list.assert_called_once_with('performer');vices.assert_not_called()
        route(kind='performer');self.assertEqual(db.favourite_list.call_count,1)
        invalid=route(kind='unknown');self.assertEqual(invalid['settings'],result['settings'])

    def test_add_edit_delete_visible_after_invalidation(self):
        ns,rows,db,vices=load();route=ns['api_favourites_list'];cache=ns['_favourites_warm']
        route(kind='performer');rows[0]['folder_name']='Changed';cache.invalidate()
        self.assertEqual(route(kind='performer')['performers'][0]['folder_name'],'Changed')
        rows.append({'id':5,'kind':'performer','folder_name':'Added'});cache.invalidate()
        self.assertEqual(len(route(kind='performer')['performers']),2)
        rows[:]=[r for r in rows if r['id']!=1];cache.invalidate()
        self.assertEqual([r['id'] for r in route(kind='performer')['performers']],[5])

    def test_full_and_multiple_categories_preserve_shape(self):
        ns,rows,db,vices=load();route=ns['api_favourites_list']
        full=route();self.assertEqual(set(full),{'performers','studios','movies','jav','vices','settings'})
        for key in ('performers','studios','movies','jav'):self.assertEqual(len(full[key]),1)
        partial=route(kinds='studio,performer')
        self.assertEqual(partial['movies'],[]);self.assertEqual(len(partial['studios']),1)

    def test_invalidation_during_build_discards_old_data(self):
        ns,*_=load();started=threading.Event();resume=threading.Event();calls=[]
        def build():
            calls.append(1)
            if len(calls)==1:
                started.set();resume.wait(2);return 'old'
            return 'new'
        cache=ns['_WarmCache']('test',build,3600);results=[]
        worker=threading.Thread(target=lambda:results.append(cache.get()))
        worker.start();self.assertTrue(started.wait(2));cache.invalidate();resume.set();worker.join(3)
        self.assertFalse(worker.is_alive());self.assertEqual(results,['new'])

if __name__=='__main__':unittest.main()
