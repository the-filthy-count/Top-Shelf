"""Disposable production-environment ASGI smoke test; never point at a real DB.
Run directly in an ephemeral container with the project mounted read-only.
"""
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

scratch=tempfile.TemporaryDirectory(prefix='top-shelf-integration-')
os.environ['DB_PATH']=str(Path(scratch.name)/'app.db')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
import main

# Exercise the real startup/shutdown/middleware/routes. Only external cache
# warmup is stubbed: this test must not depend on any configured service.
def ready():
    main._startup_complete.set()
    main._startup_status['ready']=True

with patch.object(main,'_run_startup_warmup',ready), patch('requests.sessions.Session.request',side_effect=RuntimeError('External network disabled in integration test')):
    with TestClient(main.app) as client:
        assert client.get('/api/health').status_code==200
        response=client.post('/api/auth/login',json={'password':'fixture','next':'javascript:alert(1)'})
        assert response.status_code==200 and response.json()['next']=='/unmatched'
        response=client.get('/api/favourites?kind=performer')
        assert response.status_code==200 and response.json()['performers']==[]
        response=client.post('/api/favourites/lock',json={'id':999,'matches_locked':True})
        assert response.status_code==404
        response=client.get('/api/prowlarr/search',params={'q':'fixture','cursor':'not-json'})
        assert response.status_code==400
        main._headshots_by_name_cache['fixture']=(0,None)
        response=client.post('/api/favourites/lock-all')
        assert response.status_code==200 and not main._headshots_by_name_cache
        # Slow cleanup must not block an unrelated health request.
        import threading
        from concurrent.futures import ThreadPoolExecutor
        entered=threading.Event();release=threading.Event()
        folder=Path(scratch.name)/'empty';folder.mkdir()
        def slow_remove(_):
            entered.set()
            assert release.wait(5)
        with patch.object(main,'_health_library_roots',return_value=[{'path':scratch.name}]), patch.object(main.shutil,'rmtree',slow_remove):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(client.post,'/api/health/delete-folder',json={'path':str(folder)})
                assert entered.wait(5)
                try:
                    assert client.get('/api/health').status_code==200
                finally:
                    release.set()
                assert future.result(timeout=5).status_code==200
        # Exercise the real merge route, DB schema and file-index relocation.
        root=Path(scratch.name)/'performers';root.mkdir()
        first=root/'First';second=root/'Second';first.mkdir();second.mkdir()
        (first/'one.mp4').write_bytes(b'first fixture')
        (second/'two.mp4').write_bytes(b'second fixture')
        main.db.save_directories([{'type':'performer','path':str(root),'label':'Fixture','rank':1}])
        conn=main.db.get_conn()
        one=conn.execute("INSERT INTO favourite_entities(kind,folder_name,path) VALUES('performer','First',?)",(str(first),)).lastrowid
        two=conn.execute("INSERT INTO favourite_entities(kind,folder_name,path) VALUES('performer','Second',?)",(str(second),)).lastrowid
        conn.execute("INSERT INTO library_files(filename_stem,current_filename,destination,library_root) VALUES('two','two.mp4',?,?)",(str(second/'two.mp4'),str(root)))
        conn.commit()
        payload={'primary_row_id':one,'secondary_row_ids':[two],'group_name':'Group','dest_dir':str(root)}
        with patch.object(main,'_metadata_base_dir',return_value=Path(scratch.name)/'metadata'):
            response=client.post('/api/performers/merge',json=payload)
            assert response.status_code==200, response.text
            assert client.post('/api/performers/merge',json=payload).status_code==200
        assert (root/'Group'/'one.mp4').read_bytes()==b'first fixture'
        assert (root/'Group'/'two.mp4').read_bytes()==b'second fixture'
        assert not first.exists() and not second.exists()
        assert main.db.favourite_get(two) is None
        assert conn.execute("SELECT destination FROM library_files").fetchone()[0]==str(root/'Group'/'two.mp4')
        response=client.get('/loading?next=/performer/1',follow_redirects=False)
        assert response.status_code==302 and response.headers['cache-control']=='no-store'
print('PASS: real application startup, middleware, API routes and shutdown')
