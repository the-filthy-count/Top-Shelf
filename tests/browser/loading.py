"""Isolated startup route and Chromium navigation checks. Run directly."""
import sys,ast,asyncio,threading,time,os,logging
from pathlib import Path
from unittest.mock import mock_open,patch
from starlette.responses import HTMLResponse,RedirectResponse
from playwright.sync_api import sync_playwright,expect
root=Path(__file__).resolve().parents[2]
source=(root/'main.py').read_text();tree=ast.parse(source)
node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='loading_page');node.decorator_list=[]
ns=dict(HTMLResponse=HTMLResponse,RedirectResponse=RedirectResponse,_startup_complete=threading.Event(),_log=logging.getLogger('test'),_process_boot_id='test',os=os,time=time,_process_started_monotonic=time.monotonic(),LOADING_PATH='/loading')
exec(compile(ast.Module(body=[node],type_ignores=[]),'loading','exec'),ns)
with patch('builtins.open',mock_open(read_data='__NEXT__')):
 r=asyncio.run(ns['loading_page']('/performer/1'));assert r.status_code==200 and r.headers['cache-control']=='no-store'
ns['_startup_complete'].set()
for target,want in [('/performer/1','/performer/1'),('/loading?next=/loading','/unmatched'),('//bad','/unmatched'),('/\\bad','/unmatched')]:
 r=asyncio.run(ns['loading_page'](target));assert r.status_code==302 and r.headers['location']==want and r.headers['cache-control']=='no-store'
with sync_playwright() as p:
 browser=p.chromium.launch();page=browser.new_page()
 def route(r):
  path=r.request.url.split('top-shelf.test')[-1]
  if path.startswith('/loading'):r.fulfill(content_type='text/html',body=(root/'static/loading.html').read_text().replace('__NEXT__','/performer/1'))
  elif path=='/api/startup/status':r.fulfill(json={'ready':True})
  else:r.fulfill(content_type='text/html',body='<html>Fixture</html>')
 page.route('**/*',route)
 page.goto('http://top-shelf.test/home')
 page.goto('http://top-shelf.test/loading')
 expect(page).to_have_url('http://top-shelf.test/performer/1')
 page.go_back();expect(page).to_have_url('http://top-shelf.test/home')
 browser.close()
print('PASS: loading history, ready redirect, no-store and redirect safety')
