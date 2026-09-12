"""Real Chromium + real frontend assets; all API traffic is isolated fixtures.
Run: python tests/browser/smoke.py
"""
import json
import re
from pathlib import Path
import unittest
from urllib.parse import urlparse, quote
from playwright.sync_api import sync_playwright, expect

ROOT=Path(__file__).resolve().parents[2]
IMAGE='data:image/svg+xml,'+quote('<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"><rect width="10" height="10" fill="blue"/></svg>')
class BrowserFlows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pw=sync_playwright().start()
        cls.browser=cls.pw.chromium.launch()
    @classmethod
    def tearDownClass(cls):
        cls.browser.close();cls.pw.stop()
    def setUp(self):
        self.page=self.browser.new_page()
        self.calls=[];self.handlers={}
        self.page.route('**/*',self.route)
        self.page.goto('http://top-shelf.test/')
        self.page.evaluate("window.toast=()=>{}; window.esc=x=>String(x??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('\\\"','&quot;');")
    def tearDown(self):self.page.close()
    def route(self,route):
        request=route.request;path=urlparse(request.url).path
        self.calls.append((request.method,path,request.post_data))
        if path == '/fixture.svg':
            route.fulfill(content_type='image/svg+xml',body='<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"><rect width="10" height="10" fill="blue"/></svg>');return
        if path in self.handlers:
            status,body=self.handlers[path](request)
            route.fulfill(status=status,json=body);return
        if path.startswith('/static/'):
            file=ROOT/path.lstrip('/')
            if file.is_file():route.fulfill(path=str(file));return
        if path.startswith('/api/'):
            route.fulfill(json={});return
        route.fulfill(content_type='text/html',body='<html><body></body></html>')
    def script(self,name):self.page.add_script_tag(path=str(ROOT/'static'/name))
    def test_bio_save_validation_and_refresh(self):
        data={'identity':{'canonical_name':'Test Person'},'bio':{'gender':'Female','birthdate':'1990-01-01'},'library_status':{'in_library':True,'row_id':1},'headshot_url':'/fixture.svg?v=1'}
        self.handlers['/api/performer/popup']=lambda _:(200,data)
        self.handlers['/api/performers/bio']=lambda req:(200,{'ok':True,'fields':json.loads(req.post_data)['fields']})
        self.script('performer-popup.js')
        self.page.evaluate("openPerformerPopup({libraryRowId:1,name:'Test Person'})")
        self.page.locator('[data-edit-bio]').click()
        self.page.locator('[name=biography]').fill('Edited biography')
        self.page.get_by_role('button',name='Save changes',exact=True).click()
        expect(self.page.locator('.pp-cell-bio')).to_contain_text('Edited biography')
        saves=[json.loads(body) for method,path,body in self.calls if path=='/api/performers/bio']
        self.assertEqual(saves,[{'row_id':1,'fields':{'biography':'Edited biography'}}])
        data['headshot_url']='/fixture.svg?v=2'
        self.page.evaluate("openPerformerPopup({libraryRowId:1,name:'Test Person',_refresh:true})")
        expect(self.page.locator('.pp-headshot')).to_have_attribute('src',re.compile(re.escape(data['headshot_url'])+r'&__t=\d+'))
    def test_library_headshot_changes_without_reload(self):
        row={'id':1,'kind':'performer','folder_name':'Test Person','name':'Test Person','image_url':IMAGE}
        self.handlers['/api/favourites']=lambda _:(200,{'performers':[row]})
        self.handlers['/api/favourites/row']=lambda _:(200,{'row':row})
        self.page.goto('http://top-shelf.test/static/stars.html')
        expect(self.page.locator('#favGrid img').first).to_have_attribute('src',IMAGE)
        row['image_url']=IMAGE.replace('blue','red')
        self.page.evaluate("document.dispatchEvent(new CustomEvent('library-row-updated',{detail:{rowId:1,kind:'performer'}}))")
        expect(self.page.locator('#favGrid img').first).to_have_attribute('src',row['image_url'])

    def test_image_selection_reports_each_rejected_file(self):
        self.script('poster-role-picker.js')
        self.page.evaluate("openPosterRolePicker(1,{name:'Test Person'})")
        self.page.evaluate("""() => {
          window.uploadMessages=[];window.toast=message=>uploadMessages.push(message);
          const files=[new File(['ok'],'valid.svg',{type:'image/svg+xml'}),
            new File([new Uint8Array(20*1024*1024+1)],'oversized.png',{type:'image/png'}),
            new File(['text'],'notes.txt',{type:'text/plain'})];
          posterRoleHandleImportFile(files);
        }""")
        messages=self.page.evaluate('uploadMessages.join("\\n")')
        self.assertIn('oversized.png: exceeds',messages)
        self.assertIn('notes.txt: not an image',messages)
        self.assertNotIn('valid.svg',messages)
        expect(self.page.locator('#posterRoleCandidates button')).to_have_count(1)

    def test_download_keep_files_and_retry(self):
        item={'id':'qbit-abc','name':'Test download','client':'qbittorrent','source':'torrent','status':'error','failed':True,'queue':'active','progress_pct':0}
        self.handlers['/api/downloads']=lambda _:(200,{'items':[item]})
        self.handlers['/api/downloads/remove']=lambda _:(200,{'ok':True})
        self.handlers['/api/downloads/retry']=lambda _:(200,{'ok':True})
        self.page.goto('http://top-shelf.test/static/downloads.html')
        self.page.locator('.dl-tile-remove').click()
        self.page.get_by_role('button',name='Remove from client · keep files',exact=True).click()
        self.page.wait_for_function("!document.querySelector('.dl-bar')")
        removals=[json.loads(body) for method,path,body in self.calls if path=='/api/downloads/remove']
        self.assertEqual(removals,[{'id':'qbit-abc','delete_files':False}])
        item.update(id='nzbget-h-42',client='nzbget',source='nzb',can_retry=True)
        self.page.evaluate('loadDownloads()')
        self.page.get_by_role('button',name='Retry download').click()
        self.page.wait_for_timeout(100)
        self.assertTrue(any(path=='/api/downloads/retry' for _,path,_ in self.calls))
    def test_filmstrip_reuses_frames(self):
        self.handlers['/api/queue/thumbs']=lambda _:(200,{'ready':True,'thumbs':[{'i':i,'url':IMAGE,'percent':10+i*20} for i in range(5)]})
        self.page.goto('http://top-shelf.test/static/queue.html')
        self.page.evaluate("openQueueFilmstrip('test.mp4')")
        expect(self.page.locator('#qFilmstripStrip img')).to_have_count(5)
        self.page.evaluate("openQueueFilmstrip('test.mp4')")
        expect(self.page.locator('#qFilmstripStrip img')).to_have_count(5)
        self.assertEqual(sum(path=='/api/queue/thumbs' for _,path,_ in self.calls),1)
    def test_performer_search_shows_more_than_50(self):
        releases=[{'guid':str(i),'title':f'Test Person Scene {i}', 'type':'nzb','indexer':'Test','indexer_id':1,'size':1000000} for i in range(75)]
        self.handlers['/api/prowlarr/search']=lambda _:(200,{'results':releases})
        self.page.set_content('<div id="host"><div class="ts-prowlarr-embed-status"></div><div class="ts-prowlarr-embed-filters"></div><div class="ts-prowlarr-embed-results"></div></div>')
        self.script('ts-utils.js')
        self.page.evaluate("mountEmbeddedProwlarrSearch(document.querySelector('#host'),{title:'Test Person',performer:'Test Person',dedupe:false})")
        expect(self.page.locator('.ts-prowlarr-embed-status')).to_contain_text('75')

if __name__=='__main__':unittest.main(verbosity=2)
