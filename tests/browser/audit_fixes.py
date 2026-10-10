import json
import unittest
from urllib.parse import quote
from modal_close import ModalClose, expect
from smoke import ROOT

class AuditFixes(ModalClose):
    def test_bio_escape_keeps_performer_page(self):
        self.prepare()
        self.page.goto('http://top-shelf.test/performer/1')
        self.page.locator('[data-edit-bio]').click()
        expect(self.page.locator('dialog[open]')).to_be_visible()
        self.page.keyboard.press('Escape')
        expect(self.page.locator('dialog[open]')).to_have_count(0)
        expect(self.page).to_have_url('http://top-shelf.test/performer/1')
        expect(self.page.locator('#performerPopupModal')).to_be_visible()

    def test_login_rejects_executable_return_target(self):
        self.handlers['/api/auth/login']=lambda req:(200, {'ok':True,'next':json.loads(req.post_data)['next']})
        self.page.route('**/login?*',lambda route:route.fulfill(path=str(ROOT/'static/login.html')))
        target="javascript:void(document.body.dataset.audit='executed')"
        self.page.goto('http://top-shelf.test/login?next='+quote(target,safe=''))
        self.page.locator('#pw').fill('fixture-password')
        self.page.locator('#btn').click()
        expect(self.page).to_have_url('http://top-shelf.test/unmatched')

    def test_old_full_library_response_cannot_undo_lock(self):
        row={'id':1,'kind':'performer','name':'Fixture','folder_name':'Fixture','matches_locked':False}
        self.handlers['/api/favourites']=lambda _:(200,{'performers':[dict(row)]})
        self.handlers['/api/favourites/row']=lambda _:(200,{'row':dict(row)})
        self.handlers['/api/favourites/lock']=lambda _:(200,{'ok':True})
        self.page.goto('http://top-shelf.test/static/stars.html')
        button=self.page.locator('.fav-cell[data-id="1"] .fav-card-lock')
        expect(button).to_have_attribute('data-locked','0')
        held=[]
        def hold_first(route):
            if not held: held.append(route)
            else: route.fulfill(json={'performers':[dict(row)]})
        self.page.route('http://top-shelf.test/api/favourites',hold_first)
        self.page.evaluate('void load()')
        self.page.wait_for_timeout(100)
        self.assertTrue(held)
        row['matches_locked']=True
        button.click()
        expect(button).to_have_attribute('data-locked','1')
        self.page.wait_for_timeout(100)
        held[0].fulfill(json={'performers':[dict(row,matches_locked=False)]})
        self.page.wait_for_timeout(150)
        expect(button).to_have_attribute('data-locked','1')

    def test_partial_search_can_continue_and_keep_prior_results(self):
        calls=[]
        def search(req):
            calls.append(req.url)
            if 'cursor=' in req.url:
                return 200,{'results':[{'guid':'two','title':'Fixture two','type':'nzb'}]}
            return 200,{'results':[{'guid':'one','title':'Fixture one','type':'nzb'}],
                        'partial':True,'continuation':{'Fixture':{'1':100}}}
        self.handlers['/api/prowlarr/search']=search
        self.script('ts-utils.js')
        self.page.evaluate("openProwlarrSearchPopup({title:'Fixture',strictMatch:false})")
        self.page.get_by_role('button',name='Continue search',exact=True).click()
        expect(self.page.locator('#tsProwlarrPopupResults')).to_contain_text('Fixture one')
        expect(self.page.locator('#tsProwlarrPopupResults')).to_contain_text('Fixture two')
        self.assertEqual(len(calls),2)

    def test_missing_headshot_cache_is_invalidated(self):
        for file in ('queue.html','index.html'):
            with self.subTest(file=file):
                found=[]
                self.handlers['/api/performers/headshots-by-name']=lambda _:(200,{'performers':found})
                self.page.goto('http://top-shelf.test/static/'+file)
                self.page.wait_for_function("typeof _fetchHeadshotsByName === 'function'")
                lookup=self.page.evaluate("_fetchHeadshotsByName(new Set(['Fixture']))")
                self.assertIsNone(lookup['fixture'])
                found.append({'name':'Fixture','headshot_url':'/fixture.svg?v=2'})
                self.page.evaluate("document.dispatchEvent(new CustomEvent('library-row-updated',{detail:{rowId:1,kind:'performer'}}))")
                self.page.wait_for_timeout(100)
                lookup=self.page.evaluate("_fetchHeadshotsByName(new Set(['Fixture']))")
                self.assertEqual(lookup['fixture'],'/fixture.svg?v=2')

    def test_merge_event_removes_secondary_tiles(self):
        rows=[{'id':i,'kind':'performer','folder_name':f'Fixture {i}'} for i in (1,2)]
        self.handlers['/api/favourites']=lambda _:(200,{'performers':list(rows)})
        self.page.goto('http://top-shelf.test/static/stars.html')
        expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)
        rows.pop()
        self.page.evaluate("document.dispatchEvent(new CustomEvent('lib-entity-changed',{detail:{id:1,kind:'performer',action:'merge'}}))")
        expect(self.page.locator('#favGrid .fav-cell')).to_have_count(1)
        self.assertIsNone(self.page.evaluate("JSON.parse(localStorage.getItem('ts:library-change')).rowId"))

    def test_return_after_add_cannot_be_overwritten_by_old_list(self):
        for kind, key, page in (
            ('performer', 'performers', 'stars.html'),
            ('studio', 'studios', 'studios.html'),
            ('movie', 'movies', 'movies.html'),
            ('vice', 'vices', 'vices.html'),
        ):
            with self.subTest(kind=kind):
                rows = [{'id':1, 'kind':kind, 'folder_name':'Before'}]
                self.handlers['/api/favourites'] = lambda _, key=key: (200, {key:list(rows)})
                self.page.goto('http://top-shelf.test/static/' + page)
                expect(self.page.locator('#favGrid .fav-cell')).to_have_count(1)
                held = []
                def hold_first(route):
                    if not held: held.append(route)
                    else: route.fulfill(json={key:list(rows)})
                self.page.route('http://top-shelf.test/api/favourites', hold_first)
                self.page.evaluate('void load()')
                self.page.wait_for_timeout(100)
                self.assertTrue(held)
                rows.append({'id':2, 'kind':kind, 'folder_name':'Added'})
                self.page.evaluate("""kind => {
                    localStorage.removeItem('ts:library-change');
                    sessionStorage.setItem('ts:library-invalidated', JSON.stringify({
                        type:'library-row-added', kind, rowId:2, changeId:kind+Date.now()
                    }));
                    window.dispatchEvent(new Event('focus'));
                }""", kind)
                expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)
                held[0].fulfill(json={key:rows[:1]})
                self.page.wait_for_timeout(200)
                expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)
                self.page.unroute('http://top-shelf.test/api/favourites', hold_first)

    def test_return_refreshes_library_without_a_change_notification(self):
        rows = [{'id':1, 'kind':'studio', 'folder_name':'Before'}]
        self.handlers['/api/favourites'] = lambda _: (200, {'studios':list(rows)})
        self.page.goto('http://top-shelf.test/static/studios.html')
        expect(self.page.locator('#favGrid .fav-cell')).to_have_count(1)
        rows.append({'id':2, 'kind':'studio', 'folder_name':'Added elsewhere'})
        self.page.evaluate("window.dispatchEvent(new Event('focus'))")
        expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)

if __name__=='__main__':
    names=[name for name in AuditFixes.__dict__ if name.startswith('test_')]
    result=unittest.TextTestRunner().run(unittest.TestSuite(AuditFixes(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
