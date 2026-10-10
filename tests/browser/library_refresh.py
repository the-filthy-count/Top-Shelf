"""Library mutation propagation and return-navigation regressions."""
import unittest
from urllib.parse import parse_qs, urlparse
from smoke import BrowserFlows, expect

class RefreshFlows(BrowserFlows):
    def setUp(self):
        super().setUp()
        self.page.close()
        self.context=self.browser.new_context()
        self.page=self.context.new_page()
        self.page.route('**/*',self.route)
        self.page.goto('http://top-shelf.test/')
        self.page.evaluate("window.toast=()=>{}; window.esc=x=>String(x??'')")
    def tearDown(self):
        self.context.close()

    def test_popup_lock_notifies_and_failed_save_does_not(self):
        self.handlers['/api/performer/popup'] = lambda _: (200, {
            'identity': {'canonical_name': 'Test Person'}, 'bio': {},
            'library_status': {'in_library': True, 'row_id': 1, 'matches_locked': False}})
        self.handlers['/api/favourites/lock'] = lambda _: (200, {'ok': True})
        self.script('ts-utils.js')
        self.script('performer-popup.js')
        self.page.evaluate("window.changes=[]; document.addEventListener('library-row-updated',e=>changes.push(e.detail)); openPerformerPopup({libraryRowId:1})")
        button = self.page.locator('.pp-name-action[data-action="lock"]')
        button.click()
        expect(button).to_have_class('pp-name-action is-locked')
        self.assertEqual(self.page.evaluate('changes'), [{'rowId':1,'kind':'performer'}])
        self.handlers['/api/favourites/lock'] = lambda _: (500, {'error':'fixture failure'})
        button.click()
        expect(button).to_be_enabled()
        expect(button).to_have_class('pp-name-action is-locked')
        self.assertEqual(self.page.evaluate('changes.length'), 1)

    def test_grid_lock_and_multiple_edits_on_return(self):
        rows = {i: {'id':i, 'kind':'performer', 'folder_name':f'Person {i}',
                    'name':f'Person {i}', 'matches_locked':False} for i in (1,2)}
        self.handlers['/api/favourites'] = lambda _: (200, {'performers':list(rows.values())})
        self.handlers['/api/favourites/row'] = lambda req: (200, {'row':rows[int(parse_qs(urlparse(req.url).query)['id'][0])]})
        def lock(req):
            import json
            data=json.loads(req.post_data)
            rows[data['id']]['matches_locked']=data['matches_locked']
            return 200, {'ok':True}
        self.handlers['/api/favourites/lock']=lock
        self.page.goto('http://top-shelf.test/static/stars.html')
        first=self.page.locator('.fav-cell[data-id="1"] .fav-card-lock')
        first.click()
        expect(first).to_have_attribute('data-locked','1')
        self.page.wait_for_function("JSON.parse(localStorage.getItem('ts:library-change')).rowId===1")
        # Another document makes two edits before the library becomes active again.
        other=self.page.context.new_page()
        other.route('**/*',self.route)
        other.goto('http://top-shelf.test/fixture')
        other.add_script_tag(path=str(__import__('smoke').ROOT/'static/ts-utils.js'))
        rows[1]['matches_locked']=False
        rows[2]['matches_locked']=True
        other.evaluate("for (const rowId of [1,2]) document.dispatchEvent(new CustomEvent('library-row-updated',{detail:{rowId,kind:'performer'}}))")
        changes=other.evaluate("JSON.parse(localStorage.getItem('ts:library-change')).changes")
        self.assertEqual([c['rowId'] for c in changes][-2:],[1,2])
        self.page.evaluate("window.dispatchEvent(new Event('focus'))")
        expect(first).to_have_attribute('data-locked','0')
        expect(self.page.locator('.fav-cell[data-id="2"] .fav-card-lock')).to_have_attribute('data-locked','1')
        other.close()

if __name__ == '__main__':
    suite=unittest.TestSuite(RefreshFlows(name) for name in (
        'test_popup_lock_notifies_and_failed_save_does_not',
        'test_grid_lock_and_multiple_edits_on_return'))
    result=unittest.TextTestRunner().run(suite)
    raise SystemExit(not result.wasSuccessful())
