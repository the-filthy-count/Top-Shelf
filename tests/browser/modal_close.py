"""Real modal scripts with fixture APIs; no live library changes."""
import unittest
from smoke import BrowserFlows, ROOT, expect

class ModalClose(BrowserFlows):
    def route(self, route):
        if route.request.url == 'http://top-shelf.test/performer/1':
            route.fulfill(path=str(ROOT/'static/entity.html'))
            return
        super().route(route)

    def prepare(self):
        self.handlers['/api/performer/popup'] = lambda _: (200, {
            'identity': {'canonical_name': 'Test Person'}, 'bio': {},
            'library_status': {'in_library': True, 'row_id': 1}})

    def assert_manager_closed(self):
        expect(self.page.locator('#posterRoleModal')).not_to_be_visible()
        expect(self.page.locator('#performerPopupModal')).to_be_visible()
        self.assertEqual(self.page.evaluate("document.getElementById('posterRoleModal').style.display"), '')

    def test_entity_escape_and_close_button(self):
        self.prepare()
        self.page.goto('http://top-shelf.test/performer/1')
        for action in ('escape', 'click'):
            self.page.locator('.pp-headshot-wrap').click()
            expect(self.page.locator('#posterRoleModal')).to_be_visible()
            if action == 'escape':
                self.page.keyboard.press('Escape')
            else:
                self.page.locator('.fav-roles-modal-close').click()
            self.assert_manager_closed()
            expect(self.page).to_have_url('http://top-shelf.test/performer/1')

    def test_shared_escape_closes_only_manager_and_can_reopen(self):
        self.prepare()
        self.page.add_style_tag(path=str(ROOT/'static/app-shell.css'))
        self.page.add_style_tag(path=str(ROOT/'static/library.css'))
        self.script('app-shell.js')
        self.script('performer-popup.js')
        self.script('poster-role-picker.js')
        self.page.evaluate("openPerformerPopup({libraryRowId:1,name:'Test Person'})")
        for _ in range(2):
            self.page.evaluate("openPosterRolePicker(1,{name:'Test Person'})")
            expect(self.page.locator('#posterRoleModal')).to_be_visible()
            self.page.keyboard.press('Escape')
            self.assert_manager_closed()

if __name__ == '__main__':
    suite = unittest.TestSuite(ModalClose(name) for name in (
        'test_entity_escape_and_close_button',
        'test_shared_escape_closes_only_manager_and_can_reopen'))
    result = unittest.TextTestRunner().run(suite)
    raise SystemExit(not result.wasSuccessful())
