"""Discover performer popups and their nested image manager, using isolated APIs."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from smoke import BrowserFlows, ROOT, expect

class DiscoverPerformer(BrowserFlows):
    def load_popup(self, local=False):
        self.data = {'identity': {'canonical_name': 'Example Person', 'tpdb_id': 'remote-id', 'aliases': ['Another Name']},
                     'library_status': {'in_library': local, 'row_id': 7 if local else None},
                     'headshot_url': '/fixture.svg', 'images': []}
        self.handlers['/api/performer/popup'] = lambda _: (200, self.data)
        self.page.goto('http://top-shelf.test/discover')
        self.script('performer-popup.js')

    def test_remote_image_shapes(self):
        self.load_popup()
        self.data['headshot_url'] = {'url': '/fixture.svg'}
        self.data['images'] = [None, {'url': {'url': '/fixture.svg'}}, '/fixture.svg?second', {}]
        self.page.evaluate("openPerformerPopup({tpdbId:'remote-id'})")
        expect(self.page.locator('.pp-name-text')).to_have_text('Example Person')
        expect(self.page.locator('.pp-stage-banner')).to_have_count(0)
        expect(self.page.locator('.pp-headshot')).to_have_attribute('src', '/fixture.svg')
        expect(self.page.locator('.pp-poster-slot img')).to_have_count(2)
        expect(self.page.locator('.pp-error')).to_have_count(0)

    def test_render_error_has_retry(self):
        self.load_popup()
        self.page.evaluate("() => { window.countryFlagHtml=()=>{throw new Error('Bad profile field')}; }")
        self.data['bio'] = {'country': 'US'}
        self.page.evaluate("openPerformerPopup({tpdbId:'remote-id'})")
        expect(self.page.get_by_role('alert')).to_contain_text('Bad profile field')
        expect(self.page.locator('.pp-stage-banner')).to_have_count(0)
        self.page.evaluate('delete window.countryFlagHtml')
        self.page.get_by_role('button', name='Retry', exact=True).click()
        expect(self.page.locator('.pp-headshot')).to_be_visible()
        expect(self.page.locator('.pp-error')).to_have_count(0)

    def test_timeout_and_switch(self):
        self.load_popup()
        self.page.evaluate('''() => {
          const realFetch=window.fetch;
          window.fetch=(url,opts)=>url.includes('tpdb_id=stall') ? new Promise((resolve,reject)=>{
            opts.signal.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')));
          }) : realFetch(url,opts);
        }''')
        self.page.clock.install()
        self.page.evaluate("void openPerformerPopup({tpdbId:'stall'})")
        self.page.clock.fast_forward(45001)
        expect(self.page.get_by_role('alert')).to_contain_text('timed out')
        self.page.evaluate("void openPerformerPopup({tpdbId:'stall'})")
        self.page.evaluate("openPerformerPopup({tpdbId:'remote-id'})")
        expect(self.page.locator('.pp-error')).to_have_count(0)
        expect(self.page.locator('.pp-name-text')).to_have_text('Example Person')

    def test_manager_from_discover(self):
        self.load_popup(local=True)
        for file in ['app-shell.css', 'scenes.css', 'mobile.css', 'design-system.css']:
            self.page.add_style_tag(path=str(ROOT/'static'/file))
        self.script('poster-role-picker.js')
        self.handlers['/api/performers/poster-roles'] = lambda _: (200, {'candidates': [{'image':'/fixture.svg','name':'Example','source':'TPDB'}]})
        self.page.evaluate("openPerformerPopup({tpdbId:'remote-id'})")
        self.page.locator('.pp-headshot-wrap').click()
        expect(self.page.locator('#posterRoleModal')).to_be_visible()
        expect(self.page.locator('#posterRoleCandidates button')).to_have_count(1)
        layout=self.page.locator('.fav-roles-layout')
        self.assertEqual(layout.evaluate('el=>getComputedStyle(el).display'), 'grid')
        self.assertEqual(len(layout.evaluate('el=>getComputedStyle(el).gridTemplateColumns.split(" ")')),3)
        self.assertEqual(self.page.locator('#cropViewport').evaluate('el=>getComputedStyle(el).position'),'relative')
        self.assertEqual(self.page.locator('#cropImg').evaluate('el=>getComputedStyle(el).position'),'absolute')
        self.assertIn('/discover',self.page.url)
        self.page.locator('#posterRoleCandidates button').click()
        expect(self.page.locator('#posterRoleSetPrimaryBtn')).to_be_enabled()
        self.page.locator('#posterRoleSetPrimaryBtn').click()
        expect(self.page.locator('#cropModal')).to_be_visible()
        self.page.wait_for_function("document.querySelector('#cropImg').naturalWidth > 0 && document.querySelector('#cropImg').style.transform")
        self.assertGreater(self.page.locator('#cropViewport').bounding_box()['width'],100)
        image=self.page.locator('#cropImg').bounding_box()
        viewport=self.page.locator('#cropViewport').bounding_box()
        self.assertGreaterEqual(image['width'], viewport['width']-3)
        self.assertGreaterEqual(image['height'], viewport['height']-3)
        self.page.screenshot(path='/tmp/discover-image-manager-test.png')

if __name__ == '__main__':
    names=[name for name in DiscoverPerformer.__dict__ if name.startswith('test_')]
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(DiscoverPerformer(name) for name in names))
    sys.exit(not result.wasSuccessful())
