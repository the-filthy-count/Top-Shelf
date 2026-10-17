"""Full Settings-page regression: Magpie saves before unrelated slow requests."""
import unittest
from smoke import BrowserFlows
from playwright.sync_api import expect

class MagpieSettingsFlow(BrowserFlows):
    def test_save_before_unrelated_settings_work(self):
        self.handlers['/api/settings']=lambda r:(200,{'settings':{},'directories':[]})
        self.handlers['/api/magpie/config']=lambda r:(200,{'url':'http://nas','token_set':True,'categories':{},'category_labels':{'stars':'Stars'}})
        self.handlers['/api/magpie/destinations']=lambda r:(200,[])
        self.handlers['/api/magpie/test']=lambda r:(200,[])
        self.page.goto('http://top-shelf.test/static/settings.html')
        self.page.wait_for_function('_settingsPageLoaded && typeof saveMagpieSettings === "function"')
        self.page.locator('button[data-settings-category="downloads"]').click()
        self.page.locator('#magpieSave').click()
        expect(self.page.locator('#magpieTestStatus')).to_have_text('Connected')
        self.page.locator('#magpieToken').fill('magpie_fixture')
        self.page.get_by_role('button',name='Save',exact=True).click()
        expect(self.page.locator('#magpieToken')).to_have_value('')
        posts=[path for method,path,_ in self.calls if method=='POST']
        self.assertIn('/api/magpie/config',posts)
        # Main settings processing must never block Magpie persistence.
        if '/api/settings' in posts:
            self.assertLess(posts.index('/api/magpie/config'),posts.index('/api/settings'))
        if '/api/vices' in posts:
            self.assertLess(posts.index('/api/magpie/config'),posts.index('/api/vices'))

    def test_save_without_separate_magpie_script(self):
        self.handlers['/api/settings']=lambda r:(200,{'settings':{},'directories':[]})
        self.handlers['/api/magpie/config']=lambda r:(200,{'url':'http://nas','token_set':True,'categories':{},'category_labels':{'stars':'Stars'}})
        self.handlers['/api/magpie/destinations']=lambda r:(200,[])
        attempts=[]
        def script_route(route):
            attempts.append(1)
            route.abort()
        self.page.route('**/static/magpie.js?*',script_route)
        self.page.goto('http://top-shelf.test/static/settings.html')
        self.page.wait_for_function('_settingsPageLoaded')
        self.assertEqual(self.page.evaluate('typeof window.saveMagpieSettings'),'function')
        self.page.locator('button[data-settings-category="downloads"]').click()
        self.page.locator('#magpieBase').fill('http://nas')
        self.page.locator('#magpieToken').fill('magpie_fixture')
        self.page.get_by_role('button',name='Save',exact=True).click()
        expect(self.page.locator('#magpieToken')).to_have_value('')
        self.assertEqual(len(attempts),0)
        self.assertTrue(any(method=='POST' and path=='/api/magpie/config' for method,path,_ in self.calls))
        expect(self.page.locator('#magpieToken')).to_have_attribute('placeholder','••••••••••••')

    def test_download_row_alignment_and_loading(self):
        self.handlers['/api/magpie/config']=lambda r:(200,{'url':'http://nas','token_set':True,'categories':{'stars':'photos'},'category_labels':{'stars':'Stars'}})
        self.handlers['/api/downloads']=lambda r:(200,{'items':[]})
        attempts=[]
        def block_script(route):
            attempts.append(1)
            route.abort()
        self.page.route('**/static/magpie.js*',block_script)
        self.page.goto('http://top-shelf.test/static/downloads.html')
        expect(self.page.locator('#magpieUrl')).to_be_visible()
        expect(self.page.locator('#magpieSubmit')).to_be_enabled()
        for width in (1920,1280,768,390):
            self.page.set_viewport_size({'width':width,'height':900})
            boxes=self.page.evaluate("""() => {
                const rect=s=>{const r=document.querySelector(s).getBoundingClientRect();return {left:r.left,right:r.right};};
                return [rect('#magpieForm'),rect('#dlStatCardTotal'),rect('#dlStatCardBandwidth')];
            }""")
            self.assertAlmostEqual(boxes[0]['left'],boxes[1]['left'],delta=1)
            self.assertAlmostEqual(boxes[0]['right'],boxes[2]['right'],delta=1)
        self.assertEqual(attempts,[])
        self.handlers['/api/magpie/config']=lambda r:(200,{'url':'','token_set':False,'categories':{},'category_labels':{}})
        self.page.reload()
        expect(self.page.locator('#magpieAdd')).to_be_hidden()

if __name__=='__main__': unittest.main(verbosity=2)
