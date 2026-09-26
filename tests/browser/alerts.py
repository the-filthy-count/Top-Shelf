"""Run with python tests/browser/alerts.py."""
import json
import unittest
from playwright.sync_api import expect
from smoke import BrowserFlows, ROOT

class AlertFlow(BrowserFlows):
    def test_dismissal_and_expiry(self):
        notes=[{'id':101,'kind':'error','message':'Test failure'}]
        self.handlers['/api/activity/banner']=lambda _:(200,{'notifications':notes})
        self.handlers['/api/activity/notifications/dismiss']=lambda _:(200,{'ok':True})
        self.page.clock.install()
        self.page.set_content('<div class="ts-sidebar__activity-slot"><div id="tsActivityBanner"></div></div>')
        self.page.add_style_tag(path=str(ROOT/'static/app-shell.css'))
        self.script('activity-banner.js')
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(1)
        icon=self.page.locator('.ts-sidebar-notif__symbol')
        expect(icon).to_have_css('opacity','1')
        self.page.locator('.ts-sidebar-notif').hover()
        expect(icon).to_have_css('opacity','0')
        self.page.get_by_role('button',name='Dismiss alert').click()
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(0)
        self.page.evaluate('TsActivity.refresh()')
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(0)
        notes[:]=[{'id':102,'kind':'error','message':'Timeout failure'}]
        self.page.evaluate('TsActivity.refresh()')
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(1)
        self.page.clock.fast_forward(10000)
        self.page.evaluate('TsActivity.refresh()')
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(1)
        self.page.clock.fast_forward(11000)
        expect(self.page.locator('.ts-sidebar-notif')).to_have_count(0)
        self.assertTrue(any(path=='/api/activity/notifications/dismiss' and json.loads(body)['id']==102 for _,path,body in self.calls))

if __name__=='__main__':unittest.main(verbosity=2)
