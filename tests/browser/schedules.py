"""Run: python tests/browser/schedules.py ScheduleFlow.test_controls_move_save_and_reload"""
import json
import sys
import unittest
from playwright.sync_api import expect
from smoke import BrowserFlows, ROOT
sys.path.insert(0,str(ROOT))
from scheduling import fields

class ScheduleFlow(BrowserFlows):
    def test_controls_move_save_and_reload(self):
        saved={'retry_enabled':'true','retry_frequency_h':'6','tpdb_sync_frequency_h':'168'}
        def settings(request):
            if request.method=='POST':saved.update(json.loads(request.post_data)['settings']);return 200,{'ok':True}
            return 200,{'settings':saved,'directories':[]}
        self.handlers['/api/settings']=settings
        self.handlers['/api/schedules']=lambda _:(200,{'timezone':'UTC','fields':[
            {'key':key,'label':label,'unit':unit,'default':str(default),'min':low,'max':high,'value':saved.get(key,str(default)),'next_run':None}
            for key,label,unit,default,low,high,_ in fields()]})
        self.page.goto('http://top-shelf.test/static/settings.html')
        self.page.locator('button[data-settings-category="schedule"]').click()
        panel=self.page.locator('.settings-category-panel[data-settings-category="schedule"]')
        expect(panel.locator('#cfgRetryFreq')).to_have_value('6')
        expect(panel.locator('#cfgTpdbSyncFreq')).to_have_value('168')
        expect(panel.locator('#cfgHealthPhash3IntervalDays')).to_have_count(1)
        expect(self.page.locator('#cfgRetryEnabled')).to_have_count(1)
        interval=panel.locator('[data-schedule-key="schedule_pending_check_interval"]')
        self.page.wait_for_function('_settingsPageLoaded')
        interval.fill('0');self.page.evaluate('saveSettings()')
        self.assertFalse(any(method=='POST' and path=='/api/settings' for method,path,_ in self.calls))
        interval.fill('90')
        self.page.evaluate('saveSettings()')
        self.assertEqual(saved['schedule_pending_check_interval'],'90')
        self.assertEqual(saved['retry_frequency_h'],'6')
        self.page.reload()
        self.page.locator('button[data-settings-category="schedule"]').click()
        expect(self.page.locator('[data-schedule-key="schedule_pending_check_interval"]')).to_have_value('90')

if __name__=='__main__':unittest.main(verbosity=2)
