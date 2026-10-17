import sys,unittest
from datetime import date
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from smoke import BrowserFlows
from library_stats import profile
from playwright.sync_api import expect
class InsightsFlow(BrowserFlows):
 def test_trait_analysis(self):
  p=profile([{'name':'A','height':'165','weight':'60','hair_color':'brown','measurements':'34C-26-36'},{'name':'B','height':'175','weight':'70','hair_color':'blonde'}],date.today())
  data={'generated_at':'2026-10-17T12:00:00Z','totals':{'scenes':2,'performers':2,'studios':1,'tags':1},'coverage':{},'profiles':{'mix':p},'performers':[],'studios':[],'tags':[],'months':[],'release_months':[]}
  self.handlers['/api/library/insights']=lambda _:(200,data)
  errors=[];self.page.on('pageerror',lambda e:errors.append(str(e)))
  self.page.goto('http://top-shelf.test/static/insights.html')
  self.page.get_by_role('button',name='Analyse Height',exact=True).click()
  popup=self.page.get_by_role('dialog')
  expect(popup).to_be_visible();expect(popup).to_contain_text('Standard deviation')
  expect(popup).to_contain_text('Highest five')
  expect(popup.get_by_role('combobox')).to_have_value('')
  popup.get_by_role('combobox').select_option('female')
  expect(popup).to_contain_text('National mean')
  self.page.screenshot(path='/tmp/insights-analysis-desktop.png')
  self.page.keyboard.press('Escape');expect(popup).not_to_be_visible()
  expect(self.page.get_by_role('button',name='Analyse Height',exact=True)).to_be_focused()
  self.page.set_viewport_size({'width':390,'height':844})
  self.page.get_by_role('button',name='Analyse hair color',exact=True).click()
  expect(popup.locator('.analysis-pie')).to_be_visible();expect(popup).to_contain_text('50.0%')
  self.assertTrue(popup.evaluate('(el)=>el.scrollWidth<=el.clientWidth+1'))
  self.page.screenshot(path='/tmp/insights-analysis-mobile.png')
  popup.get_by_role('button',name='Close analysis').click();expect(popup).not_to_be_visible()
  self.assertTrue(any(path=='/static/vendor/bodyapps/female.json' for _,path,_ in self.calls))
  self.assertFalse(any(path=='/static/vendor/bodyapps/male.json' for _,path,_ in self.calls))
  self.assertEqual(errors,[])
if __name__=='__main__':unittest.main(verbosity=2)
