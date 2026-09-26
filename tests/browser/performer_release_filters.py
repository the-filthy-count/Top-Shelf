import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from smoke import BrowserFlows,expect
class Test(BrowserFlows):
 def test_filters(self):
  data={'identity':{'canonical_name':'Primary Person','aliases':['Alias Person']},'bio':{},'library_status':{'in_library':True,'row_id':1}}
  queries=[]
  self.handlers['/api/performer/popup']=lambda _:(200,data)
  def search(req):
   queries.append(req.url)
   return 200,{'results':[{'title':'Alias Person keyword 1080p','guid':'1','type':'nzb'},{'title':'Alias Person other 720p','guid':'2','type':'nzb'}]}
  self.handlers['/api/prowlarr/search']=search
  self.script('ts-utils.js');self.script('performer-popup.js')
  self.page.evaluate("openPerformerPopup({libraryRowId:1,name:'Primary Person'})")
  expect(self.page.locator('.ts-prowlarr-row')).to_have_count(2)
  self.page.locator('[data-release-name="Alias Person"]').click()
  expect(self.page.locator('[data-search-name="Alias Person"]')).to_have_attribute('aria-pressed','true')
  expect(self.page.locator('.ts-prowlarr-row')).to_have_count(2)
  self.assertIn('Alias+Person',queries[-1])
  self.page.locator('[data-vice-filter]').click()
  self.page.locator('dialog input').fill('KEYWORD')
  self.page.get_by_role('button',name='Apply',exact=True).click()
  expect(self.page.locator('.ts-prowlarr-row')).to_have_count(1)
  self.page.locator('[data-vice-filter]').click()
  self.page.get_by_role('button',name='Clear',exact=True).click()
  expect(self.page.locator('.ts-prowlarr-row')).to_have_count(2)
  self.page.locator('[data-vice-filter]').click();self.page.keyboard.press('Escape')
  expect(self.page.locator('dialog')).to_have_count(0)
  expect(self.page.locator('.pp-cell-films')).to_be_visible()
unittest.TextTestRunner().run(unittest.TestSuite([Test('test_filters')]))
