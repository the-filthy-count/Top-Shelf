import unittest
from smoke import BrowserFlows, expect
class SourceFilters(BrowserFlows):
 def test_multiple_sources_and_saved_selection(self):
  for page,kind in [('stars.html','performer'),('studios.html','studio')]:
   with self.subTest(kind=kind):
    self.handlers['/api/metadata/search']=lambda _:(200,{'results':[{'id':str(i),'name':'Fixture '+source,'source':source} for i,source in enumerate(['TPDB','StashDB','FansDB','JAVStash'])]})
    self.page.goto('http://top-shelf.test/static/'+page)
    self.page.locator('#segSearch').click()
    self.page.locator('#favSearchQ').fill('Fixture')
    self.page.locator('#favSearchQ').press('Enter')
    expect(self.page.locator('#favSearchResults .fav-cell')).to_have_count(4)
    for source in ('tpdb','fansdb'):
     self.page.locator('.fav-search-source[data-source="'+source+'"]').click()
    expect(self.page.locator('#favSearchResults .fav-cell')).to_have_count(2)
    self.page.locator('.fav-search-source[data-source="javstash"]').click()
    expect(self.page.locator('#favSearchResults .fav-cell')).to_have_count(1)
    self.page.locator('.fav-search-source[data-source="stashdb"]').click()
    expect(self.page.locator('#favSearchResults .fav-cell')).to_have_count(1)
    rects=self.page.evaluate("""() => {
      const input=document.querySelector('#favSearchQ').getBoundingClientRect();
      const group=document.querySelector('.fav-search-sources').getBoundingClientRect();
      return {inputRight:input.right, groupLeft:group.left, topDelta:Math.abs(input.top-group.top)};
    }""")
    self.assertGreaterEqual(rects['groupLeft'],rects['inputRight'])
    self.assertLess(rects['topDelta'],12)
    self.page.reload()
    self.page.locator('#segSearch').click()
    expect(self.page.locator('.fav-search-source[aria-pressed="true"]')).to_have_count(1)
    expect(self.page.locator('.fav-search-source[data-source="stashdb"]')).to_have_attribute('aria-pressed','true')
if __name__ == '__main__':
 result=unittest.TextTestRunner().run(unittest.TestSuite([SourceFilters('test_multiple_sources_and_saved_selection')]))
 raise SystemExit(not result.wasSuccessful())
