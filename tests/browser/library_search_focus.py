import unittest
from smoke import BrowserFlows,expect

class SearchFocus(BrowserFlows):
 def test_focus_preserves_pending_and_completed_search(self):
  rows=[]
  self.handlers['/api/favourites']=lambda _:(200,{'performers':list(rows)})
  self.page.goto('http://top-shelf.test/static/stars.html')
  self.page.locator('#segSearch').click()
  pending=[]
  self.page.route('**/api/metadata/search?*',lambda route:pending.append(route))
  self.page.locator('#favSearchQ').fill('Fixture')
  self.page.locator('#favSearchQ').press('Enter')
  self.page.wait_for_timeout(150)
  self.assertEqual(len(pending),1)
  self.page.evaluate("""() => {
    window.dispatchEvent(new Event('focus'));
    document.dispatchEvent(new Event('visibilitychange'));
  }""")
  self.page.wait_for_timeout(150)
  self.assertEqual(len(pending),1)
  pending[0].fulfill(json={'results':[{'id':'fixture-id','name':'Fixture','source':'StashDB'}]})
  expect(self.page.locator('#favSearchResults .fav-cell')).to_have_count(1)
  self.page.evaluate("window.originalSearchTile = document.querySelector('#favSearchResults .fav-cell')")
  self.page.evaluate("window.dispatchEvent(new Event('focus')); document.dispatchEvent(new Event('visibilitychange'))")
  self.page.wait_for_timeout(250)
  self.assertTrue(self.page.evaluate("window.originalSearchTile === document.querySelector('#favSearchResults .fav-cell')"))
  self.assertEqual(len(pending),1)
  rows.append({'id':1,'kind':'performer','folder_name':'Fixture','match_stashdb_id':'fixture-id'})
  self.page.evaluate("""() => {
    document.dispatchEvent(new CustomEvent('library-row-added',{detail:{kind:'performer',rowId:1}}));
    window.dispatchEvent(new Event('focus'));
  }""")
  expect(self.page.locator('#favSearchResults .fav-search-tile-badge')).to_have_count(1)
  self.page.wait_for_timeout(150)
  self.assertEqual(len(pending),1)
  expect(self.page.locator('#favSearchQ')).to_have_value('Fixture')
  expect(self.page.locator('#favSearchResults .fav-cell--skeleton')).to_have_count(0)

if __name__=='__main__':
 result=unittest.TextTestRunner().run(unittest.TestSuite([SearchFocus('test_focus_preserves_pending_and_completed_search')]))
 raise SystemExit(not result.wasSuccessful())
