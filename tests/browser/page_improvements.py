import unittest
from pathlib import Path
from smoke import BrowserFlows, expect

class PageImprovements(BrowserFlows):
 def test_library_issues_filter(self):
  rows=[{'id':1,'kind':'performer','folder_name':'Missing','image_url':'','bio_missing_fields':['birthdate']}, {'id':2,'kind':'performer','folder_name':'Complete','image_url':'/fixture.svg','bio_missing_fields':[]}]
  self.handlers['/api/favourites']=lambda _:(200,{'performers':rows})
  self.page.goto('http://top-shelf.test/static/stars.html')
  expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)
  self.page.locator('#libraryFilterButton').click()
  self.page.locator('#libraryIssueFilter').get_by_role('button',name='Missing artwork',exact=True).click()
  expect(self.page.locator('#favGrid .fav-cell')).to_have_count(1)
  expect(self.page.locator('#favGrid')).to_contain_text('Missing')
  self.page.locator('#libraryFilterButton').click()
  self.page.locator('#libraryIssueFilter').get_by_role('button',name='Missing bio fields (stars)',exact=True).click()
  expect(self.page.locator('#favGrid .fav-cell')).to_have_count(1)
  self.page.locator('#libraryFilterButton').click()
  self.page.locator('#libraryIssueFilter').get_by_role('button',name='All records',exact=True).click()
  expect(self.page.locator('#favGrid .fav-cell')).to_have_count(2)
 def test_home_affinity_and_reversible_dismiss(self):
  self.handlers['/api/home/data']=lambda _:(200,{'discovered':[{'name':'Popular','sources':{'stashdb':50},'affinity_sources':{}},{'name':'Relevant','sources':{'stashdb':1},'affinity_sources':{'stashdb':2}}]})
  self.page.goto('http://top-shelf.test/static/home.html')
  expect(self.page.locator('#homeDiscoveredGrid .fav-cell').first).to_have_attribute('data-name','Relevant')
  self.page.locator('#homeDiscoveredGrid .fav-cell').first.hover()
  self.page.locator('#homeDiscoveredGrid .home-disc-dismiss').first.click()
  expect(self.page.locator('#homeDiscoveredGrid .fav-cell')).to_have_count(1)
  self.page.locator('#restoreDiscovered').click()
  expect(self.page.locator('#homeDiscoveredGrid .fav-cell')).to_have_count(2)
 def test_profile_ownership_filters(self):
  self.script('ts-utils.js')
  self.handlers['/api/library/scenes-in']=lambda _:(200,{'matches':{'tpdb:owned':{'row_id':1}}})
  self.page.evaluate("""async () => {const host=document.createElement('div');host.id='testProfile';host.innerHTML='<div class="pp-scenes-grid"><div class="scene-card">Owned</div><div class="scene-card"><button class="scene-wanted-btn is-wanted"></button>Wanted</div><div class="scene-card">Available</div></div>';document.body.append(host);await tsProfileOwnershipFilters(host,[{id:'owned',source:'tpdb'},{id:'wanted',source:'tpdb'},{id:'available',source:'tpdb'}]);}""")
  self.page.locator('#testProfile select').select_option('Owned')
  expect(self.page.locator('#testProfile .scene-card:visible')).to_have_count(1)
  expect(self.page.locator('#testProfile .scene-card:visible')).to_have_text('Owned')
  self.page.locator('#testProfile select').select_option('Available')
  expect(self.page.locator('#testProfile .scene-card:visible')).to_have_text('Available')

 def test_discovery_preferences(self):
  self.page.goto('http://top-shelf.test/discover')
  self.page.evaluate("document.body.innerHTML='<div id=\"scenesGrid\"><div class=\"scene-card\" data-scene-i=\"0\">Owned</div><div class=\"scene-card\" data-scene-i=\"1\">New</div></div>'")
  self.handlers['/api/library/scenes-in']=lambda _:(200,{'matches':{'tpdb:owned':{'row_id':1}}})
  source=(Path(__file__).resolve().parents[2]/'static/scenes-common.js').read_text()
  start=source.index('  let _discoveryHidden')
  end=source.index('  window.decorateLibraryMatches = decorateLibraryMatches;')+len('  window.decorateLibraryMatches = decorateLibraryMatches;')
  self.page.add_script_tag(content=source[start:end])
  self.page.evaluate("decorateLibraryMatches([{id:'owned',source:'tpdb'},{id:'new',source:'tpdb'}])")
  self.page.get_by_role('button',name='Hide already owned',exact=True).click()
  expect(self.page.locator('#scenesGrid .scene-card:visible')).to_have_count(1)
  self.page.locator('#scenesGrid .scene-card:visible .discovery-dismiss').click()
  expect(self.page.locator('#scenesGrid .scene-card:visible')).to_have_count(0)
  self.page.get_by_role('button',name='Restore hidden',exact=True).click()
  expect(self.page.locator('#scenesGrid .scene-card:visible')).to_have_count(1)
  self.page.get_by_role('button',name='Show already owned',exact=True).click()
  expect(self.page.locator('#scenesGrid .scene-card:visible')).to_have_count(2)

if __name__=='__main__':
 names=['test_discovery_preferences','test_library_issues_filter','test_home_affinity_and_reversible_dismiss','test_profile_ownership_filters']
 result=unittest.TextTestRunner().run(unittest.TestSuite(PageImprovements(n) for n in names))
 raise SystemExit(not result.wasSuccessful())
