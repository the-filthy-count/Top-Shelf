import unittest
from smoke import BrowserFlows, expect

class IafdReview(BrowserFlows):
 def test_shortlist_loads_breakdowns_without_selecting_ambiguous_scene(self):
  self.handlers['/api/iafd/movie-breakdown']=lambda _:(200,{'all_scenes':[{'number':1,'cast':['Fixture']},{'number':2,'cast':['Fixture']}],'movie_cast':[]})
  self.page.goto('http://top-shelf.test/static/queue.html')
  self.page.evaluate("""() => {
    _iafdBulkPerformer='Fixture';
    _renderIafdBulkResults({
      groups:[], unmatched:['Fixture/film.mp4'],
      filmography:[{url:'https://www.iafd.com/title.rme/id=fixture',title:'Fixture Film',year:'2005'}],
      suggestions:{'Fixture/film.mp4':[{url:'https://www.iafd.com/title.rme/id=fixture',title:'Fixture Film',year:'2005',reasons:['Title similarity 80%','Year matches']}]}
    });
  }""")
  choice=self.page.locator('#qIafdResultsGroups label select')
  expect(choice).to_contain_text('Year matches')
  choice.select_option('https://www.iafd.com/title.rme/id=fixture')
  self.page.wait_for_function("_iafdBulkGroups[0].files[0]._all_scenes.length === 2")
  self.assertEqual(self.page.evaluate('_iafdBulkGroups[0].files[0].scene_idx'),-1)
  expect(self.page.locator('#iafdBulkTick-0-0')).not_to_be_checked()
  self.assertTrue(any(path=='/api/iafd/movie-breakdown' for _,path,_ in self.calls))

if __name__=='__main__':
 result=unittest.TextTestRunner().run(unittest.TestSuite([IafdReview('test_shortlist_loads_breakdowns_without_selecting_ambiguous_scene')]))
 raise SystemExit(not result.wasSuccessful())
