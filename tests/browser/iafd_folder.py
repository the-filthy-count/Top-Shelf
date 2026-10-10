import unittest,json
from smoke import BrowserFlows,expect
class IafdFolder(BrowserFlows):
 def test_folder_scope_and_missing_cast(self):
  self.handlers['/api/library/favourite/lookup-batch']=lambda req:(200,{'results':{name:{'kind':'performer' if name=='Natalie Norton' else 'unknown'} for name in json.loads(req.post_data)['names']}})
  scans=[]
  self.handlers['/api/iafd/bulk-scan']=lambda req:(scans.append(json.loads(req.post_data)) or (200,{'groups':[]}))
  self.page.goto('http://top-shelf.test/static/queue.html')
  self.page.evaluate("""() => {
    window._queueFiles=[
      {filename:'Natalie Norton/one.mp4',performers:''},
      {filename:'Natalie Norton/Pack/two.mp4',performers:'Someone Else'},
      {filename:'Natalie Norton Extra/three.mp4',performers:'Other Person'}
    ];
    _queueCurrentFolder='Natalie Norton';
  }""")
  self.page.evaluate('openIafdBulkPicker()')
  expect(self.page.locator('#qIafdPerformerList')).to_contain_text('Natalie Norton')
  entries=self.page.evaluate('window._iafdPerfEntries')
  self.assertEqual(len(entries),1)
  self.assertEqual(entries[0]['filenames'],['Natalie Norton/one.mp4','Natalie Norton/Pack/two.mp4'])
  self.page.locator('#qIafdPerformerList .qs-perf-row').click()
  self.page.wait_for_timeout(100)
  self.assertEqual(scans,[{'performer':'Natalie Norton','filenames':entries[0]['filenames']}])
  self.page.evaluate('closeIafdBulkResults()')
  self.page.evaluate("_queueCurrentFolder=''; openIafdBulkPicker()")
  self.assertEqual(len(self.page.evaluate('window._iafdPerfEntries')),2)
result=unittest.TextTestRunner().run(unittest.TestSuite([IafdFolder('test_folder_scope_and_missing_cast')]))
raise SystemExit(not result.wasSuccessful())
