import unittest
from download_lifecycle import lifecycle
class Lifecycle(unittest.TestCase):
 def test_completed_is_not_filed(self):
  self.assertEqual(lifecycle({'progress_pct':100})['stage'],'Awaiting import')
 def test_unpack_is_not_complete(self):
  self.assertEqual(lifecycle({'progress_pct':100,'status':'UNPACKING'})['stage'],'Post-processing')
 def test_cleanup_and_disabled_setting(self):
  item={'filing':{'filed':True,'match_source':'exact'}}
  self.assertEqual(lifecycle(item,True)['stage'],'Cleanup pending')
  self.assertEqual(lifecycle(item,False)['stage'],'Filed')
 def test_fuzzy_is_not_confirmed(self):
  self.assertEqual(lifecycle({'filing':{'filed':True,'match_source':'normalized_prefix'}},True)['stage'],'Possible library match')
 def test_failure_wins(self):
  self.assertEqual(lifecycle({'failed':True,'progress_pct':100})['stage'],'Failed')
if __name__=='__main__':unittest.main()
