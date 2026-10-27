import unittest
from unittest.mock import Mock
from tpdb_joint_scenes import joint_scenes

class TPDBJointScenesTests(unittest.TestCase):
    def test_pages_and_canonical_parent_ids(self):
        wrong={'id':'wrong','performers':[{'id':'a','name':'A'},{'id':'other','name':'B'}]}
        right={'id':'right','title':'Scene','performers':[{'id':'site-a','parent':{'id':'a','name':'A'}},{'id':'site-b','parent':{'id':'b','name':'B'}}]}
        get=Mock(side_effect=[Mock(json=lambda:{'data':[wrong],'meta':{'last_page':2}}),Mock(json=lambda:{'data':[right,right],'meta':{'last_page':2}})])
        rows,complete=joint_scenes(['a','b'],'key',get)
        self.assertTrue(complete);self.assertEqual([r['id'] for r in rows],['right'])
        self.assertEqual([p['performer']['id'] for p in rows[0]['performers']],['a','b'])
        self.assertEqual([c.kwargs['params']['page'] for c in get.call_args_list],[1,2])

    def test_bound_and_malformed_payload_are_not_successful_misses(self):
        get=Mock(return_value=Mock(json=lambda:{'data':[],'meta':{'last_page':5}}))
        self.assertEqual(joint_scenes(['a','b'],'key',get,max_pages=1),([],False))
        get.return_value.json=lambda:{'data':None}
        with self.assertRaises(ValueError):joint_scenes(['a','b'],'key',get)
