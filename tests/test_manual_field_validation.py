import unittest
import re
from datetime import datetime
from test_filename_identity import load_function

class ManualFieldValidationTests(unittest.TestCase):
    def test_missing_and_invalid_fields(self):
        validate = load_function('main.py', '_manual_scene_field_errors', {'re':re, 'datetime':datetime})
        self.assertEqual(set(validate({})), {'title','studio','date'})
        for date in ['', '2026-02-30', 'not a date']:
            self.assertIn('date', validate({'title':'Scene','studio':{'name':'Studio'},'release_date':date}))
        self.assertEqual(validate({'title':'Scene','studio':{'name':'Studio'},'release_date':'2026-11-27'}), {})
