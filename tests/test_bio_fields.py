"""Test bio fields against SQLite without starting the app."""
import ast
from pathlib import Path
import sqlite3
import unittest

class BioFieldTests(unittest.TestCase):
    def setUp(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'database.py').read_text())
        self.conn=sqlite3.connect(':memory:');self.conn.row_factory=sqlite3.Row
        self.conn.execute('CREATE TABLE favourite_entities(id INTEGER PRIMARY KEY, kind TEXT)')
        self.conn.execute("INSERT INTO favourite_entities VALUES(1,'performer')")
        schema=next(n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str) and 'CREATE TABLE IF NOT EXISTS performer_bio_fields' in n.value)
        self.conn.execute(schema)
        nodes=[n for n in tree.body if (isinstance(n,ast.FunctionDef) and n.name in ('performer_bio_merge','performer_bio_set','performer_bio_set_many','_validate_performer_bio_field')) or (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='PERFORMER_BIO_FIELDS' for t in n.targets))]
        ns={'get_conn':lambda:self.conn}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'bio','exec'),ns)
        self.merge=ns['performer_bio_merge'];self.save=ns['performer_bio_set'];self.save_many=ns['performer_bio_set_many']

    def tearDown(self):self.conn.close()

    def test_fields_saved_independently_and_manual_edits_survive_refresh(self):
        self.merge(1,{'country':'A','height':'170'})
        self.save(1,'country','B')
        result=self.merge(1,{'country':'C','height':'180'})
        self.assertEqual(result['country'],'B');self.assertEqual(result['height'],'180')
        self.assertEqual(self.conn.execute("SELECT value FROM performer_bio_fields WHERE row_id=1 AND field='country'").fetchone()[0],'B')

    def test_intentionally_empty_field_survives_refresh(self):
        self.save(1,'country','')
        self.assertEqual(self.merge(1,{'country':'C'})['country'],'')

    def test_invalid_field_value_and_performer_are_rejected(self):
        for rid,field,value in [(1,'bad','x'),(1,'height',[]),(1,'height','x'*20001),(99,'height','170')]:
            with self.assertRaises(ValueError):self.save(rid,field,value)

    def test_calendar_dates(self):
        for value in ('2023-02-29', '2024-13-01', '01/02/2000', '20000101', '9999-01-01'):
            with self.assertRaises(ValueError): self.save(1, 'birthdate', value)
        self.save(1, 'birthdate', '2000-02-29')
        self.assertEqual(self.merge(1, {})['birthdate'], '2000-02-29')
        self.save(1, 'death_date', '')

    def test_birthdate_requires_eighteen(self):
        from datetime import date, timedelta
        today = date.today()
        try:
            cutoff = today.replace(year=today.year-18)
        except ValueError:
            cutoff = today.replace(year=today.year-18, day=28)
        self.save(1, 'birthdate', cutoff.isoformat())
        with self.assertRaisesRegex(ValueError, 'at least 18'):
            self.save(1, 'birthdate', (cutoff+timedelta(days=1)).isoformat())
        with self.assertRaisesRegex(ValueError, 'at least 18'):
            self.save(1, 'birthdate', today.isoformat())
        self.save(1, 'death_date', today.isoformat())

    def test_career_years(self):
        from datetime import date
        for field in ('career_start_year', 'career_end_year'):
            for value in ('99', '2000.5', 'abcd', '1899', str(date.today().year+1)):
                with self.assertRaises(ValueError): self.save(1, field, value)
            for value in ('1900', str(date.today().year), ''):
                self.save(1, field, value)

    def test_batch_validation_is_atomic(self):
        self.save(1, 'country', 'US')
        with self.assertRaises(ValueError):
            self.save_many(1, {'country':'GB','birthdate':'invalid'})
        self.assertEqual(self.merge(1,{})['country'],'US')
        self.save_many(1, {'country':'GB','height':'180 cm'})
        result=self.merge(1,{})
        self.assertEqual((result['country'],result['height']),('GB','180 cm'))

if __name__=='__main__':unittest.main()
