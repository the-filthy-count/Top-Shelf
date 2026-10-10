import sqlite3
import unittest
from datetime import date
from library_stats import build, height_cm, profile

class InsightsTests(unittest.TestCase):
    def test_numeric_traits_and_missing_values(self):
        result=profile([{'height':'5 ft 6 in','birthdate':'2000-01-01'}, {'height':'1.8 m','birthdate':'bad'}, {'height':'unknown'}],date(2026,10,5))
        self.assertEqual(result['numeric']['height']['samples'],2)
        self.assertEqual(result['numeric']['height']['mean'],173.8)
        self.assertEqual(result['numeric']['age']['median'],26)
        self.assertEqual(result['numeric']['age']['samples'],1)
        self.assertIsNone(height_cm('5 ft 14 in'))
        self.assertIsNone(height_cm('999 cm'))
        self.assertEqual(profile([],date.today())['categorical']['eye_color']['values'],[])

    def test_numeric_mode_and_ties(self):
        from library_stats import numeric_mode
        self.assertEqual(numeric_mode([160, 160, 180]), {'mode':160, 'modes':[160]})
        self.assertEqual(numeric_mode([160, 160, 180, 180]), {'mode':None, 'modes':[160,180]})
        self.assertEqual(numeric_mode([160, 180]), {'mode':None, 'modes':[]})
        self.assertEqual(numeric_mode([]), {'mode':None, 'modes':[]})

    def test_categories_report_ties(self):
        p=profile([{'eye_color':'blue','ethnicity':'A'},{'eye_color':'Brown','ethnicity':'A'},{'eye_color':'unknown','ethnicity':'B'}],date.today())
        self.assertEqual(p['categorical']['ethnicity']['values'],['A'])
        self.assertEqual(p['categorical']['eye_color'],{'values':['Blue','Brown'],'count':1,'samples':2})

    def test_only_downloaded_scenes_and_deduplicated_tags(self):
        c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
        try:
            c.executescript('''
            CREATE TABLE favourite_entities(id INTEGER,kind TEXT,folder_name TEXT,aliases_json TEXT);
            INSERT INTO favourite_entities VALUES (1,'performer','Fixture','["Alias"]');
            CREATE TABLE performer_bio_fields(row_id INTEGER,field TEXT,value TEXT);
            CREATE TABLE processed_files(id INTEGER,destination TEXT,match_source TEXT,match_external_id TEXT,performers TEXT,match_studio TEXT,processed_at TEXT,status TEXT);
            INSERT INTO processed_files VALUES(1,'/a','tpdb','1','Fixture, Alias','Studio','2026-10-01','filed');
            INSERT INTO processed_files VALUES(2,'/b','tpdb','1','Fixture','Studio','2026-10-02','filed');
            INSERT INTO processed_files VALUES(3,'/c','tpdb','2','Other','Other','2026-10-03','pending');
            INSERT INTO processed_files VALUES(4,'/d','tpdb','3','Other','Other','2026-10-03','filed');
            CREATE TABLE library_files(destination TEXT,is_removed INTEGER);
            INSERT INTO library_files VALUES('/d',1);
            CREATE TABLE wanted_items(kind TEXT,source TEXT,external_id TEXT,tags_json TEXT);
            INSERT INTO wanted_items VALUES('scene','tpdb','1','["Tag", "tag"]');
            INSERT INTO wanted_items VALUES('scene','tpdb','2','["Unfiled"]');
            CREATE TABLE feed_display_pools(kind TEXT,payload_json TEXT);
            INSERT INTO feed_display_pools VALUES('scenes','[{"source":"tpdb","id":"1","tags":[{"name":"Tag"}]}]');
            ''')
            p=build(c,date(2026,10,5))
            self.assertEqual(p['totals']['scenes'],1)
            self.assertEqual(p['performers'],[{'name':'Fixture','count':1}])
            self.assertEqual(p['tags'],[{'name':'Tag','count':1}])
            self.assertEqual(p['coverage']['tags'],1)
            self.assertEqual(p['months'],[{'month':'2026-10','count':1}])
            c.execute("INSERT INTO processed_files VALUES(5,'/unknown','tpdb','5','Not Saved','Studio','2026-10-04','filed')")
            p=build(c,date(2026,10,5))
            self.assertEqual(p['totals']['scenes'],2)
            self.assertEqual(p['totals']['performers'],1)
            self.assertEqual(p['performers'],[{'name':'Fixture','count':1}])
        finally:c.close()

    def test_indexed_library_without_filing_history_and_filters(self):
        c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
        c.executescript("""
        CREATE TABLE favourite_entities(id INTEGER,kind TEXT,folder_name TEXT,path TEXT,root_label TEXT);
        INSERT INTO favourite_entities VALUES(1,'performer','Sasha Grey','/stars/Sasha Grey','Stars');
        INSERT INTO favourite_entities VALUES(2,'performer','Other','/other/Other','Other folder');
        CREATE TABLE performer_bio_fields(row_id INTEGER,field TEXT,value TEXT);
        CREATE TABLE library_files(destination TEXT,is_removed INTEGER,current_filename TEXT);
        CREATE TABLE processed_files(id INTEGER,destination TEXT,status TEXT,processed_at TEXT);
        CREATE TABLE wanted_items(kind TEXT,source TEXT,external_id TEXT,tags_json TEXT);
        CREATE TABLE feed_display_pools(kind TEXT,payload_json TEXT);
        CREATE TABLE settings(key TEXT,value TEXT);
        INSERT INTO settings VALUES('features_dir','/movies');
        """)
        for i in range(301):
            filename=f'Studio A - S10E0203 - Scene {i}.mp4'
            c.execute('INSERT INTO library_files VALUES(?,0,?)',('/stars/Sasha Grey/'+filename,filename))
        c.execute("INSERT INTO library_files VALUES('/stars/Sasha Grey/removed.mp4',1,'removed.mp4')")
        c.execute("INSERT INTO library_files VALUES('/other/Other/Studio B - S11E0405 - Another.mp4',0,'Studio B - S11E0405 - Another.mp4')")
        c.execute("INSERT INTO library_files VALUES('/movies/Film.mp4',0,'Film.mp4')")
        result=build(c)
        self.assertEqual(result['totals']['scenes'],302)
        self.assertEqual(result['performers'][0],{'name':'Sasha Grey','count':301})
        self.assertEqual(result['months'],[]) # No fabricated download dates for old files.
        self.assertEqual(result['release_months'][0],{'month':'2010-02','count':301})
        filtered=build(c,folders=['Stars'],studio_filters=['Studio A'])
        self.assertEqual(filtered['totals']['scenes'],301)
        self.assertEqual(filtered['totals']['performers'],1)
        self.assertEqual(build(c,folders=['Stars'],studio_filters=['Studio B'])['totals']['scenes'],0)
        self.assertEqual(result['filters']['folders'],['Other folder','Stars'])
        c.close()

    def test_gender_groups_only_present_library_genders(self):
        from library_stats import profiles
        groups=profiles([{'gender':'FEMALE','height':'160'}, {'gender':'TRANSGENDER_FEMALE','height':'180'}, {'gender':'unknown'}],date.today())
        self.assertEqual(set(groups),{'female','mix'})
        self.assertEqual(groups['female']['numeric']['height']['mean'],160)
        self.assertEqual(groups['mix']['numeric']['height']['mean'],180)
        self.assertEqual(profiles([{}],date.today()),{})

    def test_weight_and_measurement_units(self):
        from library_stats import weight_kg, measurements
        self.assertAlmostEqual(weight_kg('132.277 lbs'),60,places=2)
        self.assertEqual(weight_kg('125 lbs (57 kg)'),57)
        self.assertIsNone(weight_kg('unknown'))
        self.assertIsNone(weight_kg('-5'))
        self.assertAlmostEqual(measurements('34D-26-36')['waist'],66.04)
        self.assertEqual(measurements('86-66-91 cm')['hips'],91)
        self.assertNotIn('band',measurements('86-66-91 cm'))
        self.assertEqual(measurements('unknown'),{})
        p=profile([{'weight':'60 kg','measurements':'34D-26-36'},{}],date.today())
        self.assertEqual(p['numeric']['weight']['samples'],1)
        self.assertEqual(p['numeric']['band']['mean'],34)
        self.assertEqual(p['categorical']['cup_size']['values'],['D'])
