import ast
import re
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def load_function(path,name,env):
    tree=ast.parse((ROOT/path).read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec'),env)
    return env[name]

class FilenameIdentityTests(unittest.TestCase):
    def test_parent_exact_match_but_not_reversed_name(self):
        stem=load_function('app/core/matching.py','_release_name_with_folders',{'Path':Path})
        env={'re':re,'_normalize_release_for_match':lambda s:re.sub('[^a-z0-9]','',s.lower()),
             '_tokenize_for_match':lambda s:re.findall('[a-z0-9]+',s.lower())}
        guess=load_function('database.py','_library_guess_from_release_name',env)
        filename='Daphne Dare/Slim Blonde Taylor Dare Scene.mp4'
        index=[(('daphne','dare'),'Daphne Dare',1),(('dare','taylor'),'Dare Taylor',2)]
        self.assertEqual(guess(stem(filename),[],index)['performers'],['Daphne Dare'])
        self.assertEqual(guess(stem('Dare/Taylor scene.mp4'),[],index)['performers'],[])
        for name in ['daretaylor', 'Dare Taylor', 'dare.taylor', 'DARE_TAYLOR']:
            self.assertEqual(guess(stem(name+'.mp4'),[],index)['performers'],['Dare Taylor'])
        for name in ['taylordare', 'notdaretaylor', 'daretaylors', 'Dare/Taylor']:
            self.assertEqual(guess(stem(name+'.mp4'),[],index)['performers'],[])
        index.append((('taylor','dare'),'Taylor Dare',3))
        self.assertEqual(guess(stem(filename),[],index)['performers'],['Daphne Dare','Taylor Dare'])
    def test_guess_alone_cannot_choose_folder(self):
        pick=load_function('main.py','_pick_queue_group_name_from_row',{'_resolve_library_folder_name':lambda name,kind:name})
        self.assertEqual(pick({'guess_performers':'Daphne Dare','guess_studio':'Studio'}),'')
        self.assertEqual(pick({'performers':'Taylor Dare'}),'Taylor Dare')
