import ast
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock

class CleanupRetry(TestCase):
    def test_only_successfully_imported_history_is_retried(self):
        tree=ast.parse(Path('main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_retry_download_cleanup')
        db=SimpleNamespace(get_settings=lambda:{'download_import_remove_client':'true'},download_cleanup_names=lambda:['Imported'],was_download_import_done=lambda identity:identity=='nzbget-h-4',mark_download_import_done=Mock(),download_cleanup_complete=Mock())
        rows=[{'id':'nzbget-h-1','name':'Imported','status':'SUCCESS/UNPACK'},
              {'id':'nzbget-2','name':'Imported','status':'SUCCESS'},
              {'id':'nzbget-h-3','name':'Imported','status':'FAILURE/UNPACK'},
              {'id':'nzbget-h-4','name':'Old marker','status':'SUCCESS'},
              {'id':'nzbget-h-5','name':'Unimported','status':'SUCCESS'}]
        remove=Mock(return_value={'error':'offline'})
        ns={'db':db,'Path':Path,'_dl_resolve_nzb_settings':lambda s:{'client':'nzbget'},'download_clients_combined_status':lambda _: {'items':rows},'_watch_import_name_matches_client_job':lambda a,b:a==b,'_download_remove_by_id':remove,'emit':lambda _:None,'_downloads_warm':SimpleNamespace(invalidate=Mock())}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'test','exec'),ns)
        ns['_retry_download_cleanup']()
        self.assertEqual([c.args[0] for c in remove.call_args_list],['nzbget-h-1','nzbget-h-4'])
        db.download_cleanup_complete.assert_not_called()
        remove.return_value={'ok':True}
        ns['_retry_download_cleanup']()
        db.download_cleanup_complete.assert_called_once_with('Imported')
        db.get_settings=lambda:{'download_import_remove_client':'false'}
        remove.reset_mock();ns['_retry_download_cleanup']();remove.assert_not_called()

if __name__=='__main__': main()
