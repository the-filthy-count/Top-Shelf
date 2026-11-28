import ast
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock

class PosterLoadingTests(TestCase):
    def test_saved_images_do_not_wait_for_remote_candidates(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if getattr(n, 'name', '') == 'api_performers_poster_roles')
        fn.decorator_list = []
        remote = Mock(side_effect=RuntimeError('remote unavailable'))
        ns = dict(db=SimpleNamespace(favourite_get=lambda _: {'kind':'performer','folder_name':'Example'}),
                  _metadata_dir_for=lambda *a: None, _perf_poster_resolve=lambda *a: None,
                  _favourites_matched_link_image_items=remote, _favourite_find_local_poster_file=lambda *a:None,
                  _performers_iafd_image_api_url=lambda *a:'/saved-headshot', _performers_iafd_read_meta=lambda *a:{})
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),ns)
        result = ns['api_performers_poster_roles'](1, part='saved')
        self.assertEqual(result['headshot_url'], '/saved-headshot')
        remote.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, 'remote unavailable'):
            ns['api_performers_poster_roles'](1, part='candidates')

if __name__ == '__main__': main()
