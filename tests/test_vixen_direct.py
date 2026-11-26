import unittest
from unittest.mock import patch, Mock
from app.integrations import vixen, scene_url

URL = 'https://www.vixen.com/videos/tanned-goddess-has-a-perfect-body'
SLUG = 'tanned-goddess-has-a-perfect-body'

class VixenTests(unittest.TestCase):
    def test_host_validation(self):
        self.assertEqual(vixen.identify(URL + '/?tracking=1'), ('vixen', SLUG))
        for url in ['https://vixen.com.evil.test/videos/a', 'https://evil-vixen.com/videos/a', 'https://www.vixen.com/models/a']:
            self.assertIsNone(vixen.identify(url))

    @patch('app.integrations.scene_url.validate_url', side_effect=lambda u:u)
    @patch('app.integrations.vixen.requests.post')
    def test_direct_import_no_stash_and_correct_scene(self, post, validate):
        post.return_value = Mock(status_code=200, content=b'{}')
        post.return_value.json.return_value = {'data':{'findOneVideo':{
            'title':'Example scene', 'slug': SLUG, 'description':'Plot', 'releaseDate':'2026-11-20T00:00:00Z',
            'models':[{'name':'Performer One'}], 'categories':[{'name':'Tag'}], 'runLength':'00:25:32',
            'images':{'poster':[{'src':'https://cdn.example/small.jpg','width':320}, {'src':'https://cdn.example/large.jpg','width':1920}]}}}}
        result = scene_url.scrape(URL, {'stash_url':'http://must-not-be-used','stash_api_key':'secret'})
        self.assertEqual(result['studio'], 'Vixen')
        self.assertEqual(result['performers'], ['Performer One'])
        self.assertEqual(result['duration'], 1532)
        self.assertEqual(result['date'], '2026-11-20')
        self.assertEqual(result['image'],'https://cdn.example/large.jpg')
        self.assertEqual(post.call_args.args[0], 'https://www.vixen.com/graphql')
        self.assertNotIn('secret', str(post.call_args))
        self.assertFalse(post.call_args.kwargs['allow_redirects'])

    @patch('app.integrations.vixen.requests.post')
    def test_redirect_is_explicit_failure(self, post):
        post.return_value = Mock(status_code=302)
        with self.assertRaisesRegex(ValueError, 'regional landing'):
            vixen.scrape(URL)

    def test_wrong_scene_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'different scene'):
            vixen.normalize({'title':'Wrong', 'slug':'different'}, 'vixen', SLUG)
