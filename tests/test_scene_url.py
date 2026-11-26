import unittest
from unittest.mock import patch, Mock
from app.integrations.scene_url import parse_page, scrape, validate_url, fetch_page


class SceneUrlTests(unittest.TestCase):
    def test_jsonld_graph(self):
        data = parse_page('''<script type="application/ld+json">{"@graph":[{"@type":"Organization","name":"Wrong"},{"@type":"VideoObject","name":"Scene","description":"Plot","actor":[{"name":"Liz Jordan"}],"productionCompany":{"name":"Studio"},"uploadDate":"2026-11-26T00:00:00Z","thumbnailUrl":["/poster.jpg"],"keywords":"one,two"}]}</script>''', 'https://example.com/scene')
        self.assertEqual(data['title'], 'Scene')
        self.assertEqual(data['performers'], ['Liz Jordan'])
        self.assertEqual(data['studio'], 'Studio')
        self.assertEqual(data['date'], '2026-11-26')
        self.assertEqual(data['image'], 'https://example.com/poster.jpg')
        self.assertEqual(data['tags'], ['one', 'two'])

    def test_metadata_fallback_and_invalid_json(self):
        data = parse_page('<script type="application/ld+json">bad</script><title>Fallback</title><meta property="og:title" content="Scene &amp; title"><meta name="description" content="Description">', 'https://example.com/')
        self.assertEqual(data['title'], 'Scene & title')
        self.assertEqual(data['description'], 'Description')
        self.assertEqual(data['performers'], [])

    @patch('app.integrations.scene_url.socket.getaddrinfo', return_value=[(2, 1, 6, '', ('127.0.0.1', 80))])
    def test_reject_private_and_credentials(self, lookup):
        for url in ['http://localhost/test', 'file:///tmp/private', 'https://user:secret@example.com']:
            with self.assertRaises(ValueError): validate_url(url)

    @patch('app.integrations.scene_url.validate_url', side_effect=lambda url: url)
    @patch('app.integrations.scene_url.requests.post')
    def test_stash_scene_names_no_foreign_ids(self, post, validate):
        post.return_value.json.return_value = {'data': {'scrapeSceneURL': {'title': 'Scene', 'performers': [{'name': 'Liz Jordan', 'stored_id': '123'}]}}}
        data = scrape('https://example.com/scene', {'stash_url':'http://stash:9999','stash_api_key':'key'})
        self.assertEqual(data['performers'], ['Liz Jordan'])
        self.assertEqual(data['provider'], 'Stash scraper')
        self.assertEqual(post.call_args.kwargs['headers'], {'ApiKey':'key'})

    @patch('app.integrations.scene_url.validate_url', side_effect=lambda url: url)
    @patch('app.integrations.scene_url.fetch_page', return_value={'title': 'Page'})
    @patch('app.integrations.scene_url.requests.post')
    def test_stash_error_falls_back(self, post, fetch, validate):
        post.return_value.json.return_value = {'errors':[{'message':'No scraper'}]}
        self.assertEqual(scrape('https://example.com', {'stash_url':'http://stash'})['provider'], 'Webpage metadata')
        fetch.assert_called_once()

    @patch('app.integrations.scene_url.validate_url')
    @patch('app.integrations.scene_url.requests.Session')
    def test_redirect_destination_validated(self, session, validate):
        response = Mock(is_redirect=True, headers={'Location':'http://127.0.0.1/private'})
        session.return_value.__enter__.return_value.get.return_value.__enter__.return_value = response
        validate.side_effect = ['https://example.com', ValueError('private')]
        with self.assertRaises(ValueError): fetch_page('https://example.com')
        self.assertEqual(validate.call_args.args, ('http://127.0.0.1/private',))

    @patch('app.integrations.scene_url.validate_url')
    @patch('app.integrations.scene_url.requests.Session')
    def test_homepage_redirect_is_not_a_scene(self, session, validate):
        response = Mock(is_redirect=True, headers={'Location':'https://example.com/sfw'})
        session.return_value.__enter__.return_value.get.return_value.__enter__.return_value = response
        with self.assertRaisesRegex(ValueError, 'homepage'):
            fetch_page('https://example.com/videos/a-scene')
