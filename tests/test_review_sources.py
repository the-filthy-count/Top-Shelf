import unittest
from unittest.mock import patch
from lxml import html
from app.integrations import review_sources as review


class ReviewSourcesTests(unittest.TestCase):
    def test_data18_profile_credits_stay_with_their_scene(self):
        tree = html.fromstring('''<h1><a href="/name/example">Example</a></h1><section>
          <div>Nov 04, 2026 <p><a href="/scenes/11">First</a></p>
          <a href="/name/costar">Costar</a><a href="/studios/one">One</a></div>
          <div><p><a href="/scenes/12">Second</a></p></div>
          <div><p><a href="https://other.example/scenes/13">Offsite</a></p></div>
          </section>''')
        name, rows = review.data18_profile(tree)
        self.assertEqual(name, 'Example')
        self.assertEqual([r['id'] for r in rows], ['11','12'])
        self.assertEqual(rows[0]['release_date'], '2026-11-04')
        self.assertEqual(rows[0]['studio']['name'], 'One')
        self.assertEqual(len(rows[0]['performers']), 2)
        self.assertEqual(len(rows[1]['performers']), 1)
        self.assertEqual(rows[1]['release_date'], '')

    def test_imported_credits_work_without_live_access(self):
        row = review._row('Data18','11','First scene','/scenes/11',['Example'])
        with patch.object(review.db,'app_json_cache_get',side_effect=[[row],None]), patch.object(review,'_fetch') as fetch:
            self.assertEqual(review.search('Data18','Example','First'), [(row,'Data18')])
            fetch.assert_not_called()

    def test_euro_reference_requires_matching_profile(self):
        tree = html.fromstring('<title>Other Person Porn Galleries at Europornstar.com</title><a class="model" href="/any">Sample</a>')
        with patch.object(review,'_fetch',return_value=tree):
            self.assertEqual(review._filmography('EuroPornstar','Example'), [])

    def test_afdb_ambiguous_profile_is_skipped(self):
        tree = html.fromstring('<p><a href="/actor/example-1/">Example</a><a href="/actor/example-2/">Example</a></p>')
        with patch.object(review,'_fetch',return_value=tree) as fetch:
            self.assertEqual(review._filmography('AFDB','Example'), [])
            self.assertEqual(fetch.call_count,1)
