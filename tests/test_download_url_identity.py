import unittest
import database as db


class DownloadUrlIdentityTests(unittest.TestCase):
    def setUp(self):
        self.index = [(('liz','jordan'),'Liz Jordan',1), (('t',),'Liz Jordan',1),
                      (('filester',),'Other Performer',2)]

    def guess(self, title):
        return db._library_guess_from_release_name(title, [], self.index)['performers']

    def test_share_urls_do_not_invent_names(self):
        for title in ['https://filester.gg/d/XCLK2T2', 'https://filester.gg/d/xCLK2T2?name=Liz+Jordan',
                      'https://liz-jordan.example/d/abc123', 'https://filester.gg/d/T']:
            self.assertEqual(self.guess(title), [], title)

    def test_alphanumeric_codes_do_not_supply_partial_aliases(self):
        for title in ['XCLK2T2.mp4','xCLK2T2.mp4','T123.mp4']:
            self.assertEqual(self.guess(title), [], title)

    def test_meaningful_early_names_still_match(self):
        for title in ['LizJordan scene.mp4','Liz.Jordan.Scene.mp4','Liz Jordan XCLK2T2.mp4',
                      'https://example.com/videos/liz-jordan-scene',
                      'https://example.com/media/Liz%20Jordan.mp4?token=XCLK2T2']:
            self.assertEqual(self.guess(title), ['Liz Jordan'], title)
        self.assertEqual(self.guess('T - Scene.mp4'), ['Liz Jordan'])
