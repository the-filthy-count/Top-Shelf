import unittest
import database as db
from app.core.filename_parser import parse_filename


class FilingTagTitleTests(unittest.TestCase):
    def parse(self, filename):
        return parse_filename(filename, dict(db.DEFAULTS), library_indexes=(
            [('examplestudio', 'Example Studio')],
            [(('jane', 'doe'), 'Jane Doe', 1), (('janie',), 'Jane Doe', 1)]), studio_logos=[])

    def test_stacked_alias_and_studio_tags_keep_identity_not_title(self):
        result = self.parse('[Janie] [Example Studio] Private Lesson.mp4')
        self.assertEqual(result['title'], 'Private Lesson')
        self.assertEqual(result['performer_names'], ['Jane Doe'])
        self.assertEqual(result['site'], 'Example Studio')
        self.assertNotIn('Janie', result['full_query'])
        self.assertNotIn('Example Studio', result['full_query'])

    def test_folder_punctuation_and_case(self):
        result = self.parse('Jane Doe/[JANE_DOE] Private Lesson.mp4')
        self.assertEqual(result['title'], 'Private Lesson')
        self.assertEqual(result['performer_names'], ['Jane Doe'])

    def test_bracketed_title_words_are_not_discarded(self):
        result = self.parse('Private [Jane Doe] Lesson.mp4')
        self.assertIn('Private', result['title'])
        self.assertIn('Lesson', result['title'])
        result = self.parse('[Unrecognised] Private Lesson.mp4')
        self.assertEqual(result['site'], 'Unrecognised')
        self.assertEqual(result['title'], 'Private Lesson')

    def test_tag_only_file_is_not_emptied(self):
        result = self.parse('[Jane Doe].mp4')
        self.assertFalse(result.get('folder_tag', False))

