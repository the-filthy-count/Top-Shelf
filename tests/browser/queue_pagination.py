"""Unmatched pagination counts rendered grid rows rather than tiles."""
import math
import unittest
from urllib.parse import urlparse
from playwright.sync_api import expect
import mobile


class QueuePagination(mobile.MobileFlows):
    def setUp(self):
        self.queue_files = [{'filename': f'Example {i:03d}.mp4', 'size': 1000000,
                             'prev_status': 'unmatched', 'scope': 'scene'} for i in range(151)]
        super().setUp()
        self.page.set_viewport_size({'width': 1440, 'height': 900})

    def route(self, route):
        if urlparse(route.request.url).path == '/api/queue':
            return route.fulfill(json={'files': self.queue_files})
        return super().route(route)

    def columns(self):
        return self.page.locator('#queueList').evaluate('e => getComputedStyle(e).gridTemplateColumns.split(" ").length')

    def test_rows_navigation_resize_and_filter(self):
        self.goto('/unmatched')
        cards = self.page.locator('#queueList > .queue-item')
        capacity = self.columns() * 5
        expect(cards).to_have_count(capacity)
        expect(self.page.locator('#queuePagerStatus')).to_contain_text(f'Rows 1–5 of {math.ceil(151 / self.columns())}')
        self.page.locator('#queuePagerNext').click()
        expect(self.page.locator('#queuePagerStatus')).to_contain_text('page 2/')
        anchor = cards.first.get_attribute('data-filename')
        self.page.set_viewport_size({'width': 390, 'height': 844})
        expect(cards).to_have_count(5)
        self.assertIn(anchor, cards.evaluate_all('els => els.map(e => e.dataset.filename)'))
        self.page.locator('#queuePagerLast').click()
        expect(cards).to_have_count(1)
        expect(self.page.locator('#queuePagerNext')).to_be_disabled()
        self.page.locator('#queuePagerSize').select_option('3')
        expect(cards).to_have_count(3)
        expect(self.page.locator('#queuePagerFirst')).to_be_disabled()
        self.page.locator('#queueFilter').fill('Example 00')
        expect(cards).to_have_count(3)
        expect(self.page.locator('#queuePagerStatus')).to_contain_text('Rows 1–3 of 10 · page 1/4')
        self.page.locator('#queueFilter').fill('no such file')
        expect(self.page.locator('#queuePager')).not_to_be_visible()
        self.assertEqual(self.errors, [])

    def test_folder_tiles_count_as_one_slot(self):
        self.queue_files += [dict(self.queue_files[0], filename=f'Grouped/Clip {i:03d}.mp4') for i in range(70)]
        self.goto('/unmatched')
        cards = self.page.locator('#queueList > .queue-item')
        expect(cards).to_have_count(self.columns() * 5)
        expect(self.page.locator('#queuePagerStatus')).to_contain_text(f'of {math.ceil(152 / self.columns())} ·')
        self.page.locator('.queue-item-folder').click()
        expect(cards).to_have_count(min(70, self.columns() * 5))
        expect(self.page.locator('#queuePagerStatus')).to_contain_text(f'of {math.ceil(70 / self.columns())} ·')
        self.assertEqual(self.errors, [])


if __name__ == '__main__':
    unittest.main(defaultTest=['QueuePagination.test_rows_navigation_resize_and_filter',
                               'QueuePagination.test_folder_tiles_count_as_one_slot'], verbosity=2)
