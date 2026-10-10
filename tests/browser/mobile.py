"""Phone/tablet workflows against real frontend assets and isolated API fixtures.

python tests/browser/mobile.py
TS_BROWSER=webkit python tests/browser/mobile.py
No application startup, live media access, or outgoing network requests.
"""
import json
import mimetypes
import os
from pathlib import Path
import unittest
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[2]
PAGES = {'/': 'home.html', '/studios': 'studios.html', '/stars': 'stars.html',
         '/movies': 'movies.html', '/jav': 'jav.html', '/vices': 'vices.html',
         '/scenes': 'scenes.html', '/unmatched': 'queue.html', '/downloads': 'downloads.html',
         '/releases': 'index.html', '/health': 'library.html', '/settings': 'settings.html', '/news': 'news.html'}
SVG = '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><rect width="100" height="100" fill="#987aca"/></svg>'
ASSETS = {}


class MobileFlows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pw = sync_playwright().start()
        cls.browser = getattr(cls.pw, os.environ.get('TS_BROWSER', 'chromium')).launch()

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.pw.stop()

    def setUp(self):
        self.context = self.browser.new_context(viewport={'width': 390, 'height': 844}, is_mobile=True, has_touch=True)
        self.context.route('**/*', self.route)
        self.page = self.context.new_page()
        self.page.set_default_timeout(8000)
        self.errors = []
        self.page.on('pageerror', lambda error: self.errors.append(str(error)))
        self.calls = []
        self.apply_status = 200
        self.rows = [{'id': i, 'kind': kind, 'folder_name': 'Example ' + kind,
                      'image_url': '/fixture.svg', 'path': '/library/Example', 'file_count': 12}
                     for i, kind in enumerate(['studio', 'performer', 'movie', 'jav'], 1)]

    def tearDown(self):
        self.context.close()

    def asset(self, route, path):
        if path not in ASSETS:
            ASSETS[path] = path.read_bytes() if path.is_file() else None
        data = ASSETS[path]
        if data is not None:
            route.fulfill(body=data, content_type=mimetypes.guess_type(path)[0] or 'application/octet-stream')
        else:
            route.fulfill(status=404, body='')

    def route(self, route):
        request = route.request
        path = urlparse(request.url).path
        if request.resource_type == 'eventsource':
            return route.fulfill(status=204, content_type='text/event-stream', body='')
        self.calls.append((request.method, path, request.post_data))
        if path in ('/fixture.svg', '/studio.svg') or path.endswith(('studio-thumb', 'performer-thumb')):
            return route.fulfill(body=SVG, content_type='image/svg+xml')
        if path in PAGES:
            return self.asset(route, ROOT / 'static' / PAGES[path])
        if path.startswith(('/studio/', '/performer/')):
            return self.asset(route, ROOT / 'static/entity.html')
        if path.startswith('/static/'):
            return self.asset(route, ROOT / path.lstrip('/'))
        if path.startswith('/api/'):
            data = {}
            if path == '/api/health': data = {'ok': True}
            elif path == '/api/status': data = {'version': 'test', 'running': False, 'stats': {}}
            elif path == '/api/home/data': data = {'performers': [dict(self.rows[1], id=i+20, folder_name='Example Star '+str(i), name='Example Star '+str(i)) for i in range(18)], 'discovered': [{'name': 'Discovered Star '+str(i), 'image_url': '/fixture.svg', 'sources': {'stashdb': 3}} for i in range(18)]}
            elif path == '/api/settings': data = {'settings': {}, 'directories': []}
            elif path == '/api/favourites': data = {'performers': self.rows, 'studios': [self.rows[0]], 'movies': [self.rows[2]], 'jav': [self.rows[3]]}
            elif path in ('/api/favourites/row', '/api/favourites/entity-panel'): data = {'row': self.rows[0]}
            elif path == '/api/favourites/image-search': data = {'items': [{'name': 'Example SVG logo', 'image': 'https://images.test/studio.svg', 'source': 'StashDB'}]}
            elif path == '/api/favourites/apply-image':
                return route.fulfill(status=self.apply_status, json={'ok': True} if self.apply_status == 200 else {'error': 'Could not convert this image'})
            elif path == '/api/performer/popup': data = {'identity': {'canonical_name': 'Example Performer'}, 'bio': {}, 'library_status': {'in_library': True, 'row_id': 2}}
            elif path == '/api/downloads': data = {'items': [{'id': 'qbit-example', 'name': 'Example release with a long filename', 'client': 'qbittorrent', 'source': 'torrent', 'status': 'downloading', 'progress_pct': 42, 'queue': 'active'}]}
            elif path == '/api/queue': data = {'files': [{'filename': 'Example scene with a long filename.mp4', 'size': 1000000, 'prev_status': 'unmatched', 'scope': 'scene'}]}
            elif path == '/api/queue/thumbs': data = {'ready': True, 'thumbs': [{'i': i, 'url': '/fixture.svg', 'percent': 10 + i * 20} for i in range(5)]}
            return route.fulfill(json=data)
        route.fulfill(status=404, body='')

    def goto(self, path):
        self.page.goto('http://top-shelf.test' + path)
        self.page.locator('#tsSidebarToggle').wait_for(state='attached')
        self.page.wait_for_function("!document.querySelector('#bootSplash') || document.querySelector('#bootSplash').classList.contains('boot-splash--hide')")

    def assert_fits(self, locator):
        self.page.wait_for_function("Math.abs(parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--ts-mobile-height')) - visualViewport.height) < 2")
        box = locator.bounding_box()
        size = self.page.viewport_size
        self.assertIsNotNone(box)
        self.assertGreaterEqual(box['x'], -1)
        self.assertGreaterEqual(box['y'], -1)
        self.assertLessEqual(box['x'] + box['width'], size['width'] + 1)
        self.assertLessEqual(box['y'] + box['height'], size['height'] + 1)

    def test_page_layouts_at_phone_landscape_and_desktop_sizes(self):
        for width, height in [(360, 800), (390, 844), (430, 932), (844, 390), (1440, 900)]:
            self.page.set_viewport_size({'width': width, 'height': height})
            for path in PAGES:
                with self.subTest(size=(width, height), page=path):
                    self.goto(path)
                    self.page.wait_for_timeout(60)
                    self.assertLessEqual(self.page.evaluate('document.documentElement.scrollWidth'), width + 1)
                    toggle = self.page.locator('#tsSidebarToggle')
                    if width <= 900:
                        self.assert_fits(toggle)
                        title = self.page.locator('.ts-page-title:visible').first
                        if title.count():
                            a, b = toggle.bounding_box(), title.bounding_box()
                            self.assertFalse(a['x'] < b['x'] + b['width'] and a['x'] + a['width'] > b['x'] and a['y'] < b['y'] + b['height'] and a['y'] + a['height'] > b['y'], 'Menu overlaps the title')
                    else:
                        expect(toggle).not_to_be_visible()
                        expect(self.page.locator('#tsSidebar')).to_be_visible()
            self.assertEqual(self.errors, [])

    def test_navigation_traps_focus_restores_it_and_unlocks_on_resize(self):
        self.goto('/studios')
        toggle = self.page.locator('#tsSidebarToggle')
        toggle.tap()
        expect(toggle).to_have_attribute('aria-expanded', 'true')
        self.page.wait_for_function('document.body.classList.contains("ts-mobile-layer-open")')
        self.assertTrue(self.page.locator('.app').evaluate('(el) => el.inert'))
        self.page.locator('#tsSidebar a').first.focus()
        self.page.keyboard.press('Shift+Tab')
        self.assertTrue(self.page.evaluate('document.activeElement.closest("#tsSidebar") !== null'))
        self.page.keyboard.press('Escape')
        expect(toggle).to_be_focused()
        self.assertFalse(self.page.locator('.app').evaluate('(el) => el.inert'))
        toggle.tap()
        self.page.locator('#tsSidebarClose').tap()
        expect(toggle).to_have_attribute('aria-expanded', 'false')
        toggle.tap()
        self.page.set_viewport_size({'width': 1440, 'height': 900})
        self.page.wait_for_function('!document.body.classList.contains("ts-mobile-layer-open")')
        expect(self.page.locator('#tsSidebar')).not_to_have_attribute('inert', '')
        self.assertFalse(self.page.locator('.app').evaluate('(el) => el.inert'))

    def test_populated_home_density_and_fullscreen_navigation(self):
        self.goto('/')
        for width in (360, 390, 430):
            self.page.set_viewport_size({'width': width, 'height': 800})
            grid = self.page.locator('#homeDiscoveredGrid')
            expect(grid.locator('.fav-cell')).to_have_count(3)
            self.assertEqual(grid.evaluate('e=>getComputedStyle(e).gridTemplateColumns.split(" ").length'), 3)
            search = self.page.locator('.home-search').bounding_box()
            chips = self.page.locator('#homeDiscSrcChips').bounding_box()
            self.assertLessEqual(search['y'] + search['height'], chips['y'])
            self.assert_fits(self.page.locator('#homeDiscSrcChips'))
            self.page.locator('#tsSidebarToggle').tap()
            menu = self.page.locator('#tsSidebar')
            box = menu.bounding_box()
            self.assertAlmostEqual(box['width'], width, delta=1)
            self.assertAlmostEqual(box['height'], 800, delta=1)
            expect(menu.locator('a[href="/health"]')).not_to_be_visible()
            self.assert_fits(self.page.locator('#tsSidebarClose'))
            self.page.locator('#tsSidebarClose').tap()
            expect(menu).not_to_be_visible()
        self.goto('/health')
        expect(self.page.locator('.ts-mobile-desktop-notice')).to_be_visible()
        expect(self.page.locator('.app')).not_to_be_visible()

    def test_filters_with_reduced_keyboard_height(self):
        self.goto('/studios')
        self.page.locator('#favMobileFilterOpen').tap()
        sheet = self.page.locator('#favFilterSheet')
        expect(sheet).to_be_visible()
        field = sheet.locator('#favFilter')
        field.fill('Example')
        self.page.set_viewport_size({'width': 390, 'height': 360})
        self.assert_fits(sheet.locator('.fav-filter-sheet-panel'))
        self.assertGreaterEqual(float(field.evaluate('(el) => parseFloat(getComputedStyle(el).fontSize)')), 16)
        self.page.keyboard.press('Escape')
        expect(sheet).not_to_be_visible()
        expect(self.page.locator('#favMobileFilterOpen')).to_be_focused()
        self.page.locator('#favMobileFilterOpen').tap()
        self.page.set_viewport_size({'width': 844, 'height': 390})
        expect(self.page.locator('#favToolbarHost #favToolbar')).to_be_visible()

    def test_studio_svg_picker_save_and_failure_on_phone(self):
        self.goto('/studios')
        self.page.locator('#favGrid .fav-cell').first.tap()
        expect(self.page.locator('#studioPopupModal')).to_be_visible()
        edit = self.page.locator('.studio-popup-logo-edit')
        edit.tap()
        modal = self.page.locator('#studioLogoPickModal')
        expect(modal).to_be_visible()
        self.assert_fits(modal.locator('.modal-box'))
        self.page.locator('.studio-logo-pick-tile').first.tap()
        self.page.set_viewport_size({'width': 390, 'height': 360})
        apply = self.page.locator('#studioLogoPickApplyBtn')
        self.assert_fits(apply)
        self.apply_status = 500
        apply.tap()
        expect(self.page.locator('#studioLogoPickError')).to_contain_text('Could not convert')
        expect(apply).to_be_enabled()
        self.apply_status = 200
        apply.tap()
        expect(modal).not_to_be_visible()
        saves = [json.loads(body) for method, path, body in self.calls if path == '/api/favourites/apply-image']
        self.assertEqual(saves[-1], {'row_id': 1, 'save_local': True, 'image_url': 'https://images.test/studio.svg'})
        expect(self.page.locator('#studioPopupModal')).to_be_visible()
        self.page.set_viewport_size({'width': 390, 'height': 844})
        for _ in range(2):
            self.page.locator('.studio-popup-logo-edit').tap()
            expect(modal).to_be_visible()
            self.page.keyboard.press('Escape')
            expect(modal).not_to_be_visible()
            expect(self.page.locator('#studioPopupModal')).to_be_visible()
        self.assertEqual(self.errors, [])

    def test_settings_categories_and_save_stay_reachable(self):
        self.goto('/settings')
        for name in ['directories', 'downloads', 'appearance', 'security']:
            button = self.page.locator(f'button[data-settings-category="{name}"]')
            button.tap()
            expect(button.locator('.settings-nav-label')).to_be_visible()
        self.page.set_viewport_size({'width': 844, 'height': 390})
        save = self.page.locator('.settings-shell-header-actions .btn-primary')
        self.assert_fits(save)
        field = self.page.locator('#cfgSessionTimeout')
        if field.count():
            field.fill('120')
        self.assertEqual(self.errors, [])

    def test_performer_image_manager_nested_close(self):
        self.goto('/performer/2')
        expect(self.page.locator('#performerPopupModal')).to_be_visible()
        self.assert_fits(self.page.locator('.performer-popup-header'))
        self.page.locator('.pp-headshot-wrap').tap()
        manager = self.page.locator('#posterRoleModal')
        expect(manager).to_be_visible()
        self.page.set_viewport_size({'width': 390, 'height': 360})
        self.assert_fits(manager.locator('.fav-roles-modal-box'))
        self.page.keyboard.press('Escape')
        expect(manager).not_to_be_visible()
        expect(self.page.locator('#performerPopupModal')).to_be_visible()
        self.page.locator('#tsSidebarToggle').tap()
        expect(self.page.locator('#tsSidebarClose')).to_be_visible()
        self.page.locator('#tsSidebarClose').tap()
        expect(self.page.locator('#performerPopupModal')).to_be_visible()
        self.assertEqual(self.errors, [])

    def test_all_counter_tiles(self):
        seen = set()
        geometry = """() => [...document.querySelectorAll('.sky-counter')]
            .filter(el => el.getClientRects().length).map(el => {
                const range = document.createRange(); range.selectNodeContents(el);
                const text = range.getBoundingClientRect();
                const card = el.closest('.stat-card,.emb-summary-card');
                const box = card.getBoundingClientRect();
                const label = card.querySelector('.stat-label,.emb-summary-label').getBoundingClientRect();
                const overlaps = Math.min(text.bottom, label.bottom) - Math.max(text.top, label.top) > 1;
                return {id: el.id, fits: Math.abs((text.left + text.right - box.left - box.right) / 2) < 2
                    && text.left >= box.left && text.right <= box.right
                    && text.top >= box.top && text.bottom <= box.bottom
                    && label.left >= box.left && label.right <= box.right && !overlaps};
            })"""
        for path in ('/unmatched', '/downloads', '/health'):
            self.goto(path)
            self.page.evaluate('document.fonts.ready')
            for metric in ((None, 'performers', 'studios', 'vices') if path == '/health' else (None,)):
                if metric:
                    self.page.set_viewport_size({'width': 1440, 'height': 900})
                    self.page.locator('[data-metric="' + metric + '"]').click()
                for width in ((1800, 1440, 1024) if path == '/health' else (1800, 1440, 1024, 844, 640, 430, 390, 360)):
                    self.page.set_viewport_size({'width': width, 'height': 900})
                    for value in ('0', '123456', '1234567', '12.34 TB', 'v1.2.3'):
                        with self.subTest(page=path, panel=metric, width=width, value=value):
                            self.page.locator('.sky-counter:visible').evaluate_all(
                                '(els, value) => els.forEach(el => el.textContent = value)', value)
                            self.page.wait_for_function('() => (' + geometry + ')().every(row => row.fits)')
                            seen.update(path + ':' + row['id'] for row in self.page.evaluate(geometry))
        self.assertEqual(len(seen), 47, 'Every counter tile, including hidden Health panels, must be checked')

    def test_queue_counter_text_stays_inside_cards(self):
        self.goto('/unmatched')
        self.page.evaluate('document.fonts.ready')
        for width in (1800, 1440, 844, 390, 360):
            self.page.set_viewport_size({'width': width, 'height': 900})
            for value in ('0', '123456', '1234567'):
                self.page.locator('.queue-stats-bar .stat-value').evaluate_all(
                    '(els, value) => els.forEach(el => el.textContent = value)', value)
                self.page.wait_for_function("""() => [...document.querySelectorAll('.queue-stats-bar .stat-value')].every(el => {
                    const range = document.createRange(); range.selectNodeContents(el);
                    const text = range.getBoundingClientRect(), box = el.getBoundingClientRect();
                    return text.left >= box.left - 1 && text.right <= box.right + 1;
                })""")
                bounds = self.page.locator('.queue-stats-bar .stat-value').evaluate_all("""els => els.map(el => {
                    const range = document.createRange(); range.selectNodeContents(el);
                    const text = range.getBoundingClientRect(), box = el.getBoundingClientRect();
                    const label = el.nextElementSibling.getBoundingClientRect();
                    return {left: text.left - box.left, right: text.right - box.right,
                            overlap: text.bottom - label.top};
                })""")
                for box in bounds:
                    with self.subTest(width=width, value=value):
                        self.assertGreaterEqual(box['left'], -1)
                        self.assertLessEqual(box['right'], 1)
                        self.assertLessEqual(box['overlap'], 0)

    def test_queue_counters_and_filmstrip_on_phone(self):
        self.goto('/unmatched')
        columns = self.page.locator('.queue-stats-bar').evaluate('(el) => getComputedStyle(el).gridTemplateColumns.split(" ").length')
        self.assertEqual(columns, 2)
        pager = self.page.locator('#queuePager')
        pager.scroll_into_view_if_needed()
        self.assertLessEqual(pager.evaluate('(el) => el.scrollWidth - el.clientWidth'), 1)
        self.page.evaluate("openQueueFilmstrip('Example scene with a long filename.mp4')")
        expect(self.page.locator('#qFilmstripStrip img')).to_have_count(5)
        self.page.wait_for_function('document.body.classList.contains("ts-mobile-layer-open")')
        self.assert_fits(self.page.locator('.qfs-close'))
        self.page.keyboard.press('Escape')
        self.assertEqual(self.errors, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
