"""Rendered typography and themed controls using isolated frontend fixtures."""
import re
import unittest
import mobile
from urllib.parse import urlparse

PAGES = list(mobile.PAGES) + ['/log', '/login', '/performer/2', '/studio/1']


class DesignSystemFlows(mobile.MobileFlows):
    def setUp(self):
        super().setUp()
        self.page.emulate_media(reduced_motion='reduce')

    def route(self, route):
        path = urlparse(route.request.url).path
        if path in ('/log', '/login'):
            return self.asset(route, mobile.ROOT / 'static' / (path[1:] + '.html'))
        return super().route(route)

    def goto(self, path):
        if path in ('/log', '/login'):
            self.page.goto('http://top-shelf.test' + path)
        else:
            super().goto(path)

    def test_four_text_sizes_across_pages(self):
        for width in (1440, 390):
            self.page.set_viewport_size({'width': width, 'height': 900})
            for path in PAGES:
                with self.subTest(width=width, page=path):
                    self.goto(path)
                    self.page.evaluate('document.fonts.ready')
                    unexpected = self.page.evaluate("""() => {
                        const out = [], walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                        while (walker.nextNode()) {
                            const node = walker.currentNode, el = node.parentElement;
                            if (!node.textContent.trim() || !el.getClientRects().length ||
                                el.closest('script,style,svg,i,.sky-counter,.dl-tile-pct-num,.dl-tile-pct-sym')) continue;
                            const style = getComputedStyle(el);
                            if (style.visibility === 'hidden') continue;
                            if (![10,12,14,16,20].includes(parseFloat(style.fontSize)))
                                out.push({text: node.textContent.trim().slice(0,40), size: style.fontSize, cls: el.className});
                        }
                        return out;
                    }""")
                    self.assertEqual(unexpected, [])

    def assert_controls_themed(self):
        failures = self.page.evaluate("""() => {
            const failures = []; let controls = 0;
            for (const el of document.querySelectorAll('button,a.btn-primary,a.btn-secondary,a.btn-icon')) {
                if (!el.getClientRects().length) continue;
                const s = getComputedStyle(el);
                if (s.getPropertyValue('--ui-control').trim() !== '1') continue; // content tiles/switches
                controls++;
                const probe = document.createElement('div');
                probe.style.background = s.getPropertyValue('--button-surface').trim() || s.getPropertyValue('--control-surface');
                probe.style.color = s.getPropertyValue('--button-ink').trim() || s.getPropertyValue('--text');
                document.body.append(probe);
                const expected = getComputedStyle(probe);
                if (s.backgroundColor !== expected.backgroundColor || s.color !== expected.color || s.borderRadius !== '8px')
                    failures.push({id:el.id, cls:el.className, bg:s.backgroundColor, expected:expected.backgroundColor,
                                   color:s.color, expectedColor:expected.color, radius:s.borderRadius});
                probe.remove();
            }
            if (!controls) failures.push({error: "No themed controls found"});
            return failures;
        }""")
        self.assertEqual(failures, [])

    def test_controls_across_pages_and_themes(self):
        for path in PAGES:
            self.goto(path)
            for theme in ('dark', 'light', 'glass'):
                with self.subTest(page=path, theme=theme):
                    self.page.evaluate('(theme) => document.documentElement.dataset.theme = theme', theme)
                    self.page.wait_for_timeout(150)  # allow color transitions to finish
                    self.assert_controls_themed()

    def test_dialog_states_in_every_theme(self):
        source = (mobile.ROOT / 'static/theme-init.js').read_text()
        themes = re.findall(r"'([^']+)'", source.split('var KNOWN_THEMES = [')[1].split('];')[0])
        self.goto('/settings')
        # Production dialog, without accepting its destructive action.
        self.page.evaluate("() => { window.tsConfirm('Delete the example?', {title:'Confirm delete', destructive:true}); }")
        button = self.page.locator('.ts-confirm-ok')
        button.wait_for(state='visible')
        for theme in themes:
            with self.subTest(theme=theme):
                self.page.evaluate('(theme) => document.documentElement.dataset.theme = theme', theme)
                self.page.wait_for_timeout(150)
                self.assert_controls_themed()
                self.assertEqual(button.evaluate("e => getComputedStyle(e).color"),
                                 button.evaluate("e => {const p=document.createElement('div');p.style.color=getComputedStyle(e).getPropertyValue('--red');document.body.append(p);const c=getComputedStyle(p).color;p.remove();return c;}"))
        button.focus()
        self.assertEqual(button.evaluate("e => getComputedStyle(e).outlineStyle"), 'solid')
        button.evaluate('(e) => e.disabled = true')
        self.assertEqual(button.evaluate("e => getComputedStyle(e).cursor"), 'not-allowed')
        self.page.locator('.ts-confirm-cancel').click()


if __name__ == '__main__':
    unittest.main(defaultTest=[
        'DesignSystemFlows.test_four_text_sizes_across_pages',
        'DesignSystemFlows.test_controls_across_pages_and_themes',
        'DesignSystemFlows.test_dialog_states_in_every_theme',
    ], verbosity=2)
