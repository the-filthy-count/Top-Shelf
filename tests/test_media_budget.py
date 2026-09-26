import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from media_budget import media_slot, snapshot, LIMIT

class MediaBudgetTests(unittest.TestCase):
    def test_shared_limit_and_failure_release(self):
        release = threading.Event()
        both_started = threading.Event()
        guard = threading.Lock()
        started = 0
        def work(name):
            nonlocal started
            with media_slot(name):
                with guard:
                    started += 1
                    if started == LIMIT:
                        both_started.set()
                if not release.wait(3):
                    raise RuntimeError('Test barrier timeout')
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(work, name) for name in ('Hash test', 'Filmstrip test')]
            try:
                self.assertTrue(both_started.wait(2))
                status = snapshot()
                self.assertEqual(sum(j['active'] for j in status['jobs']), LIMIT)
                self.assertTrue(all(j['elapsed_seconds'] >= 0 for j in status['jobs']))
            finally:
                release.set()
            for future in futures:
                future.result()
        with self.assertRaises(ValueError):
            with media_slot('Failure test'):
                raise ValueError('expected')
        with media_slot('After failure'):
            pass
        jobs = {j['name']: j for j in snapshot()['jobs']}
        self.assertEqual(jobs['Failure test']['errors'], 1)
        self.assertEqual(jobs['After failure']['completed'], 1)
        self.assertEqual(sum(j['active'] for j in jobs.values()), 0)

if __name__ == '__main__':
    unittest.main()
