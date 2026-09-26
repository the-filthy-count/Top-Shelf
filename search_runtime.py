"""Bounded Prowlarr work with explicit continuation instead of silent truncation."""
from concurrent.futures import ThreadPoolExecutor, wait
import threading
import time
import xml.etree.ElementTree as ET

EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix='prowlarr')
ADMISSION = threading.BoundedSemaphore(4)
DEADLINE_SECONDS = 45.0
MAX_PAGES = 10
MAX_RESULTS = 20000
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class Results(list):
    def __init__(self, values=(), continuation=None, warnings=None):
        super().__init__(values)
        self.continuation = continuation or {}
        self.warnings = warnings or []


def fetch_indexer(base, api_key, indexer, query, requester, parse, clear_cache,
                  deadline=None, offset=0):
    deadline = deadline or time.monotonic() + DEADLINE_SECONDS
    iid, name = indexer['id'], indexer['name']
    results, seen = Results(), set()
    for _ in range(MAX_PAGES):
        response = None
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Search time limit reached')
            response = requester.get(f'{base}/{iid}/api', params={
                't':'search', 'q':query, 'apikey':api_key, 'limit':100, 'offset':offset,
                'cat':'6000,6010,6020,6030,6040,6050,6060,6070,6080,6090'},
                timeout=min(3.0, remaining), stream=True)
            if response.status_code != 200:
                if response.status_code in (400,404,500):
                    clear_cache()
                raise RuntimeError(f'HTTP {response.status_code}')
            if hasattr(response, 'iter_content'):
                body = bytearray()
                for chunk in response.iter_content(65536):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Search time limit reached')
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise ValueError('Indexer response exceeds size limit')
                text = body.decode(response.encoding or 'utf-8', errors='replace')
            else:
                text = response.text  # fixture/legacy response adapters
            page = parse(text, name, indexer.get('protocol','torrent'))
            added = 0
            for item in page:
                key = item.get('guid') or item.get('download_url') or item.get('title')
                if key and key not in seen:
                    seen.add(key)
                    results.append(item)
                    added += 1
            if not page or not added:
                return results
            root = ET.fromstring(text)
            meta = next((el for el in root.iter() if el.tag.split('}')[-1]=='response'),None)
            if meta is None:
                return results
            count = len(root.findall('.//item'))
            next_offset = int(meta.get('offset',offset)) + count
            total = int(meta.get('total',next_offset))
            if next_offset <= offset or next_offset >= total:
                return results
            if total > 10_000_000 or next_offset > 10_000_000:
                raise ValueError('Invalid indexer pagination totals')
            offset = next_offset
        except Exception as exc:
            results.continuation[str(iid)] = offset
            results.warnings.append(f'{name}: {exc}')
            return results
        finally:
            if response is not None and hasattr(response,'close'):
                response.close()
    results.continuation[str(iid)] = offset
    results.warnings.append(f'{name}: more results available')
    return results


def run_indexers(base, key, indexers, query, fetch, deadline=None, continuation=None):
    deadline = deadline or time.monotonic() + DEADLINE_SECONDS
    selected = [(idx, int((continuation or {}).get(str(idx['id']),0))) for idx in indexers
                if continuation is None or str(idx['id']) in continuation]
    result = Results()
    if not ADMISSION.acquire(blocking=False):
        result.continuation = {str(idx['id']):offset for idx,offset in selected}
        result.warnings.append('Search capacity is busy. Continue to retry.')
        return result
    futures = {}
    try:
        for idx, offset in selected:
            if time.monotonic() >= deadline:
                result.continuation[str(idx['id'])] = offset
            else:
                futures[EXECUTOR.submit(fetch,base,key,idx,query,deadline,offset)] = (idx,offset)
        done, pending = wait(futures, timeout=max(0,deadline-time.monotonic()))
        for future in done:
            idx, offset = futures[future]
            try:
                page = future.result()
                if len(result) + len(page) > MAX_RESULTS:
                    result.continuation[str(idx["id"])] = offset
                    result.warnings.append("More results available; continue search.")
                    continue
                result.extend(page)
                result.continuation.update(getattr(page,'continuation',{}))
                result.warnings.extend(getattr(page,'warnings',[]))
            except Exception as exc:
                result.continuation[str(idx['id'])] = offset
                result.warnings.append(f"{idx['name']}: {exc}")
        for future in pending:
            future.cancel()
            idx, offset = futures[future]
            result.continuation[str(idx['id'])] = offset
        if result.continuation and not result.warnings:
            result.warnings.append('Search time limit reached; more results may be available.')
        return result
    finally:
        ADMISSION.release()
