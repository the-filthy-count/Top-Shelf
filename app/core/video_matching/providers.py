"""Independent provider attempts with durable success/miss observations."""
import asyncio
from dataclasses import dataclass
from .decisions import fingerprint
from .journal import digest


@dataclass
class Lookup:
    source: str
    rows: list
    error: str = ''


async def fingerprints(phash, keys, emit=None, journal=None):
    from app.integrations import stashbox as transport
    specs = (
        ('stashdb','StashDB',transport.STASHDB_ENDPOINT,transport.STASHDB_QUERY),
        ('tpdb','TPDB',transport.TPDB_ENDPOINT,transport.TPDB_QUERY),
        ('fansdb','FansDB',transport.FANSDB_ENDPOINT,transport.STASHDB_QUERY),
        ('javstash','JAVStash',transport.JAVSTASH_ENDPOINT,transport.JAVSTASH_QUERY),
    )
    configured = [spec for spec in specs if str(keys.get(spec[0]) or '').strip()]
    if not configured:
        raise RuntimeError('No fingerprint search databases configured')
    async with await transport.get_async_client() as client:
        async def lookup(spec):
            key,label,endpoint,query = spec
            inputs = {'phash':phash,'endpoint':endpoint,'credential':digest(keys[key]),'query':query}
            stage = 'fingerprint:'+key
            cached = journal.cached(stage,inputs) if journal else None
            if cached is not None:
                return Lookup(label,cached)
            try:
                rows = await transport.query_stashbox(client,phash,endpoint,keys[key],query,'scene')
                if journal:
                    journal.write(stage,inputs,result=rows)
                return Lookup(label,rows)
            except Exception as exc:
                from app.core.queue_changes import SupersededWork
                if isinstance(exc,SupersededWork):
                    raise
                if journal:
                    journal.write(stage,inputs,error=exc)
                if emit:
                    emit(f'  {label}: lookup unavailable; retry required')
                return Lookup(label,[],str(exc))
        # A rate-limited provider cannot prevent the other providers' responses
        # from being obtained and saved. Failed providers alone are repeated.
        tasks = [asyncio.create_task(lookup(spec)) for spec in configured]
        try:
            results = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    # Provider priority is explicit and stable, independent of completion order.
    review = None
    failures = []
    for result in results:
        if result.error:
            failures.append(f'{result.source}: {result.error}')
            continue
        decision = fingerprint(result.rows,phash)
        if decision.state == 'matched':
            return result.rows,result.source
        if decision.state == 'review' and review is None:
            review = (result.rows,result.source)
    if review:
        return review
    if failures:
        raise RuntimeError('Fingerprint search incomplete; retry required. '+'; '.join(failures))
    return [],'none'


def sync_fingerprints(phash, keys, request, emit=None):
    """Synchronous library-health facade using the same decision policy."""
    from app.integrations import stashbox as transport
    failures,review,configured = [],None,False
    for key,label,endpoint,query in (
        ('stashdb','StashDB',transport.STASHDB_ENDPOINT,transport.STASHDB_QUERY),
        ('tpdb','TPDB',transport.TPDB_ENDPOINT,transport.TPDB_QUERY),
        ('fansdb','FansDB',transport.FANSDB_ENDPOINT,transport.STASHDB_QUERY),
        ('javstash','JAVStash',transport.JAVSTASH_ENDPOINT,transport.JAVSTASH_QUERY)):
        if not keys.get(key):
            continue
        configured = True
        try:
            rows = request(phash,endpoint,keys[key],query,'scene')
            result = fingerprint(rows,phash)
            if result.state == 'matched':
                return rows,label
            if result.state == 'review' and review is None:
                review = (rows,label)
        except Exception as exc:
            failures.append(label+': '+str(exc))
            if emit:
                emit(f'  {label}: lookup unavailable; retry required')
    if review:
        return review
    if not configured:
        raise RuntimeError('No fingerprint search databases configured')
    if failures:
        raise RuntimeError('Fingerprint search incomplete; retry required. '+'; '.join(failures))
    return [],'none'
