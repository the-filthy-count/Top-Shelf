"""Review candidates use the same evidence and policy as manual searches.

Text evidence never authorizes automatic filing. Partial searches preserve
existing suggestions and remain eligible for retry.
"""
from pathlib import Path
from .journal import Journal, digest
from .decisions import score_suggestion


def run(db, filename, callbacks, *, limit=6, include_iafd=True, search_generation=""):
    from app.core import matching as evidence, suggestion_guard
    from app.core.queue_records import saved_ids as linked_ids
    from app.core.title_search_queries import title_search_queries
    from app.core.suggestion_rules import date_proximity
    if any(part.startswith('.') for part in Path(filename).parts):
        return 0
    record = db.queue_search_record(filename)
    if not record or not record.get('file_id'):
        raise ValueError('Suggestions require a registered video record')
    fid = record['file_id']
    if not suggestion_guard.claim(fid):
        return 0
    complete = False
    try:
        db.queue_suggestion_mark_attempt(filename)
        fields = record['fields']
        names = fields.get('performers') or []
        studio,title = fields.get('studio') or '',fields.get('title') or ''
        query = ' '.join(filter(None,[*names,studio,title,fields.get('term')]))
        if not query or not (names or studio):
            db.queue_suggestions_add(filename,[],replace=True,expected_record=record)
            complete = True
            return 0
        if fields.get('scene_number'):
            title += ' Scene '+str(fields['scene_number'])
        identities = db.suggestion_performer_ids(names)
        for name,ids in linked_ids(record).items():
            identities.setdefault(name,{}).update(ids)
        meta = evidence._build_file_meta(filename)
        date = fields.get('date') or ''
        meta.update(date=date,year=int(date[:4]) if date else 0,
                    scene_index=fields.get('scene_number'),cast_hint=len(names))
        journal = Journal(db,fid)
        sources = set(callbacks['sources']) | ({'IAFD'} if include_iafd else set())
        settings = db.get_settings()
        credentials = digest({key:value for key,value in settings.items() if key.startswith('api_key_')})
        candidates, failures = [],[]
        queries = title_search_queries(title) if title else ['']
        for source in sorted(sources):
            for title_query in queries:
                inputs = {'fields':fields,'query':title_query,'identities':identities,'credential':credentials,'generation':search_generation}
                def fetch():
                    batch = callbacks['search'](title=title_query or None,
                        performer=names[0] if names else None,studio=studio or None,
                        term=None if title_query else query,sources={source},
                        title_first=True,require_studio=not bool(names))
                    if getattr(batch,'warnings',[]):
                        candidates.extend(batch or [])
                        raise RuntimeError('; '.join(str(w) for w in batch.warnings))
                    return list(batch or [])
                try:
                    candidates.extend(journal.call('text:'+source,inputs,fetch))
                except Exception as exc:
                    from app.core.queue_changes import SupersededWork
                    if isinstance(exc,SupersededWork):
                        raise
                    failures.append(str(exc))
        rejects = db.queue_suggestion_rejects_for_file(filename)
        def rank(rows):
            accepted = []
            for raw in rows:
                scene = suggestion_guard.bind_saved_identities(raw,names,identities,record.get('studio_link'))
                if scene is None or not evidence._suggestion_identity_matches(names,studio,scene):
                    continue
                if len(names)>1 and not evidence._required_performers_in_cast(names,scene):
                    continue
                if not callbacks['allowed'](scene):
                    continue
                item = callbacks['normalize'](scene,filename)
                if not item or (item['source_db'],item['source_id']) in rejects:
                    continue
                reasons = []
                score = score_suggestion(query,scene,query_title=title or query,query_performers=names,
                    require_performers_in_cast=bool(names and evidence._required_performers_in_cast(names,scene)),
                    allow_joint_cast=len(names)>1,file_meta=meta,reasons=reasons)
                if score >= evidence._SUGGESTION_MIN_SCORE:
                    item.update(score=score,payload=dict(item.get('payload') or {},_reasons=reasons))
                    accepted.append(item)
            return accepted
        ranked = rank(candidates)
        if names and (len(names)>1 or not any(item['score']>=.8 for item in ranked)):
            inputs = {'names':names,'identities':identities,'credential':credentials,'generation':search_generation}
            def cast_fetch():
                rows,finished = callbacks['cast'](names,saved_ids=identities)
                if not finished:
                    ranked.extend(rank(rows))
                    raise RuntimeError('Cast search incomplete')
                return rows
            try:
                ranked.extend(rank(journal.call('cast',inputs,cast_fetch)))
            except Exception as exc:
                from app.core.queue_changes import SupersededWork
                if isinstance(exc,SupersededWork):
                    raise
                failures.append(str(exc))
        unique = {}
        for row in sorted(ranked,key=lambda r:(r['score'],date_proximity(r['payload'].get('release_date'),date)),reverse=True):
            unique.setdefault((row['source_db'],row['source_id']),row)
        top = list(unique.values())[:limit]
        complete = not failures
        count = db.queue_suggestions_add(filename,top,replace=complete,expected_record=record)
        journal.write('suggestions',{'fields':fields,'identities':identities},
            result={'count':count,'complete':complete},error='; '.join(failures) if failures else None)
        if top:
            callbacks['prewarm'](filename,top)
        return count
    finally:
        try:
            db.queue_suggestion_finish_attempt(filename,complete=complete)
        finally:
            suggestion_guard.release(fid)
