"""Source-specific joint-cast candidate discovery; no filing or filesystem effects."""
def search_joint_cast(performers, sources, post, performer_query, scene_query, rest_get, emit, saved_ids=None):
    """Find joint appearances using unambiguous, source-local performer IDs."""
    from app.core.matching import _performer_name_tokens_match
    results = []
    complete = True
    for source, endpoint, key in sources:
        if not key:
            continue
        try:
            ids = []
            for name in performers:
                matches = (saved_ids or {}).get(name, {}).get(source)
                if matches is None:
                    data = post(endpoint, key, performer_query, {"term": name, "limit": 25})
                    if not isinstance(data.get("searchPerformer"), list):
                        raise ValueError("Source returned no performer lookup list")
                    matches = {p["id"] for p in data["searchPerformer"]
                               if p.get("id") and _performer_name_tokens_match(name, p.get("name", ""))}
                if len(matches) != 1:
                    break
                ids.append(next(iter(matches)))
            if len(ids) != len(performers) or len(set(ids)) != len(performers):
                continue
            if source == "TPDB":
                from app.integrations.tpdb_joint_scenes import joint_scenes
                scenes, source_complete = joint_scenes(ids, key, rest_get)
                results.extend(dict(scene, _search_performer_ids={"source": source, "ids": dict(zip(performers, ids))}) for scene in scenes)
                complete = complete and source_complete
                continue
            seen = set()
            for page in range(1, 21):
                data = post(endpoint, key, scene_query, {"input": {
                    "page": page, "per_page": 100, "sort": "DATE", "direction": "DESC",
                    "performers": {"value": ids, "modifier": "INCLUDES_ALL"},
                }})
                rows = (data.get("queryScenes") or {}).get("scenes")
                if not isinstance(rows, list):
                    raise ValueError("Source returned no filmography list")
                for scene in rows:
                    cast_ids = {(p.get("performer") or {}).get("id") for p in scene.get("performers") or []}
                    if set(ids).issubset(cast_ids) and scene.get("id") not in seen:
                        seen.add(scene.get("id"))
                        results.append(dict(scene, _source=source, _search_performer_ids={"source": source, "ids": dict(zip(performers, ids))}))
                if len(rows) < 100:
                    break
            else:
                complete = False
        except Exception as exc:
            complete = False
            emit(f"  Joint-performer search failed ({source}): {exc}")
    return results, complete

