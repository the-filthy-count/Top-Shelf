"""Source-specific joint-cast candidate discovery; no filing or filesystem effects."""
def search_joint_cast(performers, sources, post, performer_query, scene_query, rest_get, emit):
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
                data = post(endpoint, key, performer_query, {"term": name, "limit": 25})
                matches = {p["id"] for p in data.get("searchPerformer") or []
                           if p.get("id") and _performer_name_tokens_match(name, p.get("name", ""))}
                if len(matches) != 1:
                    break
                ids.append(next(iter(matches)))
            if len(ids) != len(performers) or len(set(ids)) != len(performers):
                continue
            if source == "TPDB":
                from tpdb_joint_scenes import joint_scenes
                scenes, source_complete = joint_scenes(ids, key, rest_get)
                results.extend(scenes)
                complete = complete and source_complete
                continue
            data = post(endpoint, key, scene_query, {"input": {
                "page": 1, "per_page": 100, "sort": "DATE", "direction": "DESC",
                "performers": {"value": ids, "modifier": "INCLUDES_ALL"},
            }})
            for scene in (data.get("queryScenes") or {}).get("scenes") or []:
                cast_ids = {(p.get("performer") or {}).get("id") for p in scene.get("performers") or []}
                if set(ids).issubset(cast_ids):
                    results.append(dict(scene, _source=source))
        except Exception as exc:
            complete = False
            emit(f"  Joint-performer search failed ({source}): {exc}")
    return results, complete

