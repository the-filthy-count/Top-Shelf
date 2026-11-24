import httpx
import logging
import database as db

logger = logging.getLogger(__name__)

STASHDB_ENDPOINT = "https://stashdb.org/graphql"
TPDB_ENDPOINT    = "https://theporndb.net/graphql"
FANSDB_ENDPOINT  = "https://fansdb.cc/graphql"
JAVSTASH_ENDPOINT = "https://javstash.org/graphql"

# stash-box (StashDB / FansDB) — findScenesBySceneFingerprints. Field name
# unified with TPDB, but the input signature is now the strict
# [[FingerprintQueryInput!]!]! shape (matches JAVSTASH_QUERY) — the
# previous flat [FingerprintQueryInput!]! was rejected with
# GRAPHQL_VALIDATION_FAILED after a schema tightening. query_stashbox's
# result parser sniffs list-vs-flat downstream so the nested response
# (one inner list per input fingerprint) still flattens cleanly.
STASHDB_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [[FingerprintQueryInput!]!]!) {
  findScenesBySceneFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
    fingerprints { hash algorithm duration }
  }
}
"""

TPDB_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [[FingerprintQueryInput]]!) {
  findScenesBySceneFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
    fingerprints { hash algorithm duration }
  }
}
"""

# JAVStash validates the fingerprint argument strictly (non-null inner
# list and non-null items). TPDB's looser [[FingerprintQueryInput]]!
# is rejected there with GRAPHQL_VALIDATION_FAILED. Keep separate from
# TPDB_QUERY so a server-side schema change on either is patched in
# isolation.
JAVSTASH_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [[FingerprintQueryInput!]!]!) {
  findScenesBySceneFingerprints(fingerprints: $fingerprints) {
    id title release_date
    studio { id name }
    performers { performer { id name gender } }
    images { url width height }
    fingerprints { hash algorithm duration }
  }
}
"""

async def get_async_client():
    return httpx.AsyncClient(timeout=30.0)

async def query_stashbox(client: httpx.AsyncClient, phash_hex, endpoint, api_key, query, fingerprint_var):
    if fingerprint_var == "full":
        variables = {"fingerprints": [{"hash": phash_hex, "algorithm": "PHASH"}]}
    else:
        variables = {"fingerprints": [[{"hash": phash_hex, "algorithm": "PHASH"}]]}

    resp = await client.post(
        endpoint,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json", "ApiKey": api_key}
    )
    # GraphQL endpoints return 4xx with a JSON body that pinpoints the
    # broken field (e.g. "Cannot query field 'X' on type 'Y'"). httpx's
    # raise_for_status() loses that body in HTTPStatusError, so the
    # pipeline log just says "422 Unprocessable Entity" with no hint of
    # which field broke. Unwrap errors[] before falling back so the
    # query_with_fallback emit prints the actual cause.
    if resp.status_code >= 400:
        try:
            err_body = resp.json()
            err_msgs = err_body.get("errors") if isinstance(err_body, dict) else None
            if err_msgs:
                raise RuntimeError(f"HTTP {resp.status_code}: {err_msgs}")
        except ValueError:
            pass
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '')[:500]}")
    data = resp.json()

    if "errors" in data:
        raise RuntimeError(f"Stash-box error: {data['errors']}")
        
    if not isinstance(data, dict) or not isinstance(data.get("data"), dict):
        raise RuntimeError("Invalid fingerprint response: missing data")
    result = data["data"]
    field = next((key for key in ("findScenesByFullFingerprints", "findScenesBySceneFingerprints") if key in result), None)
    if field is None or not isinstance(result[field], list):
        raise RuntimeError("Invalid fingerprint response: missing scene results")
    scenes: list = []
    if "findScenesByFullFingerprints" in result:
        scenes = result["findScenesByFullFingerprints"] or []
    elif "findScenesBySceneFingerprints" in result:
        raw = result["findScenesBySceneFingerprints"] or []
        # TPDB returns [[Scene]] (one inner list per fingerprint); the
        # renamed stash-box field on StashDB / FansDB returns [Scene]
        # flat. Sniff the first element type rather than trusting the
        # variant flag, since the field name is shared but signatures
        # differ across sources.
        if raw and isinstance(raw[0], list):
            if any(not isinstance(group, list) for group in raw):
                raise RuntimeError("Invalid fingerprint response: malformed scene group")
            scenes = [s for group in raw for s in group]
        else:
            scenes = list(raw)
        
    if any(not isinstance(scene, dict) or not isinstance(scene.get("id"), str)
           or not scene["id"].strip() for scene in scenes):
        raise RuntimeError("Invalid fingerprint response: malformed scene entry")
    if scenes:
        src = ""
        if "stashdb" in endpoint:
            src = "stashdb"
        elif "fansdb" in endpoint:
            src = "fansdb"
        elif "javstash" in endpoint:
            src = "javstash"
        elif "theporndb" in endpoint or "tpdb" in endpoint:
            src = "tpdb"
            
        if src:
            try:
                for sc in scenes:
                    sid = (sc.get("id") or "").strip() if isinstance(sc, dict) else ""
                    fps = sc.get("fingerprints") or [] if isinstance(sc, dict) else []
                    if sid and fps:
                        db.scene_fingerprints_upsert(src, sid, fps)
            except Exception:
                pass
    return scenes

async def query_with_fallback(phash_hex, api_keys, emit_cb=None):
    def report(message):
        (emit_cb or logger.info)(message)
    from app.core.fingerprint_selection import select_scene
    ambiguous = None
    failures = []
    configured = 0
    async with await get_async_client() as client:
        for key, label, endpoint, query in (
            ("stashdb", "StashDB", STASHDB_ENDPOINT, STASHDB_QUERY),
            ("tpdb", "TPDB", TPDB_ENDPOINT, TPDB_QUERY),
            ("fansdb", "FansDB", FANSDB_ENDPOINT, STASHDB_QUERY),
            ("javstash", "JAVStash", JAVSTASH_ENDPOINT, JAVSTASH_QUERY),
        ):
            api_key = (api_keys.get(key) or "").strip()
            if not api_key:
                continue
            configured += 1
            try:
                matches = await query_stashbox(client, phash_hex, endpoint, api_key, query, "scene")
                if matches:
                    if select_scene(matches, phash_hex) is not None:
                        return matches, label
                    if ambiguous is None:
                        ambiguous = (matches, label)
                    report(f"  {label}: ambiguous fingerprints; checking remaining databases")
            except Exception as exc:
                failures.append(f"{label}: {exc}")
                report(f"  WARNING: {label} lookup failed ({exc}); continuing fallback")
    if ambiguous is not None:
        return ambiguous
    if not configured:
        raise RuntimeError("No fingerprint search databases configured")
    if failures:
        raise RuntimeError("Fingerprint search incomplete; retry required. " + "; ".join(failures))
    return [], "none"