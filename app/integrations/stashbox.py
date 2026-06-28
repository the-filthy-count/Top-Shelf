import httpx
import logging
import database as db

logger = logging.getLogger(__name__)

STASHDB_ENDPOINT = "https://stashdb.org/graphql"
TPDB_ENDPOINT    = "https://theporndb.net/graphql"
FANSDB_ENDPOINT  = "https://fansdb.cc/graphql"
JAVSTASH_ENDPOINT = "https://javstash.org/graphql"

# stash-box (StashDB / FansDB / JAVStash) dropped findScenesByFullFingerprints
# in favour of findScenesBySceneFingerprints — same field TPDB has used all
# along. Keep the flat [FingerprintQueryInput!]! input the new stash-box
# schema accepts; query_stashbox handles flat vs nested results downstream.
STASHDB_QUERY = """
query FindScenesBySceneFingerprints($fingerprints: [FingerprintQueryInput!]!) {
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
        
    result = data.get("data", {})
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
            scenes = [s for group in raw for s in (group or [])]
        else:
            scenes = list(raw)
        
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
    def _emit(msg):
        if emit_cb:
            emit_cb(msg)
        else:
            logger.info(msg)

    async with await get_async_client() as client:
        try:
            m = await query_stashbox(client, phash_hex, STASHDB_ENDPOINT, api_keys["stashdb"], STASHDB_QUERY, "full")
            if m: return m, "StashDB"
        except Exception as e:
            _emit(f"  WARNING: StashDB failed ({e}), trying TPDB...")
            
        try:
            m = await query_stashbox(client, phash_hex, TPDB_ENDPOINT, api_keys["tpdb"], TPDB_QUERY, "scene")
            if m: return m, "TPDB"
        except Exception as e:
            _emit(f"  WARNING: TPDB failed ({e}), trying FansDB...")
            
        try:
            m = await query_stashbox(client, phash_hex, FANSDB_ENDPOINT, api_keys["fansdb"], STASHDB_QUERY, "full")
            if m: return m, "FansDB"
        except Exception as e:
            _emit(f"  WARNING: FansDB failed ({e}), trying JAVStash...")
            
        try:
            # JAVStash uses TPDB's nested fingerprints shape but with
            # stricter non-null validation — see JAVSTASH_QUERY.
            m = await query_stashbox(client, phash_hex, JAVSTASH_ENDPOINT, api_keys["javstash"], JAVSTASH_QUERY, "scene")
            if m: return m, "JAVStash"
        except Exception as e:
            _emit(f"  WARNING: JAVStash failed ({e})")
            raise RuntimeError(f"All fallback databases failed. Last error: JAVStash failed ({e})") from e
            
    return [], "none"
