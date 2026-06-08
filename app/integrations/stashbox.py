import httpx
import logging
import database as db

logger = logging.getLogger(__name__)

STASHDB_ENDPOINT = "https://stashdb.org/graphql"
TPDB_ENDPOINT    = "https://theporndb.net/graphql"
FANSDB_ENDPOINT  = "https://fansdb.cc/graphql"
JAVSTASH_ENDPOINT = "https://javstash.org/graphql"

STASHDB_QUERY = """
query FindScenesByFullFingerprints($fingerprints: [FingerprintQueryInput!]!) {
  findScenesByFullFingerprints(fingerprints: $fingerprints) {
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
    resp.raise_for_status()
    data = resp.json()
    
    if "errors" in data:
        raise RuntimeError(f"Stash-box error: {data['errors']}")
        
    result = data.get("data", {})
    scenes: list = []
    if "findScenesByFullFingerprints" in result:
        scenes = result["findScenesByFullFingerprints"] or []
    elif "findScenesBySceneFingerprints" in result:
        nested = result["findScenesBySceneFingerprints"] or []
        scenes = [s for group in nested for s in (group or [])]
        
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
            m = await query_stashbox(client, phash_hex, JAVSTASH_ENDPOINT, api_keys["javstash"], STASHDB_QUERY, "full")
            if m: return m, "JAVStash"
        except Exception as e:
            _emit(f"  WARNING: JAVStash failed ({e})")
            raise RuntimeError(f"All fallback databases failed. Last error: JAVStash failed ({e})") from e
            
    return [], "none"
