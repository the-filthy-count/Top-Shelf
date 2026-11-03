"""Resolve authoritative cast for accepted TPDB scenes before filing."""


def hydrate_tpdb_cast(scene, fetch):
    """Picker payloads contain names only; retrieve IDs from the exact scene.

    Never infer library identity from a name, alias, or download folder.
    Failed lookups leave the queue untouched so acceptance can be retried.
    """
    sid = str(scene.get("id") or "").strip()
    try:
        authoritative = fetch({"source_db": "TPDB", "source_id": sid})
    except Exception as exc:
        raise ValueError("Could not verify the selected TPDB scene's performer IDs. Retry when TPDB is available.") from exc
    if not authoritative or str(authoritative.get("id") or "") != sid:
        raise ValueError("Could not verify the selected TPDB scene's performer IDs. Refresh the match and retry.")
    # Keep the selected title and other UI metadata, but never the picker's
    # name-only cast or the parent movie's cast when a scene was selected.
    return {**scene, "performers": authoritative.get("performers") or []}
