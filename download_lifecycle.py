"""Explain client completion separately from Top-Shelf filing/cleanup."""
def lifecycle(item, remove_enabled=False):
    status = str(item.get("status") or "").lower()
    filing = item.get("filing") or {}
    if item.get("failed"):
        return {"stage": "Failed", "reason": item.get("failure_reason") or item.get("status") or "Client reported a failure"}
    if filing.get("filed"):
        if filing.get("match_source") not in ("exact", "exact_filename", None, ""):
            return {"stage": "Possible library match", "reason": "Filename similarity found a library item; this does not confirm this download was imported."}
        return {"stage": "Cleanup pending" if remove_enabled else "Filed", "reason": "Filed in the library; waiting for client cleanup." if remove_enabled else "Filed in the library. Automatic client removal is disabled."}
    if any(word in status for word in ("unpack", "repair", "verify", "postprocess")) and not status.startswith("success"):
        return {"stage": "Post-processing", "reason": "The client is still unpacking, repairing or verifying the download."}
    try:
        complete = float(item.get("progress_pct") or 0) >= 100
    except (ValueError, TypeError):
        complete = False
    if complete or status.startswith("success"):
        return {"stage": "Awaiting import", "reason": "Download complete; no confirmed library filing yet. Check the download-watch folder or Unmatched."}
    return {"stage": "Downloading", "reason": item.get("status") or "Waiting for the download client"}
