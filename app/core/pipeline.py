import asyncio
import threading
from pathlib import Path
import database as db
from app.core.phash import compute_phash, get_video_duration
from app.core.matching import _strip_ts_uid
from app.integrations.stashbox import query_with_fallback

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".m4v", ".avi", ".wmv", ".mov", ".flv", ".webm", ".ts", ".m2ts", ".mpg", ".mpeg"}

import json

def _scene_dict_from_grab_row(grab: dict) -> dict:
    try:
        performers = json.loads(grab.get("source_performers") or "[]")
        if not isinstance(performers, list): performers = []
    except Exception: performers = []
    poster = (grab.get("source_poster_url") or "").strip()
    return {
        "id":           grab.get("source_id") or "",
        "title":        grab.get("source_title") or "",
        "release_date": grab.get("source_date") or "",
        "studio":       {"name": grab.get("source_studio") or ""},
        "performers":   [{"performer": {"name": n, "gender": ""}} for n in performers if n],
        "images":       [{"url": poster, "width": 0, "height": 0}] if poster else [],
    }

class ProcessingPipeline:
    def __init__(self, processing_state, emit_cb, flush_emit_buffer_cb, pipeline_lock, callbacks):
        self.state = processing_state
        self.emit = emit_cb
        self.flush_emit_buffer = flush_emit_buffer_cb
        self.lock = pipeline_lock
        self.callbacks = callbacks

    async def process_single(self, video: Path) -> dict:
        settings = db.get_settings()
        # DB filename key = posix path relative to source_dir when the
        # file sits in a subdirectory (``Jane Smith/scene.mp4``); bare
        # basename otherwise. Keeps root-level files backwards-compatible
        # with existing rows while nested files carry their folder name
        # for disambiguation and metadata matching.
        try:
            src_root = Path(settings.get("source_dir", "")).expanduser().resolve()
            filename = video.resolve().relative_to(src_root).as_posix()
        except (ValueError, OSError):
            filename = video.name
        db.upsert_file(filename)
        self.emit(f"FILE {filename}")
        api_keys = {
            "stashdb": settings.get("api_key_stashdb", ""),
            "tpdb":    settings.get("api_key_tpdb", ""),
            "fansdb":  settings.get("api_key_fansdb", ""),
            "javstash": (settings.get("api_key_javstash") or "").strip(),
        }

        # Duration. ffprobe is sync and blocks for a real interval per
        # file — offload it so the FastAPI event loop can keep answering
        # /api/status, /api/queue, button-click POSTs, etc.
        duration = db.get_media_duration(filename)
        if duration:
            self.emit(f"  Duration: cached ({int(duration)}s)")
        else:
            try:
                duration = await asyncio.to_thread(get_video_duration, video)
                db.update_file(filename, status="processing", media_duration=int(duration))
                self.emit(f"  Duration: {int(duration)}s")
            except Exception as e:
                self.emit(f"  Duration: FAILED - {e}")
                duration = 0

        # Phash. compute_phash does 5×ffmpeg seeks + hashing — the
        # longest sync block in the pipeline. Must run in a thread or
        # the entire event loop sits frozen for ~5–15s per file.
        # `_phash_semaphore` (2 concurrent) inside compute_phash still
        # enforces the seek-concurrency cap.
        cached = db.get_phash(filename)
        if cached:
            phash = cached
            self.emit(f"  Phash: cached ({phash})")
        else:
            try:
                self.emit("  Phash: computing...")
                phash = await asyncio.to_thread(compute_phash, video, duration=duration)
                self.emit(f"  Phash: done ({phash})")
            except Exception as e:
                self.emit(f"  Phash: FAILED - {e}")
                db.update_file(filename, status="error", error=str(e))
                return {"status": "error", "error": str(e)}

        db.update_file(filename, status="processing", phash=phash)

        # Early-resolve against the indexed library. Touches the
        # filesystem (file delete on dup discard) so off-thread.
        early = await asyncio.to_thread(
            self.callbacks.get("queue_resolve_phash"), video, filename, phash,
        )
        if early is not None:
            return early

        # Import vice-folder hint: file dropped in
        # ``<source_dir>/Vices/<Name>/`` forces filing under that vice's
        # library folder regardless of the metadata match. Phash still
        # supplies title/performers/studio/tags for the NFO — this only
        # overrides the destination.
        import_vice = None
        try:
            resolver = self.callbacks.get("resolve_import_vice_dir")
            if callable(resolver):
                import_vice = resolver(video) or None
        except Exception:
            import_vice = None
        if import_vice:
            self.emit(f"  Import folder → vice '{import_vice}'")

        # Grab tag lookup
        try:
            ts_uid = ""
            for candidate in (
                video.name,
                video.parent.name if video.parent else "",
                video.parent.parent.name if video.parent and video.parent.parent else "",
                video.parent.parent.parent.name if video.parent and video.parent.parent and video.parent.parent.parent else "",
            ):
                if not candidate: continue
                _, found = _strip_ts_uid(candidate)
                if found:
                    ts_uid = found
                    break
            grab = db.lookup_scene_grab_by_ts_uid(ts_uid) if ts_uid else None
            if not grab:
                grab = db.lookup_scene_grab_for_filename(filename)
        except Exception as exc:
            self.emit(f"  Grab tag: lookup error ({exc})")
            grab = None

        if grab and (grab.get("kind") or "scene").lower() == "scene":
            if grab.get("source_id") and grab.get("source_title"):
                from app.core.filing import _stashbox_normalize_match_source
                label = _stashbox_normalize_match_source(grab.get("source_db") or "")
                self.emit(f"  Grab tag: matched {label}/{grab.get('source_id')} — auto-filing without phash lookup")
                try:
                    scene = _scene_dict_from_grab_row(grab)
                    if import_vice:
                        scene["_force_vice"] = import_vice
                    # File move + image download + NFO write — all sync I/O,
                    # off-thread so the event loop stays responsive.
                    result = await asyncio.to_thread(
                        self.callbacks.get("file_scene_from_match"),
                        video, scene, label,
                    )
                    if result.get("status") == "filed":
                        db.mark_scene_grab_consumed(grab.get("id"))
                        return result
                    self.emit(f"  Grab tag: file_scene_from_match returned {result.get('status')!r}, falling back to phash")
                except Exception as exc:
                    self.emit(f"  Grab tag: filing failed ({exc}), falling back to phash")

        # Lookup (Async)
        try:
            self.emit("  Lookup: querying databases...")
            matches, source = await query_with_fallback(phash, api_keys, emit_cb=self.emit)
        except Exception as e:
            self.emit(f"  Lookup: FAILED - {e}")
            db.update_file(filename, status="error", error=str(e))
            return {"status": "error", "error": str(e)}

        if not matches:
            self.emit("  Lookup: no match found")
            db.update_file(filename, status="unmatched")
            # These two callbacks fire-and-forget (thumb prewarm + async
            # name-search), but call them off-thread anyway so any sync
            # setup inside them doesn't stall the loop.
            await asyncio.to_thread(self.callbacks.get("prewarm_queue_thumbs"), filename)
            await asyncio.to_thread(self.callbacks.get("run_name_search_async"), filename)
            return {"status": "unmatched"}

        matched_scene = matches[0]
        if import_vice and isinstance(matched_scene, dict):
            matched_scene["_force_vice"] = import_vice
        # File move + image download + NFO write — sync I/O off-thread.
        return await asyncio.to_thread(
            self.callbacks.get("file_scene_from_match"),
            video, matched_scene, source,
        )

    async def run_pipeline(self, filenames: list = None) -> None:
        if not self.lock.acquire(blocking=False):
            return

        try:
            self.state["running"] = True
            self.state["log"] = []
            settings = db.get_settings()
            source_dir = Path(settings.get("source_dir", ""))
            
            if filenames:
                video_files = [
                    p for f in filenames
                    if (p := db.resolve_source_video_path(source_dir, f)) is not None
                ]
            else:
                # "Run all" — walk recursively so files nested under
                # performer- or studio-named folders (e.g.
                # ``Jane Smith/scene.mp4``) are picked up alongside
                # root-level files.
                collected: list[Path] = []
                if source_dir.exists() and source_dir.is_dir():
                    for p in source_dir.rglob("*"):
                        if p.is_file() and p.suffix.lower() in VIDEO_EXTENSIONS:
                            collected.append(p)
                video_files = sorted(collected)

            n = len(video_files)
            self.state["pipeline_total"] = n
            self.state["pipeline_done"] = 0
            self.emit(f"PIPELINE START - {n} file(s)")
            self.emit("---")
            
            outcomes = {"filed": 0, "unmatched": 0, "no_dir": 0, "error": 0}
            for i, video in enumerate(video_files):
                self.state["current_file"] = video.name
                result = await self.process_single(video)
                st = (result or {}).get("status", "error")
                if st in outcomes:
                    outcomes[st] += 1
                self.state["pipeline_done"] = i + 1
                self.emit("---")
                self.flush_emit_buffer()
                
            self.emit("PIPELINE COMPLETE")
            self.flush_emit_buffer()

            try:
                n_files = len(video_files)
                if n_files == 1:
                    sole = video_files[0].name
                    if outcomes["filed"]:
                        msg = f"Sorted: {sole}"
                    elif outcomes["no_dir"]:
                        msg = f"No directory for {sole}"
                    elif outcomes["unmatched"]:
                        msg = f"Unmatched: {sole}"
                    else:
                        msg = f"Failed: {sole}"
                    db.notification_add("pipeline", msg, ttl_seconds=240)
                elif n_files > 1:
                    parts = []
                    if outcomes["filed"]:
                        parts.append(f"{outcomes['filed']} sorted")
                    if outcomes['unmatched']:
                        parts.append(f"{outcomes['unmatched']} unmatched")
                    if outcomes["no_dir"]:
                        parts.append(f"{outcomes['no_dir']} no-dir")
                    if outcomes["error"]:
                        parts.append(f"{outcomes['error']} errors")
                    summary = " · ".join(parts) if parts else "no changes"
                    db.notification_add(
                        "pipeline",
                        f"{n_files} files processed · {summary}",
                        ttl_seconds=240,
                    )
            except Exception:
                pass
        except Exception as e:
            self.emit(f"PIPELINE ERROR: {e}")
            try:
                db.notification_add("error", f"Sorting error: {e}", ttl_seconds=400)
            except Exception:
                pass
            self.flush_emit_buffer()
        finally:
            self.state["running"] = False
            self.state["current_file"] = None
            self.state["pipeline_total"] = 0
            self.state["pipeline_done"] = 0
            self.lock.release()
