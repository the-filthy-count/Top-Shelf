import asyncio
from contextlib import nullcontext
from pathlib import Path
import database as db
from download_safety import require_complete_queue_video, PartialDownloadPending, VIDEO_EXTENSIONS
from app.core.phash import compute_phash, get_video_duration
from app.core.matching import _strip_ts_uid, _release_name_with_folders
from app.integrations.stashbox import query_with_fallback
from app.core.queue_changes import assert_current, SupersededWork


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
        self.job_id = None

    def set_phase(self, phase):
        self.state["pipeline_phase"] = phase
        if self.job_id:
            db.processing_job_checkpoint(self.job_id, phase)

    async def process_single(self, video: Path) -> dict:
        from app.core.queue_changes import assert_current
        self.set_phase("Reading media")
        settings = db.get_settings()
        # DB filename key = posix path relative to source_dir when the
        # file sits in a subdirectory (``Jane Smith/scene.mp4``); bare
        # basename otherwise. Keeps root-level files backwards-compatible
        # with existing rows while nested files carry their folder name
        # for disambiguation and metadata matching.
        filename = db.queue_file_key(video, settings)
        await asyncio.to_thread(require_complete_queue_video, video, settings)
        db.upsert_file(filename)
        await asyncio.to_thread(db.queue_search_record, filename)
        from app.core.queue_media import read, save_match, assert_signature
        self.emit(f"FILE {filename}")
        self.set_phase("Reading media")
        self.set_phase("Fingerprinting")
        media = await asyncio.to_thread(read, filename, video,
            duration_probe=get_video_duration, fingerprint_probe=compute_phash)
        phash = media.get("phash")
        await asyncio.to_thread(require_complete_queue_video, video, settings)
        db.update_file(filename, status="processing", phash=phash)
        api_keys = {
            "stashdb": settings.get("api_key_stashdb", ""),
            "tpdb": settings.get("api_key_tpdb", ""),
            "fansdb": settings.get("api_key_fansdb", ""),
            "javstash": (settings.get("api_key_javstash") or "").strip(),
        }

        # Early-resolve against the indexed library. Touches the
        # filesystem (file delete on dup discard) so off-thread.
        self.set_phase("Checking library")
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

        saved = json.loads(media.get("pipeline_match_json") or "null")
        if saved:
            assert_current()
            assert_signature(video, media["media_signature"])
            scene = saved["scene"]
            if import_vice:
                scene["_force_vice"] = import_vice
            self.set_phase("Filing saved match")
            assert_current()
            return await asyncio.to_thread(self.callbacks["file_scene_from_match"], video, scene, saved["source"])

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
                    scene = await asyncio.to_thread(self.callbacks["fetch_grab_scene"], grab)
                except Exception as exc:
                    scene = None
                    self.emit(f"  Grab tag: scene retrieval failed ({exc}), falling back to phash")
                if scene:
                    await asyncio.to_thread(save_match, filename, video, {"scene": scene, "source": label}, expected_signature=media["media_signature"])
                    if import_vice:
                        scene["_force_vice"] = import_vice
                    self.set_phase("Filing")
                    assert_current()
                    result = await asyncio.to_thread(
                        self.callbacks.get("file_scene_from_match"),
                        video, scene, label,
                    )
                    if result.get("status") == "filed":
                        db.mark_scene_grab_consumed(grab.get("id"))
                    return result
                self.emit("  Grab tag: authoritative scene unavailable, falling back to phash")

        # Lookup (Async)
        try:
            self.set_phase("Matching")
            self.emit("  Lookup: querying databases...")
            matches, source = await query_with_fallback(phash, api_keys, emit_cb=self.emit)
        except Exception as e:
            self.emit(f"  Lookup: FAILED - {e}")
            db.update_file(filename, status="error", error=str(e))
            return {"status": "error", "error": str(e)}

        assert_current()
        assert_signature(video, media["media_signature"])
        if not matches:
            self.emit("  Lookup: no match found")
            # Unconfirmed filename hints must not move files into identity folders.
            db.update_file(filename, status="unmatched")
            # Persist the next step under this file's stable identity. The
            # same worker will pick it up after fingerprint work, not a pool.
            await asyncio.to_thread(self.callbacks["queue_suggestions"], filename)
            await asyncio.to_thread(self.callbacks["prewarm_queue_thumbs"], filename)
            return {"status": "unmatched"}

        from app.core.fingerprint_selection import select_scene, save_review
        matched_scene = select_scene(matches, phash)
        if matched_scene is None:
            self.emit('  Lookup: competing fingerprint matches — saved for review')
            await asyncio.to_thread(save_review, db, filename, matches, source, phash)
            return {"status": "unmatched", "review_required": True}
        await asyncio.to_thread(save_match, filename, video, {"scene": matched_scene, "source": source}, expected_signature=media["media_signature"])
        if import_vice and isinstance(matched_scene, dict):
            matched_scene["_force_vice"] = import_vice
        # File move + image download + NFO write — sync I/O off-thread.
        self.set_phase("Filing")
        assert_current()
        return await asyncio.to_thread(
            self.callbacks.get("file_scene_from_match"),
            video, matched_scene, source,
        )

    async def run_job(self, job):
        """Claim one current master-list record; never execute a stale path list."""
        from app.core.queue_changes import adopt_processing, release_processing
        filename, table = job["filename"], job["queue_table"]
        self.job_id = await asyncio.to_thread(db.processing_job_start, filename, table,
            expected_file_id=job["file_id"], expected_revision=job["revision"])
        if not self.job_id:
            return {"status": "busy"}
        adopt_processing(self.job_id)
        result = {"status": "error", "error": "Worker interrupted before completion"}
        try:
            settings = await asyncio.to_thread(db.get_settings)
            if table == "processed_movies":
                jav = filename.startswith("jav://")
                relative = filename[6:] if jav else filename
                raw = (settings.get("jav_download_watch_dir" if jav else "movies_source_dir") or "").strip()
                if not raw:
                    raise ValueError("Movie inbox is not configured")
                root = Path(raw).expanduser().resolve()
                video = root / relative
                if root not in video.resolve().parents:
                    raise ValueError("Movie path escapes inbox")
            else:
                raw = (settings.get("vice_download_watch_dir") or "").strip()
                video = await asyncio.to_thread(db.resolve_source_video_path,
                    Path(settings.get("source_dir") or ""), filename,
                    vice_dl_root=Path(raw).expanduser() if raw else None)
            if not video or not video.is_file():
                observed = await asyncio.to_thread(db.processing_work_result, self.job_id)
                if observed.get("status") == "filed":
                    result = {"status": "completed"}
                    return result
                raise FileNotFoundError("Source file unavailable")
            self.state["current_file"] = filename
            # Read the claimed revision, including the actual next step, from
            # the saved list. A concurrent request must not change this job.
            with db.get_conn() as conn:
                row = conn.execute("SELECT desired_json FROM queue_change_history WHERE file_id=? AND state='running' ORDER BY revision DESC LIMIT 1", (self.job_id,)).fetchone()
            desired = json.loads(row[0]) if row else {}
            with self.callbacks.get("job_context", lambda _: nullcontext())(desired):
                if desired.get("operation") == "suggestions":
                    self.set_phase("Searching suggestions")
                    await asyncio.to_thread(require_complete_queue_video, video, settings)
                    count = await asyncio.to_thread(self.callbacks["search_suggestions"], filename,
                        include_iafd=bool(desired.get("include_iafd")))
                    result = {"status": "completed", "added": count, "video_result": db.processing_work_result(self.job_id)}
                elif table == "processed_movies":
                    self.set_phase("Matching movie")
                    result = await asyncio.to_thread(self.callbacks["process_movie"], relative,
                        src=root, dest_kind="jav" if jav else None)
                    if not result:
                        with db.get_conn() as conn:
                            row = conn.execute("SELECT status,error FROM processed_movies WHERE filename=?", (filename,)).fetchone()
                        result = {"status": row[0] if row else "error", "error": row[1] if row else "Movie record disappeared"}
                else:
                    result = await self.process_single(video)
        except SupersededWork as exc:
            result = {"status": "superseded", "error": str(exc)}
        except PartialDownloadPending as exc:
            self.emit(f"  Deferred: {exc}")
            result = {"status": "deferred", "error": str(exc)}
        except Exception as exc:
            self.emit(f"  File failed: {filename}: {exc}")
            update = db.update_movie if table == "processed_movies" else db.update_file
            await asyncio.to_thread(update, filename, status="error", error=str(exc))
            result = {"status": "error", "error": str(exc)}
        finally:
            try:
                await asyncio.to_thread(db.processing_job_finish, self.job_id,
                    result.get("status", "error"), result.get("error"))
            finally:
                release_processing()
                self.job_id = None
        return result

    async def run_pipeline(self, filenames: list = None) -> dict:
        # Discovery only registers files. All execution below reads the same
        # durable work list, including jobs added while this worker is running.
        def register_work():
            settings = db.get_settings()
            root = Path(settings.get("source_dir") or "")
            raw = (settings.get("vice_download_watch_dir") or "").strip()
            vice = Path(raw).expanduser() if raw else None
            if filenames is not None:
                videos = [p for f in filenames if (p := db.resolve_source_video_path(root, f, vice_dl_root=vice)) is not None]
            else:
                videos = []
                for directory in (root if settings.get("source_dir") else None, vice):
                    if directory and directory.is_dir():
                        videos.extend(p for p in directory.rglob("*") if p.is_file()
                            and p.suffix.lower() in VIDEO_EXTENSIONS
                            and not any(part.startswith(".") for part in p.relative_to(directory).parts)
                            and not (directory == vice and "sample" in p.name.lower()))
            for video in sorted(set(videos)):
                filename = db.queue_file_key(video, settings)
                db.upsert_file(filename)
                db.processing_job_enqueue(filename)

        await asyncio.to_thread(register_work)
        while not self.lock.acquire(blocking=False):
            await asyncio.sleep(.25)
        outcomes = {}
        results = {}
        try:
            self.state.update(running=True, pipeline_total=0, pipeline_done=0, log=[])
            self.emit("PIPELINE START")
            while True:
                ready = await asyncio.to_thread(db.processing_work_ready)
                if not ready:
                    break
                self.state["pipeline_total"] = self.state["pipeline_done"] + ready[0]["ready_count"]
                # Refresh after every job: a rename, new manual action, or
                # newly registered download must take effect immediately.
                result = await self.run_job(ready[0])
                results[ready[0]["file_id"]] = result.get("video_result") or result
                status = result.get("status", "error")
                outcomes[status] = outcomes.get(status, 0) + 1
                self.state["pipeline_done"] += 1
                self.emit("---")
                self.flush_emit_buffer()
            self.emit("PIPELINE COMPLETE")
            if outcomes:
                summary = " · ".join(f"{n} {('waiting for download' if s == 'deferred' else s)}" for s,n in outcomes.items())
                await asyncio.to_thread(db.notification_add, "pipeline", summary, ttl_seconds=240)
            return results
        finally:
            self.flush_emit_buffer()
            self.state.update(pipeline_phase=None, running=False, current_file=None, pipeline_total=0, pipeline_done=0)
            self.lock.release()
