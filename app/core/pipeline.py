import asyncio
from contextlib import nullcontext
from pathlib import Path
import database as db
from download_safety import require_complete_queue_video, PartialDownloadPending, VIDEO_EXTENSIONS
from app.core.phash import compute_phash, get_video_duration
from app.core.queue_changes import SupersededWork


import json

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
        from app.core.video_matching.engine import MatchingEngine
        return await MatchingEngine(db,self.callbacks,phase=self.set_phase,emit=self.emit,
            complete=require_complete_queue_video,duration=get_video_duration,
            phash=compute_phash).scene(video)

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
                    search_options = {"include_iafd":bool(desired.get("include_iafd"))}
                    if desired.get("search_generation"):
                        search_options["search_generation"] = desired["search_generation"]
                    count = await asyncio.to_thread(self.callbacks["search_suggestions"], filename, **search_options)
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
