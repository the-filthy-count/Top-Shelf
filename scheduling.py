"""Background schedule settings and validation shared by the API and scheduler."""
# id, callback name, label, unit, default interval, minimum, maximum
INTERVAL_JOBS = (
    ("pending_check", "_check_pending_files", "Unmatched file checks", "seconds", 30, 10, 3600),
    ("movie_pending_check", "_check_pending_movies", "Movie file checks", "seconds", 30, 10, 3600),
    ("download_check", "_check_pending_downloads", "Download import checks", "seconds", 30, 10, 3600),
    ("rss_cache_refresh", "_refresh_rss_cache", "Download RSS refresh", "minutes", 60, 5, 10080),
    ("news_rss_cache_refresh", "_refresh_news_rss_cache", "News RSS refresh", "minutes", 60, 5, 10080),
    ("studio_logos_fetch", "_studio_logo_fetcher_job", "Missing studio logos", "hours", 6, 1, 720),
    ("wanted_library_reconcile", "_wanted_library_reconcile_job", "Wanted / library reconciliation", "minutes", 15, 5, 1440),
    ("suggestions_trickle", "_suggestions_trickle_job", "Unmatched search suggestions", "minutes", 5, 1, 1440),
    ("content_cache_refresh", "_refresh_hourly_content_cache", "Homepage content refresh", "minutes", 60, 5, 10080),
    ("tag_scene_pools_refresh", "_refresh_tag_scene_pools", "Tag scene pool refresh", "minutes", 25, 5, 10080),
    ("session_purge", "db.purge_expired_sessions", "Expired session cleanup", "hours", 1, 1, 168),
    ("activity_log_prune", "_activity_log_prune_job", "Activity log cleanup", "hours", 24, 1, 168),
    ("net_error_flush", "_net_error_flush_expired", "Repeated network error summaries", "minutes", 2, 1, 60),
    ("scene_grab_purge", "db.purge_orphan_scene_grabs", "Old download tracking cleanup", "hours", 24, 1, 168),
)

def interval_key(job_id):
    return "schedule_" + job_id + "_interval"

EXTRA_FIELDS = (
    ("favourites_scan_enabled", "Scheduled library folder scan", "boolean", "false", 0, 1, "favourites_scan"),
    ("favourites_scan_hour", "Library scan start hour", "hour", 3, 0, 23, "favourites_scan"),
    ("favourites_scan_frequency_h", "Library scan interval", "hours", 24, 1, 8760, "favourites_scan"),
    ("iafd_trickle_enabled", "Background IAFD enrichment", "boolean", "false", 0, 1, "iafd_trickle"),
    ("iafd_trickle_interval_minutes", "IAFD enrichment interval", "minutes", 5, 1, 1440, "iafd_trickle"),
    ("library_phash3_rescan_hour", "Perceptual hash check start hour", "hour", 5, 0, 23, "library_phash3_rescan"),
    ("library_phash3_check_frequency_h", "Perceptual hash check interval", "hours", 24, 1, 8760, "library_phash3_rescan"),
)
LEGACY_FIELDS = (
    ("retry_enabled", "Automatic retry", "boolean", "true", 0, 1, "retry"),
    ("retry_hour", "Retry start hour", "hour", 1, 0, 23, "retry"),
    ("retry_frequency_h", "Retry interval", "hours", 24, 1, 8760, "retry"),
    ("tpdb_sync_enabled", "TPDB sync", "boolean", "false", 0, 1, "tpdb_sync"),
    ("tpdb_sync_hour", "TPDB sync start hour", "hour", 2, 0, 23, "tpdb_sync"),
    ("tpdb_sync_frequency_h", "TPDB sync interval", "hours", 24, 1, 8760, "tpdb_sync"),
    ("library_phash3_rescan_enabled", "Recurring perceptual hashes", "boolean", "false", 0, 1, "library_phash3_rescan"),
    ("library_phash3_rescan_interval_days", "Minimum time between hashes for each video", "days", 30, 1, 365, "library_phash3_rescan"),
)

def fields():
    return [(interval_key(j), label, unit, default, low, high, j) for j, _, label, unit, default, low, high in INTERVAL_JOBS] + list(EXTRA_FIELDS)

def validate_schedule_settings(settings):
    if not isinstance(settings, dict):
        raise ValueError("Settings must be an object")
    for key, label, unit, default, low, high, _ in fields() + list(LEGACY_FIELDS):
        if key not in settings:
            continue
        value = str(settings[key]).strip()
        if not value:
            value = str(default)
        if unit == "boolean":
            if value.lower() not in {"true", "false"}:
                raise ValueError(label + ": choose enabled or disabled")
            settings[key] = value.lower()
        else:
            if not value.isascii() or not value.isdigit() or not low <= int(value) <= high:
                raise ValueError(f"{label}: enter a whole number from {low} to {high}")
            settings[key] = str(int(value))

ACTIVE_INTERVAL_SECONDS = {job_id: default * {"seconds": 1, "minutes": 60, "hours": 3600}[unit] for job_id, _, _, unit, default, _, _ in INTERVAL_JOBS}
