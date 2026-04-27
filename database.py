"""
SQLite database for Top-Shelf phash filer.
"""

import json
import re
import sqlite3
import time
import unicodedata
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import os


def studio_logo_slug(text: str) -> str:
    """Normalised lookup key for the studio_logos table.

    Mirrors main.normalise() exactly so studio names from NFOs, scene
    metadata, RSS titles, and the seed-time filenames all collapse to the
    same key. Lowercased, hyphens/dots/underscores → spaces, non-alnum
    stripped, whitespace collapsed.
    """
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("-", " ").replace("_", " ").replace(".", " ")
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()

# Use /app/database if it exists (Docker), otherwise fall back to app directory
_db_dir    = Path("/app/database") if Path("/app/database").exists() else Path(__file__).parent
DB_PATH    = Path(os.environ.get("DB_PATH", str(_db_dir / "phash_filer.db")))
_in_docker = Path("/app/database").exists()

DEFAULTS = {
    "source_dir":        "/downloads/scenes" if _in_docker else "/path/to/your/downloads/scenes",
    "series_dir":        "/library/Series" if _in_docker else "/path/to/your/library/Series",
    "pattern_series":    "{studio} - S{year}E{month}{day} - {title}",
    "pattern_performer": "{performer} - S{year}E{month}{day} - {studio} - {title}",
    "api_key_stashdb":   "",
    "api_key_tpdb":      "",
    "api_key_fansdb":    "",
    "iafd_search_enabled": "true",
    #: IAFD background enrichment — slowly scrapes each library
    #: performer's filmography so the bulk matcher and library UI
    #: can read locally instead of hitting IAFD live. Tick every N
    #: minutes; queue-mentioned performers jump to the top of the
    #: pick order inside each tick.
    "iafd_trickle_enabled":          "true",
    "iafd_trickle_interval_minutes": "5",
    "iafd_trickle_refresh_days":     "30",
    "retry_enabled":      "true",
    "retry_hour":         "1",
    "retry_frequency_h":  "24",
    "features_dir":       "/library/Features" if _in_docker else "/path/to/your/library/Features",
    "movies_source_dir":  "/downloads/movies" if _in_docker else "/path/to/your/downloads/movies",
    "api_key_tmdb":       "",
    "stash_enabled":      "true",
    "stash_url":          "",
    "stash_api_key":      "",
    "jellyfin_enabled":   "true",
    "jellyfin_url":       "",
    "jellyfin_api_key":   "",
    "plex_enabled":       "true",
    "plex_url":           "",
    "plex_token":         "",
    "emby_enabled":       "true",
    "emby_url":           "",
    "emby_api_key":       "",
    "prowlarr_url":       "",
    "prowlarr_api_key":   "",
    "prowlarr_category":  "Top-Shelf",
    "prowlarr_category_movies": "",
    "download_rss_feeds": "[]",
    "prowlarr_torrent_client": "",
    "prowlarr_nzb_client": "",
    "nzbget_url":         "",
    "nzbget_user":        "nzbget",
    "nzbget_pass":        "",
    "dl_nzb_client":      "",
    "dl_nzb_host":        "",
    "dl_nzb_port":        "",
    "dl_nzb_user":        "",
    "dl_nzb_pass":        "",
    "dl_nzb_api_key":     "",
    "dl_torrent_client":  "",
    "dl_torrent_host":    "",
    "dl_torrent_port":    "",
    "dl_torrent_user":    "",
    "dl_torrent_pass":    "",
    "tpdb_sync_enabled":      "false",
    "tpdb_sync_frequency_h":  "24",
    "tpdb_sync_hour":         "2",
    "tpdb_sync_performer_dir": "",
    "tpdb_sync_studio_dir":   "",
    "tpdb_sync_to_favs":      "false",
    "alias_lookup_enabled":   "false",
    "library_index_queue_matching_enabled": "true",
    "library_walk_prune_dirs": "",
    "media_scan_enabled": "true",
    "submit_phash_enabled": "true",
    "submit_phash_stashdb": "true",
    "submit_phash_fansdb":  "true",
    "submit_scene_stashdb": "false",
    "submit_scene_fansdb":  "false",
    "manual_submit_stashdb_default": "false",
    "media_scan_debounce_mins": "5",
    "folder_watch_enabled":    "true",
    "folder_watch_hold_secs":  "60",
    "download_watch_enabled":  "false",
    "download_watch_dir":      "/downloads/complete" if _in_docker else "/path/to/your/downloads/complete",
    "movie_download_watch_dir": "",
    "download_watch_hold_secs": "300",
    "download_import_remove_client": "false",
    "favourites_scan_enabled": "false",
    "favourites_scan_hour":    "3",
    "library_phash3_rescan_enabled":       "false",
    "library_phash3_rescan_interval_days": "30",
    "site_abbreviations":      '{"brcc": "Backroom Casting Couch", "excogi": "Exploited College Girls", "nvg": "Net Video Girls", "brf": "Backroom Facials", "hmf": "Hot MILFs Fuck", "bex": "BrazzersExxtra", "ps": "PropertySex", "rk": "Reality Kings", "atk": "ATK", "ftv": "FTV Girls", "bangcasting": "Bang! Casting", "tonightsgf": "Tonights Girlfriend", "2drops": "2 Drops", "hookhot": "Hookup Hotshot", "spacejunk": "Space Junk", "shoplyfter": "Shoplyfter", "painal": "Painal", "wasteland": "Wasteland Ultra", "xxxjob": "XXX Job Interviews", "brandnew": "Brand New Amateurs", "czechcast": "Czech Casting", "czechstreet": "Czech Streets", "escortcast": "Escort Casting", "fuckt5": "Fuck Team Five", "hoby": "Hoby Buchanon", "perv": "Perv Principal"}',
    "site_rename_map":         '{"Shoplyfter Mylf": "Shoplyfter", "Net Girl": "Net Video Girls", "Tonights Girlfriend": "Tonights Girlfriend", "BANG! Casting": "Bang! Casting", "Manyvids 2 Drops Studio": "2 Drops"}',
    "filename_patterns":       "pipe|dot_date|dot_date_short|bracket_site|dash_date",
    "filename_strip_words":    "fuckingsession.com,lulustream.com,xvideoscom,xvideos.com,pornhub.com",
    "min_file_size_mb":        "10",
    "tag_blacklist":           "",
    "monitored_tags":          "[]",
    "log_retention_days":      "14",
}

DEFAULT_PERFORMER_DIRS = [
    {"path": "/library/Stars"   if _in_docker else "/path/to/your/library/Stars",   "label": "Stars",   "rank": 1},
    {"path": "/library/Erotica" if _in_docker else "/path/to/your/library/Erotica", "label": "Erotica", "rank": 2},
    {"path": "/library/E-Girls" if _in_docker else "/path/to/your/library/E-Girls", "label": "E-Girls", "rank": 3},
]


def get_conn() -> sqlite3.Connection:
    """Return a SQLite connection with per-connection PRAGMAs applied.
    `journal_mode=WAL` is sticky on the database file (set once in
    `init_db`), but `busy_timeout` and `synchronous` are per-connection
    and reset to defaults for every new connection. Without an explicit
    busy_timeout writes fail immediately on contention with
    `database is locked` — the symptom is random 500s and lost
    session-slide writes (which look like sporadic logouts because the
    expiry stops getting refreshed)."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10.0)
    conn.row_factory = sqlite3.Row
    # 10s busy_timeout: any writer that finds the lock held will retry
    # for up to 10s before raising. Plenty of headroom for transient
    # contention from the trickle workers / Watchdog / library indexer.
    conn.execute("PRAGMA busy_timeout=10000")
    # WAL durability sweet spot — fsyncs only at checkpoints, not on
    # every commit. Safe under WAL because the WAL itself is fsynced.
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _filename_stem_ext(name: str) -> tuple[str, str | None]:
    p = Path(name or "")
    stem = p.stem
    suf = p.suffix.lower().lstrip(".")
    return stem, (suf or None)


def _path_base_ext(path_str: str) -> tuple[str, str | None]:
    p = Path((path_str or "").strip())
    if not str(p):
        return "", None
    base = str(p.with_suffix(""))
    suf = p.suffix.lower().lstrip(".")
    return base, (suf or None)


# Same set as main.VIDEO_EXTENSIONS — try stored ext first, then these (re-encode .mp4 → .mkv).
_VIDEO_SUFFIXES_TRY: tuple[str, ...] = (
    ".mp4",
    ".mkv",
    ".m4v",
    ".avi",
    ".wmv",
    ".mov",
    ".flv",
    ".webm",
    ".ts",
    ".m2ts",
    ".mpg",
    ".mpeg",
)


def processed_files_find_on_disk_path(
    destination: str | None,
    destination_base: str | None,
    destination_ext: str | None,
) -> str | None:
    """
    Resolve a filed video on disk: exact destination path, or same folder + same basename
    with any supported video extension (handles re-encode mp4 → mkv, etc.).
    """
    dest = (destination or "").strip()
    if dest and Path(dest).is_file():
        return str(Path(dest).resolve())
    base = (destination_base or "").strip()
    if not base and dest:
        base = str(Path(dest).with_suffix(""))
    if not base:
        return None
    parent = Path(base).parent
    stem_name = Path(base).name
    stored = (destination_ext or "").strip().lower().lstrip(".")
    order: list[str] = []
    if stored:
        order.append("." + stored)
    for suf in _VIDEO_SUFFIXES_TRY:
        if suf not in order:
            order.append(suf)
    seen: set[str] = set()
    for suf in order:
        if suf in seen:
            continue
        seen.add(suf)
        cand = parent / (stem_name + suf)
        if cand.is_file():
            return str(cand.resolve())
    return None


def processed_files_repair_destination_row_if_found(row: dict) -> bool:
    """If the video exists (exact path or alternate extension), refresh destination* columns. Returns True if on disk."""
    resolved = processed_files_find_on_disk_path(
        row.get("destination"),
        row.get("destination_base"),
        row.get("destination_ext"),
    )
    if not resolved:
        return False
    rid = int(row["id"])
    prev = (row.get("destination") or "").strip()
    b, e = _path_base_ext(resolved)
    base_ok = (row.get("destination_base") or "").strip()
    if prev == resolved and base_ok:
        return True
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE processed_files
            SET destination = ?, destination_base = ?, destination_ext = ?
            WHERE id = ?
            """,
            (resolved, b, e, rid),
        )
        conn.commit()
    return True


def _backfill_processed_files_path_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT id, filename, destination FROM processed_files")
    for r in cur.fetchall():
        rid = int(r["id"])
        fn = r["filename"] or ""
        dst = (r["destination"] or "").strip()
        fst, fex = _filename_stem_ext(fn)
        conn.execute(
            "UPDATE processed_files SET filename_stem = ?, filename_ext = ? WHERE id = ?",
            (fst, fex, rid),
        )
        if dst:
            b, e = _path_base_ext(dst)
            conn.execute(
                "UPDATE processed_files SET destination_base = ?, destination_ext = ? WHERE id = ?",
                (b, e, rid),
            )


def _migrate_processed_filename_to_stem_core(
    conn: sqlite3.Connection,
    *,
    skip_if_done: bool = True,
    progress: Callable[[str, int, int, int], None] | None = None,
) -> dict:
    """
    Scene pipeline identity: normalize processed_files.filename to stem (no extension) where safe.

    Scope: this column only — not library_files, not files on disk. Purpose: lookups and
    upserts stay correct when the same logical download is re-muxed/re-encoded to another
    video extension in the source folder (same stem). Code matches rows using filename OR
    stem (get_phash, update_file, resolve_source_video_path). Filed library paths use
    destination_base + processed_files_find_on_disk_path (alternate extensions) separately.

    Skips rows where multiple processed_files share the same filename_stem (would be two
    different scenes); those keep a full basename key until disambiguated.

    progress(phase, current, total, updated) — phase is "scan" | "apply" | "done".
    Returns {"updated", "skipped_ambiguous", "already_done"}.
    """
    if skip_if_done:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("migration_processed_filename_stem_v1",),
        ).fetchone()
        if row and (row["value"] or "").strip().lower() == "done":
            if progress:
                progress("done", 0, 0, 0)
            return {"updated": 0, "skipped_ambiguous": 0, "already_done": True}

    cur = conn.execute(
        "SELECT id, filename, filename_stem, filename_ext FROM processed_files"
    )
    rows = cur.fetchall()
    total = len(rows)
    if progress:
        progress("scan", 0, total, 0)

    stems: dict[str, int] = {}
    for i, r in enumerate(rows):
        st = (r["filename_stem"] or "").strip()
        if st:
            stems[st] = stems.get(st, 0) + 1
        if progress and (total <= 1 or i == 0 or (i + 1) % max(1, total // 25) == 0 or i + 1 == total):
            progress("scan", i + 1, total, 0)

    updated = 0
    skipped_ambiguous = 0
    for i, r in enumerate(rows):
        rid = int(r["id"])
        fn = (r["filename"] or "").strip()
        st = (r["filename_stem"] or "").strip()
        fex = (r["filename_ext"] or "").strip().lower()
        if not st or not fn or fn == st:
            pass
        elif stems.get(st, 0) > 1:
            skipped_ambiguous += 1
        elif fex and fn.lower() == f"{st}.{fex}".lower():
            conn.execute(
                "UPDATE processed_files SET filename = ? WHERE id = ?",
                (st, rid),
            )
            updated += 1
        if progress and (total <= 1 or (i + 1) % max(1, total // 25) == 0 or i + 1 == total):
            progress("apply", i + 1, total, updated)

    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        ("migration_processed_filename_stem_v1", "done"),
    )
    if progress:
        progress("done", total, total, updated)
    return {
        "updated": updated,
        "skipped_ambiguous": skipped_ambiguous,
        "already_done": False,
    }


def _one_time_migrate_processed_filename_to_stem(conn: sqlite3.Connection) -> None:
    _migrate_processed_filename_to_stem_core(conn, skip_if_done=True, progress=None)


def resolve_source_video_path(source_dir: Path, db_filename: str) -> Path | None:
    """Resolve a processed_files.filename (full basename or stem) to a file under source_dir."""
    p = source_dir / db_filename
    if p.is_file():
        return p
    stem = Path(db_filename).stem
    for suf in _VIDEO_SUFFIXES_TRY:
        q = source_dir / (stem + suf)
        if q.is_file():
            return q
    return None


def init_db() -> None:
    with get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                filename      TEXT NOT NULL,
                phash         TEXT,
                match_source  TEXT,
                match_title   TEXT,
                match_studio  TEXT,
                match_date    TEXT,
                performers    TEXT,
                destination   TEXT,
                status        TEXT NOT NULL DEFAULT 'pending',
                error         TEXT,
                processed_at  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_filename ON processed_files(filename)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pf_status ON processed_files(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pf_processed_at ON processed_files(processed_at)")
        for col_sql in (
            "ALTER TABLE processed_files ADD COLUMN filename_stem TEXT",
            "ALTER TABLE processed_files ADD COLUMN filename_ext TEXT",
            "ALTER TABLE processed_files ADD COLUMN destination_base TEXT",
            "ALTER TABLE processed_files ADD COLUMN destination_ext TEXT",
            "ALTER TABLE processed_files ADD COLUMN match_external_id TEXT",
        ):
            try:
                conn.execute(col_sql)
            except sqlite3.OperationalError:
                pass
        _backfill_processed_files_path_columns(conn)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS prowlarr_indexers (
                id           INTEGER PRIMARY KEY,
                name         TEXT NOT NULL,
                protocol     TEXT NOT NULL,
                fetched_at   TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS auth (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                password_hash  TEXT,
                session_hours  INTEGER NOT NULL DEFAULT 24
            )
        """)
        # Ensure single auth row exists
        conn.execute("INSERT OR IGNORE INTO auth (id) VALUES (1)")
        conn.commit()

        # ── Scene-grab provenance ────────────────────────────────────
        # When a user grabs a release from /scenes (clicked a scene tile,
        # searched Prowlarr, sent a result to the download client), we
        # tag the release title with the originating scene's source DB +
        # ID so /downloads can later show the scene's poster on the
        # tile. Match keys are the release title (== job name on the
        # client) and the prowlarr GUID (secondary, since some clients
        # surface it). source_performers is JSON-encoded.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scene_grab_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                release_title       TEXT NOT NULL,
                guid                TEXT,
                download_url        TEXT,
                source_db           TEXT NOT NULL,
                source_id           TEXT NOT NULL,
                kind                TEXT NOT NULL DEFAULT 'scene',
                source_title        TEXT,
                source_studio       TEXT,
                source_performers   TEXT,
                source_poster_url   TEXT,
                source_date         TEXT,
                consumed_at         TEXT,
                created_at          TEXT NOT NULL
            )
        """)
        # download_url / kind / consumed_at / ts_uid added after the
        # initial table shipped — ALTER for any DB created before they
        # existed.
        # download_url: best cross-client match key (NZB/magnet URL).
        # kind: 'scene' vs 'movie', so the auto-file path can short
        #   circuit single-scene grabs without filing every track of a
        #   multi-scene movie release as the parent movie.
        # consumed_at: timestamp the row was used to auto-file, so the
        #   purge job can trim used rows aggressively while keeping
        #   pending ones around longer.
        # ts_uid: a short Top-Shelf-issued opaque id we inject into
        #   the job name as `[ts-XXXXXXXX]` at grab time. Survives
        #   client-side renames and unpack mangling, so it's the
        #   bulletproof match key when filename-based heuristics fail.
        for col, decl in (
            ("download_url", "TEXT"),
            ("kind",         "TEXT NOT NULL DEFAULT 'scene'"),
            ("consumed_at",  "TEXT"),
            ("ts_uid",       "TEXT"),
        ):
            try:
                conn.execute(f"ALTER TABLE scene_grab_log ADD COLUMN {col} {decl}")
            except sqlite3.OperationalError:
                pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scene_grab_release ON scene_grab_log(release_title COLLATE NOCASE)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scene_grab_guid ON scene_grab_log(guid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scene_grab_dlurl ON scene_grab_log(download_url)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scene_grab_uid ON scene_grab_log(ts_uid)")
        conn.commit()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_movies (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                filename      TEXT NOT NULL,
                tmdb_id       TEXT,
                tpdb_id       TEXT,
                match_source  TEXT,
                title         TEXT,
                year          TEXT,
                overview      TEXT,
                poster_url    TEXT,
                destination   TEXT,
                status        TEXT NOT NULL DEFAULT 'pending',
                error         TEXT,
                processed_at  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_filename ON processed_movies(filename)")
        try:
            conn.execute(
                "ALTER TABLE processed_movies ADD COLUMN tpdb_id TEXT"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE processed_movies ADD COLUMN match_source TEXT"
            )
        except sqlite3.OperationalError:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS directories (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                type  TEXT NOT NULL,
                path  TEXT NOT NULL,
                label TEXT NOT NULL,
                rank  INTEGER NOT NULL DEFAULT 0,
                gender_filters TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE directories ADD COLUMN gender_filters TEXT")
        except sqlite3.OperationalError:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS download_import_done (
                id           TEXT PRIMARY KEY,
                imported_at  TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS favourite_entities (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                kind             TEXT NOT NULL,
                folder_name      TEXT NOT NULL,
                path             TEXT NOT NULL UNIQUE,
                root_label       TEXT,
                is_favourite     INTEGER NOT NULL DEFAULT 0,
                image_url        TEXT,
                aliases_json     TEXT,
                match_tpdb_id    TEXT,
                match_tpdb_name  TEXT,
                match_stashdb_id TEXT,
                match_stashdb_name TEXT,
                match_fansdb_id  TEXT,
                match_fansdb_name TEXT,
                sort_birth_date    TEXT,
                gender_filters_json TEXT,
                matches_locked   INTEGER NOT NULL DEFAULT 0,
                scanned_at       TEXT,
                updated_at       TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_favourite_kind ON favourite_entities(kind)"
        )
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN sort_birth_date TEXT"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN gender_filters_json TEXT"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN matches_locked INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN path_missing INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN match_tmdb_id TEXT"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN match_tmdb_name TEXT"
            )
        except sqlite3.OperationalError:
            pass
        for _col in ("match_iafd_url", "match_freeones_url", "match_babepedia_url", "match_coomer_url"):
            try:
                conn.execute(f"ALTER TABLE favourite_entities ADD COLUMN {_col} TEXT")
            except sqlite3.OperationalError:
                pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN is_group INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN group_ids_json TEXT"
            )
        except sqlite3.OperationalError:
            pass
        # Manual per-performer weight override for the scene-filing router.
        # NULL = fall back to the default (2 if is_favourite, 1 otherwise).
        # Set values are clamped to 1..10 by the application layer.
        try:
            conn.execute(
                "ALTER TABLE favourite_entities ADD COLUMN weight INTEGER"
            )
        except sqlite3.OperationalError:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS library_files (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                filename_stem       TEXT NOT NULL,
                current_filename    TEXT NOT NULL,
                destination         TEXT NOT NULL UNIQUE,
                library_root        TEXT NOT NULL DEFAULT '',
                source_record_id    INTEGER,
                phash_1             TEXT,
                phash_2             TEXT,
                phash_3             TEXT,
                phash_3_scanned_at  TEXT,
                is_removed          INTEGER NOT NULL DEFAULT 0,
                removed_at          TEXT,
                created_at          TEXT,
                updated_at          TEXT,
                media_codec         TEXT,
                media_width         INTEGER,
                media_height        INTEGER,
                file_created_iso    TEXT,
                media_mtime         REAL,
                FOREIGN KEY (source_record_id) REFERENCES processed_files(id) ON DELETE SET NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_library_files_stem_root "
            "ON library_files(filename_stem, library_root)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_library_files_removed "
            "ON library_files(is_removed)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_library_files_lr_dest "
            "ON library_files(library_root, destination)"
        )
        for _alter in (
            "ALTER TABLE library_files ADD COLUMN media_codec TEXT",
            "ALTER TABLE library_files ADD COLUMN media_width INTEGER",
            "ALTER TABLE library_files ADD COLUMN media_height INTEGER",
            "ALTER TABLE library_files ADD COLUMN file_created_iso TEXT",
            "ALTER TABLE library_files ADD COLUMN media_mtime REAL",
        ):
            try:
                conn.execute(_alter)
            except sqlite3.OperationalError:
                pass
        try:
            conn.execute(
                "ALTER TABLE library_files ADD COLUMN duplicate_review_pending INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lf_dup_pending ON library_files(duplicate_review_pending)"
        )
        try:
            conn.execute(
                "ALTER TABLE library_files ADD COLUMN duplicate_ignored INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lf_dup_ignored ON library_files(duplicate_ignored)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                line       TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log(created_at)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_notifications (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                kind       TEXT NOT NULL DEFAULT 'info',
                message    TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_notifications_expires ON app_notifications(expires_at)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS spotlight_excluded (
                performer_id TEXT PRIMARY KEY,
                name         TEXT NOT NULL DEFAULT '',
                excluded_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spotlight_top_cache (
                performer_id TEXT PRIMARY KEY,
                name         TEXT NOT NULL DEFAULT '',
                data         TEXT NOT NULL,
                cached_at    TEXT NOT NULL
            )
        """)

        # Studio logos: deterministic studio name → on-disk PNG mapping. Rows
        # are populated by (a) the one-shot seed from the static archive,
        # (b) library / NFO scanning that adds new studio names found in
        # scene metadata, and (c) the background fetcher that downloads
        # missing logos from StashDB / TPDB / FansDB. The endpoint serves
        # files from `{DB parent}/metadata/studio_logos/{logo_path}`.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS studio_logos (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                slug              TEXT    NOT NULL UNIQUE,
                name              TEXT    NOT NULL DEFAULT '',
                logo_path         TEXT,
                source            TEXT    NOT NULL DEFAULT 'unknown',
                aliases_json      TEXT,
                last_fetched_at   TEXT,
                last_attempted_at TEXT,
                created_at        TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_studio_logos_path_null ON studio_logos(logo_path) WHERE logo_path IS NULL")
        try:
            conn.execute(
                "ALTER TABLE studio_logos ADD COLUMN fetch_attempts INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE studio_logos ADD COLUMN last_error TEXT"
            )
        except sqlite3.OperationalError:
            pass

        # Wanted items: user-starred scenes/movies they want acquired.
        # Surfaces in /downloads under a "Wanted" tab; auto-cleared when
        # the matching phash lands in the library.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wanted_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                kind           TEXT    NOT NULL,
                source         TEXT    NOT NULL,
                external_id    TEXT    NOT NULL,
                title          TEXT    NOT NULL DEFAULT '',
                studio         TEXT    NOT NULL DEFAULT '',
                release_date   TEXT    NOT NULL DEFAULT '',
                performers     TEXT    NOT NULL DEFAULT '',
                thumb_url      TEXT    NOT NULL DEFAULT '',
                description    TEXT    NOT NULL DEFAULT '',
                phash          TEXT,
                added_at       TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                acquired_at    TEXT,
                UNIQUE(kind, source, external_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wanted_active ON wanted_items(acquired_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wanted_phash ON wanted_items(phash) WHERE phash IS NOT NULL")
        # Additive columns added after initial schema ship. Wrapped in
        # try/except so second-boot migrations don't raise on existing DBs.
        for _col, _sql in [
            ("tags_json",      "ALTER TABLE wanted_items ADD COLUMN tags_json    TEXT NOT NULL DEFAULT '[]'"),
            ("duration",       "ALTER TABLE wanted_items ADD COLUMN duration     INTEGER NOT NULL DEFAULT 0"),
            ("source_url",     "ALTER TABLE wanted_items ADD COLUMN source_url   TEXT NOT NULL DEFAULT ''"),
            ("stashdb_id",     "ALTER TABLE wanted_items ADD COLUMN stashdb_id   TEXT NOT NULL DEFAULT ''"),
            ("stashdb_url",    "ALTER TABLE wanted_items ADD COLUMN stashdb_url  TEXT NOT NULL DEFAULT ''"),
            ("fansdb_id",      "ALTER TABLE wanted_items ADD COLUMN fansdb_id    TEXT NOT NULL DEFAULT ''"),
            ("fansdb_url",     "ALTER TABLE wanted_items ADD COLUMN fansdb_url   TEXT NOT NULL DEFAULT ''"),
            ("enriched_at",    "ALTER TABLE wanted_items ADD COLUMN enriched_at  TEXT"),
        ]:
            try:
                conn.execute(_sql)
            except sqlite3.OperationalError:
                pass

        # IAFD enrichment tables — persist scraped performer + film data
        # so the bulk matcher, library cards and background trickle worker
        # can all read from one durable source instead of each re-fetching
        # via cloudscraper. Keyed by the canonical IAFD URLs (/person.rme/id=…
        # and /title.rme/id=…) because those are the only stable identifiers
        # IAFD exposes. Scrape timestamps drive the background refresh
        # policy (oldest-first re-scrape, plus queue-jumpers).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iafd_performers (
                url           TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                last_scraped  TEXT,
                film_count    INTEGER NOT NULL DEFAULT 0,
                first_year    TEXT,
                last_year     TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iafd_perf_name ON iafd_performers(name COLLATE NOCASE)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iafd_perf_scraped ON iafd_performers(last_scraped)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS iafd_films (
                url           TEXT PRIMARY KEY,
                title         TEXT NOT NULL,
                year          TEXT,
                studio        TEXT,
                last_scraped  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iafd_film_title ON iafd_films(title COLLATE NOCASE)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iafd_film_year ON iafd_films(year)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS iafd_performer_films (
                performer_url TEXT NOT NULL,
                film_url      TEXT NOT NULL,
                PRIMARY KEY (performer_url, film_url)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iafd_pf_film ON iafd_performer_films(film_url)")

        # Per-scene cast is stored as a JSON array rather than an
        # exploded cast-per-row table because every read is all-scenes-
        # for-one-film (the bulk matcher and the confirm modal), and
        # JSON is a fine fit for that access pattern without needing
        # joins. If we ever want "which films featured these two
        # performers together" we can normalise later.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS iafd_film_scenes (
                film_url     TEXT NOT NULL,
                scene_number INTEGER NOT NULL,
                label        TEXT NOT NULL DEFAULT '',
                cast_json    TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (film_url, scene_number)
            )
        """)

        _one_time_migrate_processed_filename_to_stem(conn)
        conn.commit()

    # Seed defaults if empty
    with get_conn() as conn:
        for key, value in DEFAULTS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value)
            )
        row = conn.execute("SELECT COUNT(*) as c FROM directories").fetchone()
        if row["c"] == 0:
            for d in DEFAULT_PERFORMER_DIRS:
                conn.execute(
                    "INSERT INTO directories (type, path, label, rank, gender_filters) VALUES ('performer', ?, ?, ?, ?)",
                    (d["path"], d["label"], d["rank"], "[]"),
                )
        conn.commit()


# --- Settings ---

_settings_cache: dict | None = None
_settings_cache_ts: float = 0.0
_SETTINGS_CACHE_TTL = 5.0  # seconds


def get_settings() -> dict:
    global _settings_cache, _settings_cache_ts
    now = time.time()
    if _settings_cache is not None and (now - _settings_cache_ts) < _SETTINGS_CACHE_TTL:
        return _settings_cache
    with get_conn() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        _settings_cache = {r["key"]: r["value"] for r in rows}
        _settings_cache_ts = now
        return _settings_cache


def save_settings(data: dict) -> None:
    global _settings_cache, _settings_cache_ts
    with get_conn() as conn:
        for key, value in data.items():
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value))
            )
        conn.commit()
    _settings_cache = None
    _settings_cache_ts = 0.0


def spotlight_get_excluded_ids() -> set[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT performer_id FROM spotlight_excluded").fetchall()
        return {r["performer_id"] for r in rows}


def spotlight_exclude(performer_id: str, name: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO spotlight_excluded (performer_id, name, excluded_at) VALUES (?, ?, ?)",
            (performer_id, name, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def spotlight_load_top_cache(max_age_days: int = 30) -> list[dict]:
    """Return cached performers whose cached_at is within max_age_days."""
    import json as _json
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT data FROM spotlight_top_cache WHERE cached_at > ?",
            (cutoff,),
        ).fetchall()
    result = []
    for r in rows:
        try:
            result.append(_json.loads(r["data"]))
        except Exception:
            pass
    return result


def spotlight_save_top_cache(performers: list[dict]) -> None:
    """Persist the top 100 performers. Purges entries older than 30 days.
    Uses INSERT OR IGNORE so cached_at is never overwritten for existing entries,
    but updates name/data fields when a performer is already in the cache.
    """
    import json as _json
    now_iso = datetime.now(timezone.utc).isoformat()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    with get_conn() as conn:
        conn.execute("DELETE FROM spotlight_top_cache WHERE cached_at <= ?", (cutoff,))
        for p in performers[:100]:
            pid = p.get("id") or ""
            if not pid:
                continue
            conn.execute(
                """INSERT INTO spotlight_top_cache (performer_id, name, data, cached_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(performer_id) DO UPDATE SET name = excluded.name, data = excluded.data""",
                (pid, p.get("name") or "", _json.dumps(p), now_iso),
            )
        conn.commit()


# ── Studio logos ─────────────────────────────────────────────────────
# Lookup-by-slug for the /api/studio-logo endpoint, plus mutators used by
# the seed migration, library-scan auto-discover, and background fetcher.
# All callers convert their input through `studio_logo_slug()` first.

def studio_logo_get_by_slug(slug: str) -> dict | None:
    """Exact slug lookup. Returns the row dict or None."""
    if not slug:
        return None
    with get_conn() as conn:
        r = conn.execute(
            "SELECT * FROM studio_logos WHERE slug = ?", (slug,)
        ).fetchone()
        return dict(r) if r else None


def studio_logo_get_by_id(row_id: int) -> dict | None:
    """Primary-key lookup. Used by the manager endpoints so per-row
    delete / refetch doesn't need to re-scan the whole table."""
    try:
        rid = int(row_id)
    except (TypeError, ValueError):
        return None
    with get_conn() as conn:
        r = conn.execute(
            "SELECT * FROM studio_logos WHERE id = ?", (rid,)
        ).fetchone()
        return dict(r) if r else None


def studio_logo_list_all() -> list[dict]:
    """Every row, ordered by name. Used by the substring fallback path."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM studio_logos ORDER BY length(slug) DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def studio_logo_list_with_files() -> list[dict]:
    """Rows that have a logo file on disk — the "Logo library" listing.

    Used by the manager panel under /health → Studios so the user can
    browse every logo currently cached in metadata/studio_logos/,
    refetch stale ones, or delete entries they don't want. Ordered by
    name (COLLATE NOCASE) so the UI shows a predictable alphabetical
    list regardless of scrape order.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, slug, name, logo_path, source,
                      last_fetched_at, created_at
               FROM studio_logos
               WHERE logo_path IS NOT NULL
               ORDER BY LOWER(name), LOWER(slug)"""
        ).fetchall()
        return [dict(r) for r in rows]


STUDIO_LOGO_MAX_ATTEMPTS = 5


def studio_logo_list_pending(limit: int = 100, max_attempts: int = STUDIO_LOGO_MAX_ATTEMPTS) -> list[dict]:
    """Rows that don't have a logo file yet — fetcher targets these.

    Ordered by oldest last_attempted_at first (NULL counts as oldest) so
    a single pass can spread retries fairly across all unfetched studios.
    Rows whose ``fetch_attempts`` has hit ``max_attempts`` are parked in
    the dead-letter list (see ``studio_logo_list_unresolvable``) and
    excluded here so the fetcher stops wasting budget on them.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM studio_logos
               WHERE logo_path IS NULL
                 AND COALESCE(fetch_attempts, 0) < ?
               ORDER BY COALESCE(last_attempted_at, '') ASC
               LIMIT ?""",
            (int(max_attempts), int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]


def studio_logo_count_pending(max_attempts: int = STUDIO_LOGO_MAX_ATTEMPTS) -> int:
    """Count of rows still eligible for a fetcher pass."""
    with get_conn() as conn:
        r = conn.execute(
            """SELECT COUNT(*) AS n FROM studio_logos
               WHERE logo_path IS NULL
                 AND COALESCE(fetch_attempts, 0) < ?""",
            (int(max_attempts),),
        ).fetchone()
        return int(r["n"] or 0)


def studio_logo_list_unresolvable(min_attempts: int = STUDIO_LOGO_MAX_ATTEMPTS) -> list[dict]:
    """Dead-letter list: rows that have failed enough times to be parked.

    Used by the UI so the user can see exactly which studios StashDB
    couldn't resolve and either drop a PNG manually or delete the row.
    Ordered most-attempts-first, then by name so the list is stable.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, slug, name, fetch_attempts, last_error,
                      last_attempted_at, source
               FROM studio_logos
               WHERE logo_path IS NULL
                 AND COALESCE(fetch_attempts, 0) >= ?
               ORDER BY fetch_attempts DESC, name COLLATE NOCASE ASC""",
            (int(min_attempts),),
        ).fetchall()
        return [dict(r) for r in rows]


def studio_logo_mark_attempt_failed(row_id: int, error: str | None) -> None:
    """Bump fetch_attempts on a pending row and record the last error.

    Called from the fetcher whenever a row fails to produce a usable
    logo. Distinct from ``studio_logo_set_path(row_id, None)`` which was
    the previous (counterless) failure signal.
    """
    now = datetime.now(timezone.utc).isoformat()
    err = (error or "")[:400]
    with get_conn() as conn:
        conn.execute(
            """UPDATE studio_logos
               SET fetch_attempts = COALESCE(fetch_attempts, 0) + 1,
                   last_error = ?,
                   last_attempted_at = ?
               WHERE id = ?""",
            (err, now, int(row_id)),
        )
        conn.commit()


def studio_logo_reset_attempts(row_ids: list[int] | None = None) -> int:
    """Reset fetch_attempts + last_error so rows re-enter the pending queue.

    With no ids, resets every unresolvable row. Returns rows updated.
    """
    with get_conn() as conn:
        if row_ids:
            placeholders = ",".join("?" for _ in row_ids)
            cur = conn.execute(
                f"""UPDATE studio_logos
                    SET fetch_attempts = 0, last_error = NULL
                    WHERE id IN ({placeholders})""",
                tuple(int(i) for i in row_ids),
            )
        else:
            cur = conn.execute(
                """UPDATE studio_logos
                   SET fetch_attempts = 0, last_error = NULL
                   WHERE logo_path IS NULL
                     AND COALESCE(fetch_attempts, 0) > 0"""
            )
        conn.commit()
        return cur.rowcount or 0


def studio_logo_delete(row_ids: list[int]) -> int:
    """Remove rows from the pending/unresolvable list entirely.

    Used when the user confirms a studio isn't worth keeping (e.g. a
    junk name that slipped in from an NFO typo). Returns rows deleted.
    """
    if not row_ids:
        return 0
    with get_conn() as conn:
        placeholders = ",".join("?" for _ in row_ids)
        cur = conn.execute(
            f"DELETE FROM studio_logos WHERE id IN ({placeholders})",
            tuple(int(i) for i in row_ids),
        )
        conn.commit()
        return cur.rowcount or 0


# --- Wanted items ---------------------------------------------------
# Scenes or movies the user has flagged as "wanted" from /scenes. Surface
# under a Wanted tab in /downloads; auto-acquired when a matching phash
# is scanned into the library.


def wanted_add(
    kind: str,
    source: str,
    external_id: str,
    title: str = "",
    studio: str = "",
    release_date: str = "",
    performers: str = "",
    thumb_url: str = "",
    description: str = "",
    phash: str | None = None,
    tags: list[str] | None = None,
    duration: int | None = None,
    source_url: str = "",
) -> int:
    """Upsert a wanted item. Returns the row id. Resets ``acquired_at``
    so re-adding after auto-clear brings it back to the active list."""
    import json as _json_local
    k = (kind or "").strip().lower() or "scene"
    s = (source or "").strip().lower()
    eid = (external_id or "").strip()
    if not eid:
        raise ValueError("external_id required")
    tags_json = _json_local.dumps([str(t) for t in (tags or []) if t])
    dur = int(duration or 0)
    src_url = (source_url or "").strip()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO wanted_items
              (kind, source, external_id, title, studio, release_date,
               performers, thumb_url, description, phash,
               tags_json, duration, source_url,
               added_at, acquired_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
            ON CONFLICT(kind, source, external_id) DO UPDATE SET
              title        = excluded.title,
              studio       = excluded.studio,
              release_date = excluded.release_date,
              performers   = excluded.performers,
              thumb_url    = excluded.thumb_url,
              description  = excluded.description,
              phash        = COALESCE(excluded.phash, wanted_items.phash),
              tags_json    = excluded.tags_json,
              duration     = excluded.duration,
              source_url   = excluded.source_url,
              acquired_at  = NULL
            RETURNING id
            """,
            (k, s, eid, title or "", studio or "", release_date or "",
             performers or "", thumb_url or "", description or "",
             phash or None, tags_json, dur, src_url),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"]) if row else 0


def wanted_remove(row_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM wanted_items WHERE id = ?", (int(row_id),))
        conn.commit()
        return (cur.rowcount or 0) > 0


def wanted_remove_by_external(kind: str, source: str, external_id: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM wanted_items WHERE kind = ? AND source = ? AND external_id = ?",
            ((kind or "").strip().lower(), (source or "").strip().lower(), (external_id or "").strip()),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0


def wanted_list(active_only: bool = True) -> list[dict]:
    """Return wanted items as dicts, newest first."""
    sql = "SELECT * FROM wanted_items"
    if active_only:
        sql += " WHERE acquired_at IS NULL"
    sql += " ORDER BY added_at DESC, id DESC"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql).fetchall()]


def wanted_active_external_ids() -> list[dict]:
    """Lightweight ``{kind, source, external_id}`` tuples for active
    items — used to paint the eye-icon state on /scenes cards without
    shipping full thumb/description payloads."""
    with get_conn() as conn:
        return [
            {"kind": r["kind"], "source": r["source"], "external_id": r["external_id"]}
            for r in conn.execute(
                "SELECT kind, source, external_id FROM wanted_items WHERE acquired_at IS NULL"
            ).fetchall()
        ]


def wanted_mark_acquired(kind: str, source: str, external_id: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE wanted_items SET acquired_at = CURRENT_TIMESTAMP "
            "WHERE kind = ? AND source = ? AND external_id = ? AND acquired_at IS NULL",
            ((kind or "").strip().lower(), (source or "").strip().lower(), (external_id or "").strip()),
        )
        conn.commit()
        return cur.rowcount or 0


def wanted_mark_acquired_by_phash(phash: str) -> int:
    ph = (phash or "").strip().lower()
    if not ph:
        return 0
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE wanted_items SET acquired_at = CURRENT_TIMESTAMP "
            "WHERE LOWER(phash) = ? AND acquired_at IS NULL",
            (ph,),
        )
        conn.commit()
        return cur.rowcount or 0


def wanted_set_crossref(row_id: int, source: str, external_id: str, url: str) -> int:
    """Save a StashDB or FansDB cross-reference on an existing wanted row."""
    src = (source or "").strip().lower()
    col_id = {"stashdb": "stashdb_id", "fansdb": "fansdb_id"}.get(src)
    col_url = {"stashdb": "stashdb_url", "fansdb": "fansdb_url"}.get(src)
    if not col_id or not col_url:
        return 0
    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE wanted_items SET {col_id} = ?, {col_url} = ?, enriched_at = CURRENT_TIMESTAMP "
            f"WHERE id = ?",
            ((external_id or "").strip(), (url or "").strip(), int(row_id)),
        )
        conn.commit()
        return cur.rowcount or 0


def wanted_mark_enriched(row_id: int) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE wanted_items SET enriched_at = CURRENT_TIMESTAMP WHERE id = ?",
            (int(row_id),),
        )
        conn.commit()
        return cur.rowcount or 0


def wanted_set_phash(kind: str, source: str, external_id: str, phash: str) -> int:
    """Stash the computed phash onto an existing wanted row so future
    phash-only scans (e.g. when the file arrives under a different
    filename) can still auto-clear by phash."""
    ph = (phash or "").strip().lower()
    if not ph:
        return 0
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE wanted_items SET phash = ? "
            "WHERE kind = ? AND source = ? AND external_id = ? AND acquired_at IS NULL "
            "  AND (phash IS NULL OR phash = '')",
            (ph, (kind or "").strip().lower(), (source or "").strip().lower(), (external_id or "").strip()),
        )
        conn.commit()
        return cur.rowcount or 0


def studio_logo_count(only_with_file: bool = False) -> int:
    with get_conn() as conn:
        if only_with_file:
            r = conn.execute(
                "SELECT COUNT(*) AS n FROM studio_logos WHERE logo_path IS NOT NULL"
            ).fetchone()
        else:
            r = conn.execute("SELECT COUNT(*) AS n FROM studio_logos").fetchone()
        return int(r["n"] or 0)


def studio_logo_upsert(
    name: str,
    *,
    logo_path: str | None = None,
    source: str | None = None,
) -> int | None:
    """Insert or update a studio_logos row keyed on the slug derived from name.

    Returns the row id, or None if the name was empty / could not be slugged.
    `logo_path` and `source` are only updated when explicitly provided so the
    library-scan auto-discover path (which only knows the name) doesn't clobber
    a previously-fetched file.
    """
    slug = studio_logo_slug(name)
    if not slug:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, logo_path, source FROM studio_logos WHERE slug = ?",
            (slug,),
        ).fetchone()
        if cur is None:
            cur2 = conn.execute(
                """INSERT INTO studio_logos (slug, name, logo_path, source, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (slug, name, logo_path, source or "unknown",
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            return int(cur2.lastrowid)
        # Existing row: only update when the caller explicitly passed a value.
        sets, args = [], []
        if logo_path is not None:
            sets.append("logo_path = ?"); args.append(logo_path)
        if source is not None:
            sets.append("source = ?"); args.append(source)
        if sets:
            args.append(int(cur["id"]))
            conn.execute(
                f"UPDATE studio_logos SET {', '.join(sets)} WHERE id = ?",
                args,
            )
            conn.commit()
        return int(cur["id"])


def studio_logo_set_path(row_id: int, logo_path: str | None, source: str | None = None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        if logo_path is None:
            conn.execute(
                """UPDATE studio_logos
                   SET last_attempted_at = ?
                   WHERE id = ?""",
                (now, int(row_id)),
            )
        else:
            # Success — clear any prior failure counters so a row that
            # recovered (alias added, PNG dropped manually, etc.) isn't
            # permanently stuck in the dead-letter list on next resync.
            conn.execute(
                """UPDATE studio_logos
                   SET logo_path = ?, source = COALESCE(?, source),
                       last_fetched_at = ?, last_attempted_at = ?,
                       fetch_attempts = 0, last_error = NULL
                   WHERE id = ?""",
                (logo_path, source, now, now, int(row_id)),
            )
        conn.commit()


def mark_download_import_done(client_item_id: str) -> None:
    """Record that a client job was cleared after download watch processing (dedup for remove-from-client)."""
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO download_import_done (id, imported_at) VALUES (?, ?)",
            (client_item_id, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def was_download_import_done(client_item_id: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM download_import_done WHERE id = ? LIMIT 1",
            (client_item_id,),
        ).fetchone()
        return row is not None


# --- Directories ---

def _directory_row_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    raw = d.get("gender_filters")
    if isinstance(raw, str) and raw.strip():
        try:
            d["gender_filters"] = json.loads(raw)
        except json.JSONDecodeError:
            d["gender_filters"] = []
    else:
        d["gender_filters"] = []
    return d


def get_directories(dir_type: str = None) -> list[dict]:
    with get_conn() as conn:
        if dir_type:
            rows = conn.execute(
                "SELECT * FROM directories WHERE type = ? ORDER BY rank ASC",
                (dir_type,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM directories ORDER BY type, rank ASC"
            ).fetchall()
        return [_directory_row_dict(r) for r in rows]


def save_directories(dirs: list[dict]) -> None:
    """Replace all directory entries with the provided list."""
    with get_conn() as conn:
        conn.execute("DELETE FROM directories")
        for d in dirs:
            gf_json = "[]"
            if d.get("type") == "performer":
                raw = d.get("gender_filters")
                if isinstance(raw, list):
                    gf_json = json.dumps(raw)
                elif isinstance(raw, str) and raw.strip():
                    gf_json = raw.strip()
            conn.execute(
                "INSERT INTO directories (type, path, label, rank, gender_filters) VALUES (?, ?, ?, ?, ?)",
                (d["type"], d["path"], d["label"], int(d["rank"]), gf_json),
            )
        conn.commit()


# --- Favourites index (library performer / studio folders) ---
#
# Rows cache folder path + matched IDs per source (TPDB / StashDB / FansDB) so the UI
# can cross-reference the same entity across databases.


def favourite_list() -> list[dict]:
    """Library index rows (performers, studios, movies): alphabetical by folder name within each kind."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM favourite_entities
            ORDER BY kind, folder_name COLLATE NOCASE
            """
        )
        return [dict(r) for r in cur.fetchall()]


def favourite_get(row_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM favourite_entities WHERE id = ?", (row_id,)
        ).fetchone()
        return dict(row) if row else None


def favourite_get_by_path(path_str: str) -> dict | None:
    """Return the favourites index row for a library folder path (exact or resolved match)."""
    raw = (path_str or "").strip()
    if not raw:
        return None
    try:
        target = Path(raw).expanduser().resolve()
    except OSError:
        target = Path(raw).expanduser()
    target_s = str(target)
    raw_norm = raw.rstrip("/")
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM favourite_entities")
        for r in cur.fetchall():
            d = dict(r)
            p = (d.get("path") or "").strip()
            if not p:
                continue
            try:
                pr = Path(p).expanduser().resolve()
            except OSError:
                pr = Path(p).expanduser()
            ps = str(pr)
            if (
                p == raw
                or p == path_str
                or ps == target_s
                or ps.rstrip("/") == raw_norm
                or p.rstrip("/") == raw_norm
            ):
                return d
    return None


def favourite_delete(row_id: int) -> bool:
    """Remove a favourites index row. Returns True if a row was deleted."""
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM favourite_entities WHERE id = ?", (row_id,))
        conn.commit()
        return (cur.rowcount or 0) > 0


def favourite_refresh_all_path_existence() -> None:
    """Set path_missing from ``Path(path).is_dir()`` for every row. Ignores matches_locked."""
    now = datetime.now(timezone.utc).isoformat()
    rows = favourite_list()
    with get_conn() as conn:
        for r in rows:
            pth = (r.get("path") or "").strip()
            ok = False
            if pth:
                try:
                    ok = Path(pth).expanduser().is_dir()
                except OSError:
                    ok = False
            missing = 0 if ok else 1
            conn.execute(
                "UPDATE favourite_entities SET path_missing = ?, updated_at = ? WHERE id = ?",
                (missing, now, r["id"]),
            )
        conn.commit()


def favourite_upsert_folder(
    kind: str,
    folder_name: str,
    path: str,
    root_label: str | None,
    gender_filters_json: str | None = None,
) -> int:
    """Insert or update folder row (preserves matches, star, image on re-scan); return row id."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO favourite_entities (
                kind, folder_name, path, root_label,
                is_favourite, sort_birth_date, gender_filters_json,
                scanned_at, updated_at, path_missing
            ) VALUES (?, ?, ?, ?, 0, NULL, ?, ?, ?, 0)
            ON CONFLICT(path) DO UPDATE SET
                kind = excluded.kind,
                folder_name = excluded.folder_name,
                root_label = excluded.root_label,
                gender_filters_json = excluded.gender_filters_json,
                scanned_at = excluded.scanned_at,
                updated_at = excluded.updated_at,
                path_missing = 0
            """,
            (kind, folder_name, path, root_label, gender_filters_json, now, now),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM favourite_entities WHERE path = ?", (path,)).fetchone()
        return int(row["id"]) if row else 0


def favourite_update_matches(
    row_id: int,
    *,
    image_url: str | None = None,
    aliases_json: str | None = None,
    match_tmdb_id: str | None = None,
    match_tmdb_name: str | None = None,
    match_tpdb_id: str | None = None,
    match_tpdb_name: str | None = None,
    match_stashdb_id: str | None = None,
    match_stashdb_name: str | None = None,
    match_fansdb_id: str | None = None,
    match_fansdb_name: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    fields: list[tuple[str, str | None]] = [("updated_at", now)]
    for k, v in [
        ("image_url", image_url),
        ("aliases_json", aliases_json),
        ("match_tmdb_id", match_tmdb_id),
        ("match_tmdb_name", match_tmdb_name),
        ("match_tpdb_id", match_tpdb_id),
        ("match_tpdb_name", match_tpdb_name),
        ("match_stashdb_id", match_stashdb_id),
        ("match_stashdb_name", match_stashdb_name),
        ("match_fansdb_id", match_fansdb_id),
        ("match_fansdb_name", match_fansdb_name),
    ]:
        if v is not None:
            fields.append((k, v))
    set_clause = ", ".join(f"{k} = ?" for k, _ in fields)
    vals = [v for _, v in fields] + [row_id]
    with get_conn() as conn:
        conn.execute(
            f"UPDATE favourite_entities SET {set_clause} WHERE id = ?",
            vals,
        )
        conn.commit()


def favourite_clear_image_url(row_id: int) -> None:
    """Explicitly set image_url to NULL for a performer row."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET image_url = NULL, updated_at = ? WHERE id = ?",
            (now, row_id),
        )
        conn.commit()


def favourite_overwrite_matches(
    row_id: int,
    *,
    image_url: str | None,
    aliases_json: str | None,
    match_tmdb_id: str | None,
    match_tmdb_name: str | None,
    match_tpdb_id: str | None,
    match_tpdb_name: str | None,
    match_stashdb_id: str | None,
    match_stashdb_name: str | None,
    match_fansdb_id: str | None,
    match_fansdb_name: str | None,
    sort_birth_date: str | None,
) -> None:
    """Set all match-related columns, including NULL (clears stale wrong guesses on refresh)."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE favourite_entities SET
                image_url = ?,
                aliases_json = ?,
                match_tmdb_id = ?,
                match_tmdb_name = ?,
                match_tpdb_id = ?,
                match_tpdb_name = ?,
                match_stashdb_id = ?,
                match_stashdb_name = ?,
                match_fansdb_id = ?,
                match_fansdb_name = ?,
                sort_birth_date = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                image_url,
                aliases_json,
                match_tmdb_id,
                match_tmdb_name,
                match_tpdb_id,
                match_tpdb_name,
                match_stashdb_id,
                match_stashdb_name,
                match_fansdb_id,
                match_fansdb_name,
                sort_birth_date,
                now,
                row_id,
            ),
        )
        conn.commit()


def favourite_set_sort_birth_date(row_id: int, sort_birth_date: str | None) -> None:
    """Set ISO sort key (YYYY-MM-DD) for favourites ordering; None clears."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET sort_birth_date = ?, updated_at = ? WHERE id = ?",
            (sort_birth_date, now, row_id),
        )
        conn.commit()


def favourite_set_aliases_json(row_id: int, aliases_json: str | None) -> None:
    """Replace just the ``aliases_json`` column on a favourite row.

    Used by the manual alias editor on /health so users can tie
    individual performer names (``Silvia Dellai``) to a group folder
    (``Dellai Twins``) for the headshot + RSS matchers. The wider
    ``favourite_overwrite_matches`` helper would clear every other
    match column, which we don't want when the user is only editing
    aliases.
    """
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET aliases_json = ?, updated_at = ? WHERE id = ?",
            (aliases_json, now, row_id),
        )
        conn.commit()


def favourite_set_star(row_id: int, is_favourite: bool) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET is_favourite = ?, updated_at = ? WHERE id = ?",
            (1 if is_favourite else 0, now, row_id),
        )
        conn.commit()


def favourite_set_weight(row_id: int, weight: int | None) -> None:
    """Manual router weight for the scene-filing performer picker.

    `weight` must be an int 1..10 or None. None clears the override so the
    default kicks back in (2 if is_favourite, 1 otherwise). Anything out of
    range is rejected with ValueError — callers are expected to validate at
    the API layer before hitting this.
    """
    if weight is not None:
        if not isinstance(weight, int) or weight < 1 or weight > 10:
            raise ValueError("weight must be an integer 1..10 or None")
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET weight = ?, updated_at = ? WHERE id = ?",
            (weight, now, row_id),
        )
        conn.commit()


def favourite_set_is_group(row_id: int, is_group: bool) -> None:
    """Toggle the group flag on a favourites row. When un-toggling, group_ids_json is cleared."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        if is_group:
            conn.execute(
                "UPDATE favourite_entities SET is_group = 1, updated_at = ? WHERE id = ?",
                (now, row_id),
            )
        else:
            conn.execute(
                "UPDATE favourite_entities SET is_group = 0, group_ids_json = NULL, updated_at = ? WHERE id = ?",
                (now, row_id),
            )
        conn.commit()


def _group_ids_load(row_id: int) -> dict:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT group_ids_json FROM favourite_entities WHERE id = ?",
            (row_id,),
        ).fetchone()
    if not row or not row["group_ids_json"]:
        return {"tpdb": [], "stashdb": [], "fansdb": []}
    try:
        data = json.loads(row["group_ids_json"]) or {}
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "tpdb": [str(x) for x in (data.get("tpdb") or [])],
        "stashdb": [str(x) for x in (data.get("stashdb") or [])],
        "fansdb": [str(x) for x in (data.get("fansdb") or [])],
    }


def favourite_group_add_id(row_id: int, source: str, ext_id: str) -> dict:
    """Append an additional crosswalk ID to a group folder. Returns the resulting group dict."""
    src = (source or "").strip().lower()
    if src not in ("tpdb", "stashdb", "fansdb"):
        raise ValueError(f"Unknown source: {source}")
    ext = (ext_id or "").strip()
    if not ext:
        raise ValueError("Empty id")
    data = _group_ids_load(row_id)
    if ext not in data[src]:
        data[src].append(ext)
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET is_group = 1, group_ids_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(data), now, row_id),
        )
        conn.commit()
    return data


def favourite_group_remove_id(row_id: int, source: str, ext_id: str) -> dict:
    """Remove a single ID from a group folder. Returns the resulting group dict."""
    src = (source or "").strip().lower()
    if src not in ("tpdb", "stashdb", "fansdb"):
        raise ValueError(f"Unknown source: {source}")
    ext = (ext_id or "").strip()
    data = _group_ids_load(row_id)
    data[src] = [x for x in data[src] if x != ext]
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET group_ids_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(data), now, row_id),
        )
        conn.commit()
    return data


def favourite_set_matches_locked(row_id: int, locked: bool) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET matches_locked = ?, updated_at = ? WHERE id = ?",
            (1 if locked else 0, now, row_id),
        )
        conn.commit()


def favourite_set_all_matches_locked(locked: bool) -> int:
    """Set matches_locked for every favourites row; returns row count updated."""
    now = datetime.now(timezone.utc).isoformat()
    val = 1 if locked else 0
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE favourite_entities SET matches_locked = ?, updated_at = ?",
            (val, now),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def favourite_clear_all_match_ids(row_id: int) -> None:
    """Clear external match columns and sort key; keep image and aliases."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE favourite_entities SET
                match_tmdb_id = NULL, match_tmdb_name = NULL,
                match_tpdb_id = NULL, match_tpdb_name = NULL,
                match_stashdb_id = NULL, match_stashdb_name = NULL,
                match_fansdb_id = NULL, match_fansdb_name = NULL,
                sort_birth_date = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, row_id),
        )
        conn.commit()


def favourite_clear_source_match(row_id: int, source: str) -> None:
    """Clear a single source's match fields. source: TMDB, TPDB, STASHDB, FANSDB, IAFD, FREEONES, or BABEPEDIA."""
    u = (source or "").strip().upper()
    if u == "TMDB":
        clause = "match_tmdb_id = NULL, match_tmdb_name = NULL"
    elif u == "TPDB":
        clause = "match_tpdb_id = NULL, match_tpdb_name = NULL"
    elif u in ("STASHDB", "STASH"):
        clause = "match_stashdb_id = NULL, match_stashdb_name = NULL"
    elif u in ("FANSDB", "FANS"):
        clause = "match_fansdb_id = NULL, match_fansdb_name = NULL"
    elif u == "IAFD":
        clause = "match_iafd_url = NULL"
    elif u == "FREEONES":
        clause = "match_freeones_url = NULL"
    elif u == "BABEPEDIA":
        clause = "match_babepedia_url = NULL"
    elif u == "COOMER":
        clause = "match_coomer_url = NULL"
    else:
        raise ValueError("source must be TMDB, TPDB, StashDB, FansDB, IAFD, Freeones, Babepedia, or Coomer")
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            f"UPDATE favourite_entities SET {clause}, updated_at = ? WHERE id = ?",
            (now, row_id),
        )
        conn.commit()


def favourite_set_ext_link(row_id: int, site: str, url: str) -> None:
    """Store a confirmed external profile URL/ID for a performer.
    site: tmdb, iafd, freeones, or babepedia.
    For TMDB, url may be a full person URL (https://www.themoviedb.org/person/123) or a bare numeric ID.
    """
    import re as _re
    s = (site or "").strip().lower()
    if s == "tmdb":
        m = _re.search(r'/person/(\d+)', url)
        store_val = m.group(1) if m else (url.strip() or None)
        col = "match_tmdb_id"
    else:
        col_map = {
            "iafd": "match_iafd_url",
            "freeones": "match_freeones_url",
            "babepedia": "match_babepedia_url",
            "coomer": "match_coomer_url",
        }
        col = col_map.get(s)
        if not col:
            raise ValueError("site must be tmdb, iafd, freeones, babepedia, or coomer")
        store_val = url.strip() or None
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            f"UPDATE favourite_entities SET {col} = ?, updated_at = ? WHERE id = ?",
            (store_val, now, row_id),
        )
        conn.commit()


def favourite_studios_with_tpdb_ids() -> list[dict]:
    """Studio rows in the Favourites index with a TPDB site id (for scenes feed)."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT kind, folder_name, match_tpdb_id
            FROM favourite_entities
            WHERE kind = 'studio'
              AND match_tpdb_id IS NOT NULL
              AND trim(COALESCE(match_tpdb_id, '')) != ''
            """
        )
        return [dict(r) for r in cur.fetchall()]


def favourite_starred_with_tpdb_ids() -> list[dict]:
    """Heart-starred performer/studio rows with a TPDB id (for scenes feed)."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT kind, folder_name, match_tpdb_id
            FROM favourite_entities
            WHERE COALESCE(is_favourite, 0) = 1
              AND match_tpdb_id IS NOT NULL
              AND trim(COALESCE(match_tpdb_id, '')) != ''
              AND kind IN ('performer', 'studio')
            """
        )
        return [dict(r) for r in cur.fetchall()]


def favourite_delete_missing_paths(valid_paths: set[str]) -> int:
    """Remove index rows whose folders no longer exist (optional cleanup)."""
    with get_conn() as conn:
        rows = conn.execute("SELECT id, path FROM favourite_entities").fetchall()
        removed = 0
        for r in rows:
            if r["path"] not in valid_paths:
                conn.execute("DELETE FROM favourite_entities WHERE id = ?", (r["id"],))
                removed += 1
        conn.commit()
        return removed


def favourite_find_performer_folder_by_crosswalk(
    tpdb_id: str | None,
    stash_id: str | None,
    fans_id: str | None,
) -> dict | None:
    """Resolve library folder via entity index (Favourites DB table) by TPDB / StashDB / FansDB performer id.

    Also matches group folders (is_group=1) where the requested ID appears in
    group_ids_json under its source key.
    """
    tid = (tpdb_id or "").strip()
    sid = (stash_id or "").strip()
    fid = (fans_id or "").strip()
    if not (tid or sid or fid):
        return None
    parts: list[str] = []
    args: list[str] = []
    if tid:
        parts.append("trim(COALESCE(match_tpdb_id,'')) = ?")
        args.append(tid)
    if sid:
        parts.append("trim(COALESCE(match_stashdb_id,'')) = ?")
        args.append(sid)
    if fid:
        parts.append("trim(COALESCE(match_fansdb_id,'')) = ?")
        args.append(fid)
    if not parts:
        return None
    where = " OR ".join(parts)
    with get_conn() as conn:
        # Pull the weighting fields alongside path/folder_name so the
        # scene-filing router can score candidates without a second
        # query per performer.
        row = conn.execute(
            f"SELECT id, path, folder_name, is_favourite, weight FROM favourite_entities "
            f"WHERE kind = 'performer' AND ({where}) LIMIT 1",
            args,
        ).fetchone()
        if row:
            return dict(row)
        # Fallback: group folders with additional linked IDs in group_ids_json
        group_rows = conn.execute(
            "SELECT id, path, folder_name, is_favourite, weight, group_ids_json "
            "FROM favourite_entities "
            "WHERE kind = 'performer' AND is_group = 1 AND group_ids_json IS NOT NULL AND group_ids_json != ''"
        ).fetchall()
        for gr in group_rows:
            try:
                data = json.loads(gr["group_ids_json"] or "{}") or {}
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            tpdb_list = [str(x).strip() for x in (data.get("tpdb") or []) if str(x).strip()]
            stash_list = [str(x).strip() for x in (data.get("stashdb") or []) if str(x).strip()]
            fans_list = [str(x).strip() for x in (data.get("fansdb") or []) if str(x).strip()]
            if (tid and tid in tpdb_list) or (sid and sid in stash_list) or (fid and fid in fans_list):
                return {
                    "id": gr["id"],
                    "path": gr["path"],
                    "folder_name": gr["folder_name"],
                    "is_favourite": gr["is_favourite"],
                    "weight": gr["weight"],
                }
        return None


def favourite_find_studio_folder_by_crosswalk(
    tpdb_id: str | None,
    stash_id: str | None,
    fans_id: str | None,
) -> dict | None:
    """Resolve Series library folder via entity index by site/studio ids."""
    tid = (tpdb_id or "").strip()
    sid = (stash_id or "").strip()
    fid = (fans_id or "").strip()
    if not (tid or sid or fid):
        return None
    parts: list[str] = []
    args: list[str] = []
    if tid:
        parts.append("trim(COALESCE(match_tpdb_id,'')) = ?")
        args.append(tid)
    if sid:
        parts.append("trim(COALESCE(match_stashdb_id,'')) = ?")
        args.append(sid)
    if fid:
        parts.append("trim(COALESCE(match_fansdb_id,'')) = ?")
        args.append(fid)
    if not parts:
        return None
    where = " OR ".join(parts)
    with get_conn() as conn:
        row = conn.execute(
            f"SELECT path, folder_name FROM favourite_entities WHERE kind = 'studio' AND ({where}) LIMIT 1",
            args,
        ).fetchone()
        return dict(row) if row else None


# --- Processed files ---

def upsert_file(filename: str) -> int:
    fst, fex = _filename_stem_ext(filename)
    with get_conn() as conn:
        # 1) Prefer exact filename match (current behavior target).
        cur = conn.execute(
            "SELECT id, filename FROM processed_files WHERE filename = ? LIMIT 1",
            (filename,),
        )
        row = cur.fetchone()
        # 2) Legacy fallback: older rows may have filename saved as stem only.
        if not row:
            cur = conn.execute(
                """
                SELECT id, filename
                FROM processed_files
                WHERE filename = ?
                LIMIT 1
                """,
                (fst,),
            )
            row = cur.fetchone()
        if row:
            rid = int(row["id"])
            stored = (row["filename"] or "").strip()
            if stored != filename:
                conn.execute(
                    """
                    UPDATE processed_files
                    SET filename = ?, filename_stem = ?, filename_ext = ?
                    WHERE id = ?
                    """,
                    (filename, fst, fex, rid),
                )
                conn.commit()
            return rid
        cur = conn.execute(
            """
            INSERT INTO processed_files (filename, filename_stem, filename_ext, status)
            VALUES (?, ?, ?, 'pending')
            """,
            (filename, fst, fex),
        )
        conn.commit()
        return cur.lastrowid


def get_phash(filename: str) -> str | None:
    """Return cached phash for a filename, or None if not yet computed."""
    fst, _ = _filename_stem_ext(filename)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT phash FROM processed_files
            WHERE (filename = ? OR filename = ?) AND phash IS NOT NULL
            """,
            (filename, fst),
        ).fetchone()
        return row["phash"] if row else None


def update_file(filename, status, phash=None, match_source=None, match_title=None,
                match_studio=None, match_date=None, performers=None,
                destination=None, error=None, match_external_id=None):
    fst, _ = _filename_stem_ext(filename)
    fields = {"status": status, "processed_at": datetime.now().isoformat(timespec="seconds")}
    for k, v in [("phash", phash), ("match_source", match_source),
                 ("match_title", match_title), ("match_studio", match_studio),
                 ("match_date", match_date), ("performers", performers),
                 ("destination", destination), ("error", error),
                 ("match_external_id", match_external_id)]:
        if v is not None:
            fields[k] = v
    if destination is not None:
        d = (destination or "").strip()
        if d:
            b, e = _path_base_ext(d)
            fields["destination_base"] = b
            fields["destination_ext"] = e
        else:
            fields["destination_base"] = None
            fields["destination_ext"] = None
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values())
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM processed_files WHERE filename = ? OR filename = ? LIMIT 1",
            (filename, fst),
        ).fetchone()
        if not row:
            return
        conn.execute(
            f"UPDATE processed_files SET {set_clause} WHERE id = ?",
            values + [int(row["id"])],
        )
        conn.commit()


def get_history(limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM processed_files WHERE status != 'pending' ORDER BY processed_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Movie history
# ---------------------------------------------------------------------------

def upsert_movie(filename: str) -> int:
    with get_conn() as conn:
        cur = conn.execute("SELECT id FROM processed_movies WHERE filename = ?", (filename,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur = conn.execute(
            "INSERT INTO processed_movies (filename, status) VALUES (?, 'pending')", (filename,)
        )
        conn.commit()
        return cur.lastrowid


def update_movie(filename: str, status: str, tmdb_id: str = None, tpdb_id: str = None,
                 match_source: str = None, title: str = None,
                 year: str = None, overview: str = None, poster_url: str = None,
                 destination: str = None, error: str = None) -> None:
    fields = {"status": status, "processed_at": datetime.now().isoformat(timespec="seconds")}
    for k, v in [
        ("tmdb_id", tmdb_id),
        ("tpdb_id", tpdb_id),
        ("match_source", match_source),
        ("title", title),
        ("year", year),
        ("overview", overview),
        ("poster_url", poster_url),
        ("destination", destination),
        ("error", error),
    ]:
        if v is not None:
            fields[k] = v
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [filename]
    with get_conn() as conn:
        conn.execute(f"UPDATE processed_movies SET {set_clause} WHERE filename = ?", values)
        conn.commit()


def get_movie_history(limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM processed_movies WHERE status != 'pending' ORDER BY processed_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]


def get_movie_by_filename(filename: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM processed_movies WHERE filename = ?", (filename,)
        ).fetchone()
        return dict(row) if row else None


def get_movie_rows_map() -> dict[str, dict]:
    """All processed_movies rows keyed by filename (for queue UI)."""
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM processed_movies")
        return {r["filename"]: dict(r) for r in cur.fetchall()}


def get_movie_terminal_filenames() -> set[str]:
    """Filenames that are done (filed or skipped) — do not auto-queue again."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT filename FROM processed_movies WHERE status IN ('filed', 'skipped')"
        )
        return {r["filename"] for r in cur.fetchall()}


def get_movie_stats() -> dict:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='filed'     THEN 1 ELSE 0 END) AS filed,
                SUM(CASE WHEN status='unmatched' THEN 1 ELSE 0 END) AS unmatched,
                SUM(CASE WHEN status='error'     THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN status='skipped'   THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN status='no_dir'    THEN 1 ELSE 0 END) AS no_dir,
                SUM(CASE WHEN status='removed'   THEN 1 ELSE 0 END) AS removed
            FROM processed_movies WHERE status != 'pending' AND status != 'removed'
        """)
        row = dict(cur.fetchone())
    return {k: 0 if row[k] is None else row[k] for k in row}


def reconcile_movies_source(source_dir) -> int:
    """
    Walk processed_movies rows whose source file should still be present
    (anything that isn't already terminal) and flip to 'removed' when
    the file is gone from the movies source dir. Returns the number of
    rows updated. Prevents the stats counter from counting orphans.
    """
    from pathlib import Path as _Path
    try:
        base = _Path(str(source_dir)) if source_dir else None
    except Exception:
        return 0
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT filename FROM processed_movies
            WHERE status NOT IN ('filed','removed','pending')
        """)
        candidates = [r["filename"] for r in cur.fetchall()]
        updated = 0
        for fname in candidates:
            exists = False
            if base is not None and base.exists():
                try:
                    exists = (base / fname).exists()
                except OSError:
                    exists = False
            if not exists:
                conn.execute(
                    "UPDATE processed_movies SET status = 'removed', processed_at = ? WHERE filename = ?",
                    (datetime.now().isoformat(timespec="seconds"), fname),
                )
                updated += 1
        if updated:
            conn.commit()
    return updated


def save_setting(key: str, value: str) -> None:
    global _settings_cache, _settings_cache_ts
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
    _settings_cache = None
    _settings_cache_ts = 0.0


def activity_log_append(line: str) -> None:
    if line is None:
        return
    s = str(line)
    if not s.strip():
        return
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO activity_log (created_at, line) VALUES (?, ?)",
            (now, s),
        )
        conn.commit()


def activity_log_fetch(*, limit: int = 5000, offset: int = 0) -> list[dict]:
    lim = max(1, min(int(limit), 50_000))
    off = max(0, int(offset))
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, created_at, line FROM activity_log
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (lim, off),
        )
        rows = [dict(r) for r in cur.fetchall()]
    rows.reverse()
    return rows


def activity_log_count() -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM activity_log").fetchone()
        return int(row["cnt"]) if row else 0


def activity_log_clear() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM activity_log")
        conn.commit()


def activity_log_prune(retention_days: int) -> int:
    """Delete log lines older than retention_days. Returns rows removed."""
    d = int(retention_days)
    if d <= 0:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=d)).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM activity_log WHERE created_at < ?", (cutoff,))
        conn.commit()
        return int(cur.rowcount or 0)


def notification_add(kind: str, message: str, ttl_seconds: int = 300) -> int | None:
    """Ephemeral UI notification (header strip). Returns new id or None."""
    msg = (message or "").strip()
    if not msg:
        return None
    k = (kind or "info").strip()[:32] or "info"
    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=max(30, int(ttl_seconds)))
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO app_notifications (created_at, kind, message, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                now.isoformat(timespec="seconds"),
                k,
                msg[:2000],
                exp.isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid) if cur.lastrowid else None


def notifications_fetch_active(*, limit: int = 10) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lim = max(1, min(int(limit), 50))
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, created_at, kind, message, expires_at
            FROM app_notifications
            WHERE expires_at > ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (now, lim),
        )
        return [dict(r) for r in cur.fetchall()]


def notifications_delete_expired() -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM app_notifications WHERE expires_at < ?", (now,))
        conn.commit()
        return int(cur.rowcount or 0)


def notification_dismiss(notification_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM app_notifications WHERE id = ?",
            (int(notification_id),),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0


def get_retry_files() -> list[str]:
    """Return filenames of all non-filed, non-pending, non-removed records."""
    with get_conn() as conn:
        cur = conn.execute(
            """SELECT filename FROM processed_files
               WHERE status IN ('unmatched', 'no_dir', 'error')
               ORDER BY processed_at ASC"""
        )
        return [r["filename"] for r in cur.fetchall()]


VALID_SORT_COLS = {"processed_at", "match_studio", "match_date", "filename", "match_title", "performers"}
VALID_SORT_DIRS = {"ASC", "DESC"}

def get_history_paged(status: str = None, page: int = 1, per_page: int = 20,
                      sort_by: str = "processed_at", sort_dir: str = "DESC",
                      filter_text: str = None) -> dict:
    sort_by  = sort_by  if sort_by  in VALID_SORT_COLS else "processed_at"
    sort_dir = sort_dir if sort_dir in VALID_SORT_DIRS else "DESC"
    order    = f"{sort_by} {sort_dir}"
    offset   = (page - 1) * per_page

    # Build WHERE clause
    conditions = ["status != 'pending'"]
    params = []
    if status:
        conditions = [f"status = ?"]
        params.append(status)
    if filter_text:
        like = f"%{filter_text}%"
        conditions.append(
            "(filename LIKE ? OR match_title LIKE ? OR match_studio LIKE ? OR performers LIKE ?)"
        )
        params.extend([like, like, like, like])

    where = "WHERE " + " AND ".join(conditions)

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM processed_files {where}", params
        ).fetchone()["c"]
        cur = conn.execute(
            f"SELECT * FROM processed_files {where} ORDER BY {order} LIMIT ? OFFSET ?",
            params + [per_page, offset]
        )
        rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        fn = (row.get("filename") or "").strip()
        st = (row.get("filename_stem") or "").strip()
        row["display_name"] = st or (Path(fn).stem if fn else "")
    return {
        "rows":     rows,
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, -(-total // per_page)),
        "sort_by":  sort_by,
        "sort_dir": sort_dir,
    }


def get_stats() -> dict:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='filed'     THEN 1 ELSE 0 END) AS filed,
                SUM(CASE WHEN status='unmatched' THEN 1 ELSE 0 END) AS unmatched,
                SUM(CASE WHEN status='error'     THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN status='no_dir'    THEN 1 ELSE 0 END) AS no_dir,
                SUM(CASE WHEN status='removed'    THEN 1 ELSE 0 END) AS removed
            FROM processed_files WHERE status != 'pending'
        """)
        return dict(cur.fetchone())


# ---------------------------------------------------------------------------
# Library files (health / phash corpus)
# ---------------------------------------------------------------------------

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def library_file_dict(row: sqlite3.Row | dict) -> dict:
    return dict(row)


def get_processed_file_id_by_filename(filename: str) -> int | None:
    fst, _ = _filename_stem_ext(filename)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM processed_files WHERE filename = ? OR filename = ? LIMIT 1",
            (filename, fst),
        ).fetchone()
        return int(row["id"]) if row else None


def processed_file_delete_by_id(row_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM processed_files WHERE id = ?", (int(row_id),))
        conn.commit()
        return (cur.rowcount or 0) > 0


def processed_file_get_by_id(row_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM processed_files WHERE id = ?",
            (int(row_id),),
        ).fetchone()
        return dict(row) if row else None


def processed_file_update_library_destination(
    row_id: int,
    *,
    destination: str,
    filename: str,
) -> None:
    """Update filed destination and filename basenames after an on-disk rename."""
    dst = (destination or "").strip()
    fn = (filename or "").strip()
    if not dst or not fn:
        return
    b, ext = _path_base_ext(dst)
    fst, fext = _filename_stem_ext(fn)
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE processed_files SET
                destination = ?,
                filename = ?,
                filename_stem = ?,
                filename_ext = ?,
                destination_base = ?,
                destination_ext = ?,
                processed_at = ?
            WHERE id = ?
            """,
            (
                dst,
                fn,
                fst,
                fext,
                b,
                ext,
                now,
                int(row_id),
            ),
        )
        conn.commit()


def processed_files_delete_by_ids(row_ids: list[int]) -> int:
    """Delete processed_files rows by primary key; returns number of rows removed."""
    ids = [int(x) for x in row_ids if x is not None]
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    with get_conn() as conn:
        cur = conn.execute(
            f"DELETE FROM processed_files WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        return int(cur.rowcount or 0)


def processed_files_delete_removed_status() -> int:
    """Delete all processed_files rows with status 'removed'. Returns number deleted."""
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM processed_files WHERE status = 'removed'")
        conn.commit()
        return int(cur.rowcount or 0)


def processed_files_filed_rows() -> list[dict]:
    """Filing history rows that reference a library destination (for orphan detection)."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, filename, destination, destination_base, destination_ext,
                   filename_stem, filename_ext
            FROM processed_files
            WHERE status = 'filed'
              AND destination IS NOT NULL
              AND TRIM(destination) != ''
            ORDER BY id
            """
        )
        return [dict(r) for r in cur.fetchall()]


_RELEASE_TAG_RE = re.compile(
    r"\b("
    r"xxx|web.?dl|web.?rip|webrip|webhd|web|hdtv|proper|repack|internal|real|"
    r"2160p|1080p|720p|540p|480p|360p|4k|uhd|sd|hdr|hdr10|dv|"
    r"bluray|bdrip|brrip|dvdrip|hdrip|cam|ts|tc|"
    r"h\.?264|h\.?265|x264|x265|hevc|avc|xvid|divx|"
    r"aac|aac2|ac3|dts|ddp|ddp5|eac3|mp3|flac|opus|"
    r"mp4|mkv|avi|wmv|m4v|mov|flv"
    r")\b",
    re.IGNORECASE,
)

# Known file extensions we strip during release-name normalisation. Kept
# explicit (rather than accepting any 1-5 char alnum suffix) because a
# permissive extension-strip wipes out legitimate trailing title words:
# e.g. `BrazzersExxtra.Ana.Khalifa.And.Alana.Rose.Sluts.Up` would lose
# the final `Up` token, breaking every multi-token performer match that
# depended on the full tail.
_RELEASE_KNOWN_EXTENSIONS = {
    # video
    "mp4", "mkv", "avi", "wmv", "m4v", "mov", "flv", "mpg", "mpeg", "ts", "m2ts",
    "webm", "vob", "ogv",
    # download-client containers / sidecars
    "nzb", "torrent", "magnet", "par2",
    # archives
    "zip", "rar", "7z", "tar", "gz", "bz2",
    # art / metadata
    "jpg", "jpeg", "png", "webp", "gif", "nfo", "srt", "sub", "ass", "vtt",
}


def _library_guess_from_release_name(
    release_name: str,
    studio_index: list[tuple[str, str]] | None = None,
    performer_index: list[tuple[tuple[str, ...], str, int]] | None = None,
) -> dict:
    """Best-effort studio + performer match from a release name alone.

    Used for downloads that haven't been filed yet (and therefore have no
    `processed_files` row to read studio/performers off).

    Studios: matched by alnum prefix against the studio_logos slug (same
    approach as the filing pipeline — studio is typically the first
    word in the release name).

    Performers: matched by requiring each performer's name tokens (or
    alias tokens, or canonical-DB-name tokens) to appear as a
    *contiguous subsequence* of the release name's tokens. A release
    that mentions "Livi.Blossom" matches only the library row whose
    folder_name is "Livi Blossom" or whose aliases_json contains
    "Livi Blossom" — not every row with a "Blossom" token.

    Per-row dedup: a single library row is returned at most once in
    the result even if both its folder_name AND an alias match the
    same release. Token-range overlap also blocks the same position
    being claimed by a shorter performer once a longer one has
    already locked it in (the longest-first sort tries the most
    specific name first).

    studio_index / performer_index are optional pre-built caches so
    callers enriching many items in one pass only pay the DB read once.
    """
    empty = {"studio": None, "performers": []}
    if not release_name:
        return empty
    release_key = _normalize_release_for_match(release_name)
    if not release_key or len(release_key) < 8:
        return empty
    try:
        if studio_index is None or performer_index is None:
            studio_index, performer_index = _build_library_match_indexes()
    except Exception:
        return empty
    studio = None
    best_studio_len = 0
    for slug_key, display in studio_index:
        if len(slug_key) < 4:
            continue
        if release_key.startswith(slug_key) and len(slug_key) > best_studio_len:
            studio = display
            best_studio_len = len(slug_key)

    release_tokens = _tokenize_for_match(release_name)
    perfs: list[str] = []
    if release_tokens and performer_index:
        # Track token ranges already claimed by a matched performer so
        # a shorter name can't re-claim a position inside a longer
        # matched one ("Emma Rosie" wins over "Emma" at the same pair).
        used: list[tuple[int, int]] = []
        # Track which library rows have already contributed a match so
        # a single row can't surface twice via folder_name + alias both
        # hitting the title. Without this, a folder named
        # "Livi Blossom" that also has "Livi Blossom" as a scraped
        # alias would show up as a duplicate headshot tile.
        matched_row_ids: set[int] = set()

        def _overlaps(a: int, b: int) -> bool:
            return any(not (b < s or a > e) for s, e in used)

        for ptokens, display, row_id in performer_index:
            if row_id in matched_row_ids:
                continue
            n = len(ptokens)
            if n == 0 or n > len(release_tokens):
                continue
            for i in range(len(release_tokens) - n + 1):
                if release_tokens[i:i + n] == list(ptokens):
                    if not _overlaps(i, i + n - 1):
                        used.append((i, i + n - 1))
                        matched_row_ids.add(row_id)
                        perfs.append(display)
                    break
    return {"studio": studio, "performers": perfs}


def _build_library_match_indexes() -> tuple[list[tuple[str, str]], list[tuple[tuple[str, ...], str, int]]]:
    """Build indexes for studio (alnum prefix) and performer (token
    subsequence) matching over downloads lacking a filed record.

    Returns ``(studio_idx, performer_idx)``:
      - studio_idx: list of ``(alnum_slug, display_name)`` sorted longest-first
      - performer_idx: list of ``(name_tokens, display_name, row_id)``
        sorted by token count desc, then total char count desc. Each
        library row contributes one entry per name surface:
          * folder_name (always)
          * canonical DB primary names (match_tpdb_name /
            match_stashdb_name / match_fansdb_name) when set

    Notes on alias handling: we deliberately do NOT pull entries from
    ``aliases_json``. That column is populated by the scraper with the
    alternate-name lists exposed by TPDB / StashDB / FansDB, which are
    too noisy for release-name matching (first-name-only entries, stage
    names shared across multiple performers, etc.). Only names the
    external DBs flag as the *primary* performer name — already stored
    in the ``match_*_name`` columns — are trusted here.
    """
    alnum = re.compile(r"[^a-z0-9]")
    studio_idx: list[tuple[str, str]] = []
    # `perf_idx_raw` carries the tier as a 4th element so the sort can
    # prefer folder_name surfaces over canonical surfaces; the final
    # returned `perf_idx` drops tier after sorting.
    perf_idx_raw: list[tuple[tuple[str, ...], str, int, int]] = []
    with get_conn() as conn:
        for r in conn.execute(
            "SELECT name, slug FROM studio_logos WHERE logo_path IS NOT NULL"
        ).fetchall():
            slug = (r["slug"] or "").lower()
            key = alnum.sub("", slug)
            name = r["name"] or r["slug"]
            if key and len(key) >= 4:
                studio_idx.append((key, name))
        for r in conn.execute(
            "SELECT id, folder_name, "
            "match_tpdb_name, match_stashdb_name, match_fansdb_name "
            "FROM favourite_entities WHERE kind='performer'"
        ).fetchall():
            folder_name = (r["folder_name"] or "").strip()
            if not folder_name:
                continue
            row_id = int(r["id"])
            seen_token_tuples: set[tuple[str, ...]] = set()

            def _push(name_str: str, *, tier: int) -> None:
                s = (name_str or "").strip()
                if not s:
                    return
                tokens = tuple(_tokenize_for_match(s))
                if not tokens:
                    return
                # Min char-length stays at 5 so pathological 1-2 letter
                # tokens (stray initials) don't match.
                total_chars = sum(len(t) for t in tokens)
                if total_chars < 5:
                    return
                if tokens in seen_token_tuples:
                    return
                seen_token_tuples.add(tokens)
                perf_idx_raw.append((tokens, folder_name, row_id, tier))

            # Folder name — primary surface (tier 0).
            _push(folder_name, tier=0)
            # Canonical DB primary names (tier 1).
            for field in ("match_tpdb_name", "match_stashdb_name", "match_fansdb_name"):
                _push((r[field] or "").strip(), tier=1)
    studio_idx.sort(key=lambda x: -len(x[0]))
    # Sort precedence (most important first):
    #   1. Token count desc — "emma rosie" (2) tried before "emma" (1)
    #   2. Tier asc         — folder_name (0) before canonical (1)
    #                         so two rows with identical tokens prefer
    #                         the one where those tokens are the actual
    #                         folder name, not just a DB primary on
    #                         someone else
    #   3. Total char count desc — more specific name wins on tie
    perf_idx = [
        (tokens, display, row_id)
        for tokens, display, row_id, tier
        in sorted(
            perf_idx_raw,
            key=lambda x: (-len(x[0]), x[3], -sum(len(t) for t in x[0])),
        )
    ]
    return studio_idx, perf_idx


def _normalize_release_for_match(name: str) -> str:
    """
    Collapse a release/filename to an alnum-only key suitable for fuzzy matching
    between download-client display names (e.g. 'Deeplush.26.04.15.Isabella.XXX.1080p.MP4-WRB')
    and Top-Shelf's filed filenames (e.g. 'deeplush.26.04.15.isabella.mp4').

    Strips: extension, trailing release group (`-GROUP`), quality/codec/container
    tags, and all non-alnum characters. Lowercased.
    """
    if not name:
        return ""
    s = name.strip()
    # strip extension (only when the tail is a known extension — see
    # _RELEASE_KNOWN_EXTENSIONS for why a permissive 1–5 char alnum
    # strip silently truncated legitimate trailing title words).
    if "." in s:
        stem, ext = s.rsplit(".", 1)
        if ext.lower() in _RELEASE_KNOWN_EXTENSIONS:
            s = stem
    # strip trailing release group (-GROUPNAME)
    s = re.sub(r"-[a-z0-9]{1,20}$", "", s, flags=re.IGNORECASE)
    # strip quality / codec / container / audio tags
    s = _RELEASE_TAG_RE.sub(" ", s)
    # alnum only
    return re.sub(r"[^a-z0-9]", "", s.lower())


# Splits on non-alnum boundaries PLUS camelCase / digit↔letter transitions so
# "BrazzersExxtra.Ana.Khalifa.And.Alana.Rose" and
# "BrazzersExxtraAnaKhalifaAndAlanaRose" both tokenize to the same list.
# Kept anchored via lookarounds so splits don't consume characters.
_TOKEN_SPLIT_RE = re.compile(
    r"[^A-Za-z0-9]+"
    r"|(?<=[a-z])(?=[A-Z])"
    r"|(?<=[A-Z])(?=[A-Z][a-z])"
    r"|(?<=[A-Za-z])(?=\d)"
    r"|(?<=\d)(?=[A-Z])"
)


def _tokenize_for_match(name: str) -> list[str]:
    """Lowercase alnum tokens with dot/camelCase/digit-boundary awareness.

    Used by performer matching so "Ana Rose" does NOT match "Alana Rose"
    (the alnum-only substring approach does, because `anarose` is inside
    `alanarose`). Strips extension and trailing release group tags the
    same way `_normalize_release_for_match` does so tokenisation is
    consistent with the rest of the pipeline.
    """
    if not name:
        return []
    s = name.strip()
    if "." in s:
        stem, ext = s.rsplit(".", 1)
        if 1 <= len(ext) <= 5 and ext.isalnum():
            s = stem
    s = re.sub(r"-[a-z0-9]{1,20}$", "", s, flags=re.IGNORECASE)
    s = _RELEASE_TAG_RE.sub(" ", s)
    parts = _TOKEN_SPLIT_RE.split(s)
    return [p.lower() for p in parts if p]


def enrich_download_items_with_filing(items: list[dict]) -> None:
    """
    Augment download client rows with ``filing``: whether the item name matches a
    ``filed`` processed_files or processed_movies row (same basename / stem heuristics
    as the rest of the app). Used by the Downloads page to show when a client item
    corresponds to work already completed in Top-Shelf.
    """
    if not items:
        return

    keys: set[str] = set()
    for it in items:
        n = (it.get("name") or "").strip()
        if not n:
            continue
        p = Path(n)
        keys.add(p.stem.lower())
        keys.add(p.name.lower())
        keys.add(n.lower())
    if not keys:
        for it in items:
            it["filing"] = {"filed": False}
        return

    keys_list = list(keys)
    scene_map: dict[str, dict] = {}
    movie_map: dict[str, dict] = {}

    def _index_filed_row(store: dict[str, dict], row: dict, kind: str) -> None:
        pa = row.get("processed_at") or ""
        fn = (row.get("filename") or "").strip()
        stem_raw = (row.get("filename_stem") or "").strip()
        stem = stem_raw.lower() if stem_raw else (Path(fn).stem.lower() if fn else "")
        tagged = {**dict(row), "_kind": kind}
        keyset = {stem, fn.lower()}
        if fn:
            keyset.add(Path(fn).name.lower())
        for key in keyset:
            if not key:
                continue
            prev = store.get(key)
            if prev is None or pa > (prev.get("processed_at") or ""):
                store[key] = tagged

    batch = 500
    with get_conn() as conn:
        pf_cols = {r[1] for r in conn.execute("PRAGMA table_info(processed_files)").fetchall()}
        pf_has_stem = "filename_stem" in pf_cols
        for i in range(0, len(keys_list), batch):
            chunk = keys_list[i : i + batch]
            ph = ",".join("?" * len(chunk))
            if pf_has_stem:
                cur = conn.execute(
                    f"""
                    SELECT filename, filename_stem, processed_at, destination, status,
                           match_studio, performers
                    FROM processed_files
                    WHERE status = 'filed'
                      AND (
                        lower(filename) IN ({ph})
                        OR lower(ifnull(filename_stem, '')) IN ({ph})
                      )
                    """,
                    chunk + chunk,
                )
            else:
                cur = conn.execute(
                    f"""
                    SELECT filename, processed_at, destination, status,
                           match_studio, performers
                    FROM processed_files
                    WHERE status = 'filed'
                      AND lower(filename) IN ({ph})
                    """,
                    chunk,
                )
            for r in cur.fetchall():
                row = dict(r)
                if "filename_stem" not in row:
                    row["filename_stem"] = Path(row.get("filename") or "").stem
                _index_filed_row(scene_map, row, "scene")

            cur = conn.execute(
                f"""
                SELECT filename, processed_at, destination, status
                FROM processed_movies
                WHERE status = 'filed'
                  AND lower(filename) IN ({ph})
                """,
                chunk,
            )
            for r in cur.fetchall():
                row = dict(r)
                row["filename_stem"] = Path(row.get("filename") or "").stem
                _index_filed_row(movie_map, row, "movie")

    def _best_match(name: str, store: dict[str, dict]) -> dict | None:
        n = name.strip()
        if not n:
            return None
        p = Path(n)
        candidates: list[dict] = []
        for k in (p.stem.lower(), p.name.lower(), n.lower()):
            if k in store:
                candidates.append(store[k])
        if not candidates:
            return None
        return max(candidates, key=lambda r: (r.get("processed_at") or "", r.get("filename") or ""))

    # Fuzzy fallback index: alnum-only normalized key → best filed row.
    # Release names on download clients typically carry extra tags the filed
    # file doesn't (.XXX.1080p.MP4-GRP), so exact-stem lookups never match.
    # We load this lazily — only when the exact matcher misses for at least
    # one item — so clean Top-Shelf libraries don't pay the scan cost.
    scene_norm: dict[str, dict] | None = None
    movie_norm: dict[str, dict] | None = None

    def _ensure_normalized_indexes() -> None:
        nonlocal scene_norm, movie_norm
        if scene_norm is not None:
            return
        scene_norm = {}
        movie_norm = {}
        try:
            with get_conn() as conn:
                for r in conn.execute(
                    "SELECT filename, filename_stem, processed_at, destination, "
                    "match_studio, performers FROM processed_files WHERE status='filed'"
                ).fetchall():
                    row = dict(r)
                    stem = (row.get("filename_stem") or
                            Path(row.get("filename") or "").stem)
                    key = _normalize_release_for_match(stem)
                    if not key or len(key) < 8:
                        continue
                    row["_kind"] = "scene"
                    prev = scene_norm.get(key)
                    if prev is None or (row.get("processed_at") or "") > (prev.get("processed_at") or ""):
                        scene_norm[key] = row
                for r in conn.execute(
                    "SELECT filename, processed_at, destination FROM processed_movies WHERE status='filed'"
                ).fetchall():
                    row = dict(r)
                    stem = Path(row.get("filename") or "").stem
                    key = _normalize_release_for_match(stem)
                    if not key or len(key) < 8:
                        continue
                    row["_kind"] = "movie"
                    prev = movie_norm.get(key)
                    if prev is None or (row.get("processed_at") or "") > (prev.get("processed_at") or ""):
                        movie_norm[key] = row
        except Exception:
            pass

    def _normalized_match(name: str, store: dict[str, dict] | None) -> dict | None:
        if not store:
            return None
        key = _normalize_release_for_match(name)
        if not key or len(key) < 8:
            return None
        # Direct hit
        if key in store:
            return store[key]
        # Prefix match either way — the release name usually has the filed
        # stem as a prefix (extra tags appended), but occasionally the filed
        # file is the longer side (trailing descriptor).
        for k, v in store.items():
            if key.startswith(k) or k.startswith(key):
                # Require the shorter side to be at least 70% of the longer
                # to avoid matching one studio's short slug to another.
                shorter = min(len(k), len(key))
                longer = max(len(k), len(key))
                if longer > 0 and shorter / longer >= 0.7:
                    return v
        return None

    # Lazy caches for the library-name fallback (studios + performers).
    lib_studio_idx: list[tuple[str, str]] | None = None
    lib_perf_idx: list[tuple[tuple[str, ...], str, int]] | None = None

    def _library_guess(name: str) -> dict:
        nonlocal lib_studio_idx, lib_perf_idx
        if lib_studio_idx is None or lib_perf_idx is None:
            try:
                lib_studio_idx, lib_perf_idx = _build_library_match_indexes()
            except Exception:
                lib_studio_idx, lib_perf_idx = [], []
        return _library_guess_from_release_name(name, lib_studio_idx, lib_perf_idx)

    for it in items:
        n = (it.get("name") or "").strip()
        if not n:
            it["filing"] = {"filed": False}
            continue
        sr = _best_match(n, scene_map)
        mr = _best_match(n, movie_map)
        match_source = "exact" if (sr or mr) else None
        if sr is None and mr is None:
            _ensure_normalized_indexes()
            sr = _normalized_match(n, scene_norm)
            mr = _normalized_match(n, movie_norm)
            if sr or mr:
                match_source = "normalized_prefix"
        if sr is None and mr is None:
            # No filed row — fall back to library name-match so unfiled
            # downloads still get studio logos + performer headshots on
            # the progress bar.
            guess = _library_guess(n)
            it["filing"] = {
                "filed": False,
                "studio": guess["studio"],
                "performers": guess["performers"],
                "match_source": "library_guess",
            }
            continue
        if sr is None:
            best = mr
        elif mr is None:
            best = sr
        else:
            pa_s = sr.get("processed_at") or ""
            pa_m = mr.get("processed_at") or ""
            best = mr if pa_m > pa_s else sr
        dest = (best.get("destination") or "").strip()
        studio = (best.get("match_studio") or "").strip()
        perfs_raw = (best.get("performers") or "").strip()
        perfs: list[str] = []
        if perfs_raw:
            # Stored as comma-separated; tolerate ` / ` and `|` as alt separators.
            tmp = perfs_raw.replace("|", ",").replace(" / ", ",")
            perfs = [p.strip() for p in tmp.split(",") if p.strip()]
        # Validate each filed performer actually appears in the current
        # download's name — filters out stale performer names that got
        # locked into `processed_files.performers` historically (e.g. an
        # older filing auto-matched the wrong person, or a fuzzy-prefix
        # hit pulled in the previous scene's cast). We keep the filed
        # row for `destination` / `processed_at` / `filed` flag but
        # prune the displayed performer list so the bar shows only
        # people the release actually names. Falls back to the raw list
        # when the release name is too cryptic to contain any of the
        # performer names — avoids blanking the cast on edge cases like
        # release-titles that carry only a scene code.
        if perfs:
            release_tokens = _tokenize_for_match(n)
            if release_tokens:
                def _perf_in_release(perf_name: str) -> bool:
                    tk = _tokenize_for_match(perf_name)
                    if not tk or len(tk) > len(release_tokens):
                        return False
                    for i in range(len(release_tokens) - len(tk) + 1):
                        if release_tokens[i:i + len(tk)] == tk:
                            return True
                    return False
                validated = [p for p in perfs if _perf_in_release(p)]
                if validated:
                    perfs = validated
        it["filing"] = {
            "filed": True,
            "processed_at": best.get("processed_at"),
            "kind": best.get("_kind"),
            "destination": dest if dest else None,
            "studio": studio or None,
            "performers": perfs,
            "match_source": match_source,
            # Preserve the backing row's filename so the UI can show WHICH
            # filed record is being matched against — essential for
            # diagnosing stale / cross-matched fuzzy hits.
            "matched_filename": (best.get("filename") or "").strip() or None,
        }


def library_file_get_by_destination(destination: str) -> dict | None:
    dst = (destination or "").strip()
    if not dst:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM library_files WHERE destination = ? LIMIT 1",
            (dst,),
        ).fetchone()
        return dict(row) if row else None


def library_file_find_by_stem_root(stem: str, library_root: str) -> dict | None:
    """Prefer a non-removed row; if multiple, most recently updated."""
    st = (stem or "").strip()
    if not st:
        return None
    lr = (library_root or "").strip()
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM library_files
            WHERE filename_stem = ? AND library_root = ? AND is_removed = 0
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (st, lr),
        ).fetchone()
        if row:
            return dict(row)
        row = conn.execute(
            """
            SELECT * FROM library_files
            WHERE filename_stem = ? AND library_root = ?
            ORDER BY is_removed ASC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (st, lr),
        ).fetchone()
        return dict(row) if row else None


def library_file_upsert_from_pipeline(
    *,
    destination: str,
    current_filename: str,
    filename_stem: str,
    library_root: str,
    source_record_id: int | None,
    phash_1: str | None,
) -> int:
    """Insert or update row when a scene is filed; sets phash_1 once if new."""
    now = _iso_now()
    dst = (destination or "").strip()
    cur_fn = (current_filename or "").strip()
    stem = (filename_stem or "").strip()
    lr = (library_root or "").strip()
    if not (dst and cur_fn and stem):
        return 0
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, phash_1 FROM library_files WHERE destination = ?",
            (dst,),
        ).fetchone()
        if row:
            pid = int(row["id"])
            sets = [
                "current_filename = ?",
                "library_root = ?",
                "updated_at = ?",
            ]
            vals: list = [cur_fn, lr, now]
            if source_record_id is not None:
                sets.append("source_record_id = ?")
                vals.append(source_record_id)
            if phash_1 and not (row["phash_1"] or "").strip():
                sets.append("phash_1 = ?")
                vals.append(phash_1)
            vals.append(pid)
            conn.execute(
                f"UPDATE library_files SET {', '.join(sets)} WHERE id = ?",
                vals,
            )
            conn.commit()
            return pid
        conn.execute(
            """
            INSERT INTO library_files (
                filename_stem, current_filename, destination, library_root,
                source_record_id, phash_1, phash_2, phash_3,
                phash_3_scanned_at, is_removed, removed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, ?, ?)
            """,
            (
                stem,
                cur_fn,
                dst,
                lr,
                source_record_id,
                (phash_1 or "").strip() or None,
                now,
                now,
            ),
        )
        conn.commit()
        return int(conn.execute("SELECT last_insert_rowid() AS x").fetchone()["x"])


def library_file_upsert_from_movie_filing(
    *,
    destination: str,
    current_filename: str,
    filename_stem: str,
    library_root: str,
    phash_1: str | None,
) -> int:
    """Movie pipeline: no processed_files FK."""
    return library_file_upsert_from_pipeline(
        destination=destination,
        current_filename=current_filename,
        filename_stem=filename_stem,
        library_root=library_root,
        source_record_id=None,
        phash_1=phash_1,
    )


def library_file_insert_index_row(
    *,
    destination: str,
    current_filename: str,
    filename_stem: str,
    library_root: str,
    phash_2: str,
    source_record_id: int | None,
) -> int:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO library_files (
                filename_stem, current_filename, destination, library_root,
                source_record_id, phash_1, phash_2, phash_3,
                phash_3_scanned_at, is_removed, removed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, 0, NULL, ?, ?)
            """,
            (
                filename_stem,
                current_filename,
                destination,
                library_root,
                source_record_id,
                phash_2,
                now,
                now,
            ),
        )
        conn.commit()
        return int(conn.execute("SELECT last_insert_rowid() AS x").fetchone()["x"])


def library_file_insert_index_stub(
    *,
    destination: str,
    current_filename: str,
    filename_stem: str,
    library_root: str,
    source_record_id: int | None,
) -> int:
    """Insert a library_files row before phash/ffprobe (phash_2 NULL). Index job fills media next."""
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO library_files (
                filename_stem, current_filename, destination, library_root,
                source_record_id, phash_1, phash_2, phash_3,
                phash_3_scanned_at, is_removed, removed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, NULL, ?, ?)
            """,
            (
                filename_stem,
                current_filename,
                destination,
                library_root,
                source_record_id,
                now,
                now,
            ),
        )
        conn.commit()
        return int(conn.execute("SELECT last_insert_rowid() AS x").fetchone()["x"])


def library_file_set_phash2(row_id: int, phash_2: str) -> None:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET phash_2 = ?, updated_at = ?
            WHERE id = ? AND (phash_2 IS NULL OR TRIM(COALESCE(phash_2, '')) = '')
            """,
            (phash_2, now, int(row_id)),
        )
        conn.commit()


def library_file_set_phash3(row_id: int, phash_3: str) -> None:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET
                phash_3 = ?, phash_3_scanned_at = ?, is_removed = 0, removed_at = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (phash_3, now, now, int(row_id)),
        )
        conn.commit()


def library_file_update_destination_row(
    row_id: int,
    *,
    destination: str,
    current_filename: str,
    filename_stem: str,
) -> None:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET
                destination = ?, current_filename = ?, filename_stem = ?,
                is_removed = 0, removed_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (destination, current_filename, filename_stem, now, int(row_id)),
        )
        conn.commit()


def library_file_delete_by_destination(destination: str) -> None:
    dst = (destination or "").strip()
    if not dst:
        return
    with get_conn() as conn:
        conn.execute("DELETE FROM library_files WHERE destination = ?", (dst,))
        conn.commit()


def library_file_mark_removed(row_id: int) -> None:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET is_removed = 1, removed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now, now, int(row_id)),
        )
        conn.commit()


def library_file_clear_removed_if_exists(destination: str) -> None:
    now = _iso_now()
    dst = (destination or "").strip()
    if not dst:
        return
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET is_removed = 0, removed_at = NULL, updated_at = ?
            WHERE destination = ?
            """,
            (now, dst),
        )
        conn.commit()


def library_file_update_media_probe(
    row_id: int,
    *,
    media_codec: str | None,
    media_width: int | None,
    media_height: int | None,
    file_created_iso: str | None,
    media_mtime: float,
) -> None:
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET
                media_codec = ?, media_width = ?, media_height = ?,
                file_created_iso = ?, media_mtime = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                media_codec,
                media_width,
                media_height,
                file_created_iso,
                media_mtime,
                now,
                int(row_id),
            ),
        )
        conn.commit()


def library_files_list_active_rows() -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM library_files
            WHERE is_removed = 0
            ORDER BY library_root, destination
            """
        )
        return [dict(r) for r in cur.fetchall()]


def _library_scope_sql(scope_prefix: str | None) -> tuple[str, list]:
    """Optional filter: destination must start with scope_prefix (Health tree scope)."""
    if not scope_prefix or not str(scope_prefix).strip():
        return "", []
    sp = str(scope_prefix).strip().rstrip(os.sep)
    return " AND destination LIKE ?", [sp + os.sep + "%"]


def library_files_count_for_root(library_root: str) -> int:
    lr = (library_root or "").strip()
    if not lr:
        return 0
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM library_files WHERE is_removed = 0 AND library_root = ?",
            (lr,),
        ).fetchone()
        return int(row["c"] or 0)


def library_files_count_under_path(
    library_root: str,
    dir_path: str,
    scope_prefix: str | None = None,
) -> int:
    """Count indexed files under dir_path (folder + all subfolders), optionally scoped."""
    lr = (library_root or "").strip()
    dp = os.path.normpath((dir_path or "").strip())
    if not lr or not dp:
        return 0
    lr_n = os.path.normpath(lr)
    extra_sql, extra_args = _library_scope_sql(scope_prefix)
    if os.path.normpath(dp) == lr_n:
        with get_conn() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS c FROM library_files
                WHERE is_removed = 0 AND library_root = ?
                {extra_sql}
                """,
                (lr, *extra_args),
            ).fetchone()
            return int(row["c"] or 0)
    p = dp.rstrip(os.sep) + os.sep
    with get_conn() as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c FROM library_files
            WHERE is_removed = 0 AND library_root = ? AND destination LIKE ?
            {extra_sql}
            """,
            (lr, p + "%", *extra_args),
        ).fetchone()
        return int(row["c"] or 0)


def library_files_expand_dir(
    library_root: str,
    dir_path: str,
    scope_prefix: str | None = None,
) -> tuple[list[dict], list[tuple[str, int]]]:
    """
    Immediate children under dir_path: leaf rows and (subdir_name, subtree_count) pairs.
    Uses targeted SQL (no full-table load).
    """
    lr = (library_root or "").strip()
    dp = os.path.normpath((dir_path or "").strip())
    if not lr or not dp:
        return [], []
    p = dp.rstrip(os.sep) + os.sep
    plen = len(p)
    extra_sql, extra_args = _library_scope_sql(scope_prefix)

    with get_conn() as conn:
        sql_leaves = f"""
            SELECT * FROM library_files
            WHERE is_removed = 0 AND library_root = ?
              AND destination LIKE ?
              AND instr(substr(destination, ? + 1), '/') = 0
              {extra_sql}
        """
        args_leaves = [lr, p + "%", plen, *extra_args]
        leaf_rows = [dict(r) for r in conn.execute(sql_leaves, args_leaves).fetchall()]

        sql_seg = f"""
            SELECT DISTINCT substr(substr(destination, ? + 1), 1,
                instr(substr(destination, ? + 1) || '/', '/') - 1) AS seg
            FROM library_files
            WHERE is_removed = 0 AND library_root = ?
              AND destination LIKE ?
              AND instr(substr(destination, ? + 1), '/') > 0
              {extra_sql}
        """
        args_seg = [plen, plen, lr, p + "%", plen, *extra_args]
        segs: list[str] = []
        for row in conn.execute(sql_seg, args_seg).fetchall():
            seg = (row["seg"] or "").strip()
            if seg:
                segs.append(seg)
        segs = sorted(set(segs), key=lambda s: s.lower())

    out_pairs: list[tuple[str, int]] = []
    for seg in segs:
        child_path = os.path.normpath(os.path.join(dp, seg))
        c = library_files_count_under_path(lr, child_path, scope_prefix)
        out_pairs.append((seg, c))

    leaf_rows.sort(key=lambda r: str(r.get("destination") or "").lower())
    return leaf_rows, out_pairs


def library_files_find_processed_match_for_stem(stem: str) -> int | None:
    """First filed processed_files row whose destination basename stem matches."""
    st = (stem or "").strip()
    if not st:
        return None
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, destination FROM processed_files
            WHERE status = 'filed' AND destination IS NOT NULL AND TRIM(destination) != ''
            """,
        ).fetchall()
    for r in rows:
        dest = (r["destination"] or "").strip()
        if not dest:
            continue
        try:
            if Path(dest).stem == st:
                return int(r["id"])
        except Exception:
            continue
    return None


def library_files_duplicate_groups() -> list[dict]:
    """Groups of 2+ active files sharing effective phash (3 then 2 then 1).

    Within each phash bucket we dedupe by case-folded destination so a
    stale index row (e.g. the file was originally indexed as `.MKV` and
    later re-indexed as `.mkv` without the old row being marked removed)
    doesn't surface as a "duplicate" of itself. When two rows collide on
    the casefolded key we prefer the one whose destination still exists
    on disk — that's the row the user can actually act on.
    """
    from pathlib import Path
    rows = library_files_list_active_rows()
    buckets: dict[str, list[dict]] = {}
    for r in rows:
        p3 = (r.get("phash_3") or "").strip()
        p2 = (r.get("phash_2") or "").strip()
        p1 = (r.get("phash_1") or "").strip()
        eff = p3 or p2 or p1
        if not eff:
            continue
        buckets.setdefault(eff, []).append(r)
    out = []
    for ph, members in sorted(buckets.items(), key=lambda x: x[0]):
        seen: dict[str, dict] = {}
        for m in members:
            dest = (m.get("destination") or "").strip()
            if not dest:
                continue
            key = dest.casefold()
            prev = seen.get(key)
            if prev is None:
                seen[key] = m
                continue
            # Collision on case-folded path: prefer the row whose file
            # actually exists on disk over a stale index entry.
            try:
                prev_exists = Path(prev.get("destination") or "").is_file()
            except OSError:
                prev_exists = False
            try:
                cur_exists = Path(dest).is_file()
            except OSError:
                cur_exists = False
            if cur_exists and not prev_exists:
                seen[key] = m
        unique_members = list(seen.values())
        if len(unique_members) < 2:
            continue
        out.append({"phash": ph, "files": unique_members})
    return out


def library_files_active_matching_phash(phash: str) -> list[dict]:
    """
    Active library_files rows whose effective phash (phash_3 → phash_2 → phash_1)
    equals the given string. Uses OR match on stored columns, then filters to effective phash.
    """
    ph = (phash or "").strip()
    if not ph:
        return []
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM library_files
            WHERE is_removed = 0
              AND (
                TRIM(COALESCE(phash_1, '')) = ?
                OR TRIM(COALESCE(phash_2, '')) = ?
                OR TRIM(COALESCE(phash_3, '')) = ?
              )
            """,
            (ph, ph, ph),
        )
        rows = [dict(r) for r in cur.fetchall()]
    out: list[dict] = []
    for r in rows:
        p3 = (r.get("phash_3") or "").strip()
        p2 = (r.get("phash_2") or "").strip()
        p1 = (r.get("phash_1") or "").strip()
        eff = p3 or p2 or p1
        if eff == ph:
            out.append(r)
    return out


def library_files_mark_duplicate_pending(row_ids: list[int]) -> None:
    """Set duplicate_review_pending=1 for the given row IDs."""
    ids = [int(x) for x in row_ids if x is not None]
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    now = _iso_now()
    with get_conn() as conn:
        conn.execute(
            f"UPDATE library_files SET duplicate_review_pending = 1, updated_at = ? "
            f"WHERE id IN ({placeholders})",
            (now, *ids),
        )
        conn.commit()


def library_files_clear_duplicate_pending(row_id: int) -> None:
    """Clear duplicate_review_pending after manual resolution."""
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE library_files SET duplicate_review_pending = 0, updated_at = ?
            WHERE id = ?
            """,
            (_iso_now(), int(row_id)),
        )
        conn.commit()


def library_files_set_duplicate_ignored_for_phash(phash: str, ignored: int) -> int:
    """
    Set duplicate_ignored (0 or 1) for every active library_files row whose effective
    phash (phash_3 → phash_2 → phash_1) equals phash. Returns number of rows updated.
    """
    ph = (phash or "").strip()
    if not ph:
        return 0
    want = 1 if int(ignored) else 0
    rows = library_files_list_active_rows()
    ids: list[int] = []
    for r in rows:
        p3 = (r.get("phash_3") or "").strip()
        p2 = (r.get("phash_2") or "").strip()
        p1 = (r.get("phash_1") or "").strip()
        eff = p3 or p2 or p1
        if eff == ph:
            ids.append(int(r["id"]))
    if not ids:
        return 0
    now = _iso_now()
    placeholders = ",".join("?" * len(ids))
    with get_conn() as conn:
        conn.execute(
            f"""
            UPDATE library_files SET duplicate_ignored = ?, updated_at = ?
            WHERE id IN ({placeholders})
            """,
            (want, now, *ids),
        )
        conn.commit()
    return len(ids)


def library_files_count_removed() -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM library_files WHERE is_removed = 1"
        ).fetchone()
        return int(row["c"] or 0)


def library_files_clean_removed(*, purge_linked_processed_files: bool) -> dict:
    """Delete is_removed rows; optionally delete linked processed_files."""
    removed_ids: list[int] = []
    pf_ids: list[int] = []
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, source_record_id FROM library_files WHERE is_removed = 1"
        )
        for r in cur.fetchall():
            removed_ids.append(int(r["id"]))
            sid = r["source_record_id"]
            if sid is not None and purge_linked_processed_files:
                pf_ids.append(int(sid))
        conn.execute("DELETE FROM library_files WHERE is_removed = 1")
        pf_deleted = 0
        if purge_linked_processed_files:
            for pid in set(pf_ids):
                c2 = conn.execute("DELETE FROM processed_files WHERE id = ?", (pid,))
                pf_deleted += c2.rowcount or 0
        conn.commit()
    return {
        "library_files_deleted": len(removed_ids),
        "processed_files_deleted": pf_deleted,
    }


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_password_hash() -> str | None:
    with get_conn() as conn:
        cur = conn.execute("SELECT password_hash, session_hours FROM auth WHERE id = 1")
        row = cur.fetchone()
        return row["password_hash"] if row else None


def get_session_hours() -> int:
    with get_conn() as conn:
        cur = conn.execute("SELECT session_hours FROM auth WHERE id = 1")
        row = cur.fetchone()
        return row["session_hours"] if row else 24


def set_password(hashed: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE auth SET password_hash = ? WHERE id = 1", (hashed,))
        conn.commit()


def set_session_hours(hours: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE auth SET session_hours = ? WHERE id = 1", (hours,))
        conn.commit()


def create_session(token: str, hours: int) -> None:
    from datetime import timedelta
    now = datetime.now()
    expires = now + timedelta(hours=hours)
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sessions (token, created_at, expires_at) VALUES (?, ?, ?)",
            (token, now.isoformat(), expires.isoformat())
        )
        conn.commit()


def validate_session(token: str) -> bool:
    """Validate + lazily slide the session's idle-expiry window.

    `session_hours` acts as an idle timeout — an active session keeps
    renewing itself, an abandoned one dies after the configured period.

    Slide is THROTTLED: we only update `expires_at` when the current
    window has consumed >5% of its duration since the last slide. With
    the default 24h session that's a write at most once every ~72
    minutes per active session, regardless of how many polls /
    requests fire from the page. The previous behaviour wrote to disk
    on every authenticated request, which produced enough write
    contention under polling load that some slides silently lost the
    SQLite lock race — the old `expires_at` stuck around, eventually
    expired, and the next request bounced the user to /login. Lazy
    slide eliminates that race entirely while preserving the rolling
    idle-window semantic.
    """
    if not token:
        return False
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT expires_at FROM sessions WHERE token = ?", (token,)
        )
        row = cur.fetchone()
        if not row:
            return False
        now = datetime.now()
        expires_at = datetime.fromisoformat(row["expires_at"])
        if expires_at < now:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            return False
        # Pull session_hours from the same connection (avoids opening a
        # second connection just to read a single column).
        hcur = conn.execute("SELECT session_hours FROM auth WHERE id = 1")
        hrow = hcur.fetchone()
        hours = hrow["session_hours"] if hrow else 24
        from datetime import timedelta
        full_window = timedelta(hours=hours)
        remaining = expires_at - now
        consumed = full_window - remaining
        # Only slide if we've burned >5% of the window since last slide.
        if consumed.total_seconds() > full_window.total_seconds() * 0.05:
            new_expires = (now + full_window).isoformat()
            conn.execute(
                "UPDATE sessions SET expires_at = ? WHERE token = ?",
                (new_expires, token),
            )
            conn.commit()
        return True


def delete_session(token: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()


def purge_expired_sessions() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE expires_at < ?",
                     (datetime.now().isoformat(),))
        conn.commit()


# ---------------------------------------------------------------------------
# Scene-grab provenance — links download-client jobs to the source scene
# they were grabbed from on /scenes.
# ---------------------------------------------------------------------------

def record_scene_grab(
    release_title: str,
    guid: str,
    source_db: str,
    source_id: str,
    source_title: str = "",
    source_studio: str = "",
    source_performers: list[str] | None = None,
    source_poster_url: str = "",
    source_date: str = "",
    download_url: str = "",
    kind: str = "scene",
    ts_uid: str = "",
) -> None:
    """Persist the link from a download client job to the scene it was
    grabbed from on /scenes. Stores three potential match keys —
    `download_url` (magnet / NZB URL we sent to the client, the most
    stable identifier), `guid` (prowlarr GUID), and `release_title`
    (the job name on the client) — so the read-side can prefer
    whichever the client surfaces back. Idempotent: re-grabbing the
    same release just appends another row — the lookup picks the
    most recent."""
    rt = (release_title or "").strip()
    if not rt:
        return
    perfs_json = json.dumps([p for p in (source_performers or []) if p])
    k = (kind or "scene").strip().lower()
    if k not in ("scene", "movie"):
        k = "scene"
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO scene_grab_log
                (release_title, guid, download_url, source_db, source_id,
                 kind, ts_uid, source_title, source_studio, source_performers,
                 source_poster_url, source_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rt, (guid or "").strip(), (download_url or "").strip(),
                (source_db or "").strip(), (source_id or "").strip(),
                k, (ts_uid or "").strip(),
                source_title or "", source_studio or "", perfs_json,
                source_poster_url or "", source_date or "",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def lookup_scene_grab_by_ts_uid(ts_uid: str) -> dict | None:
    """Direct lookup by the Top-Shelf-injected uid extracted from a
    `[ts-XXXXXXXX]` tag in the filename. Bulletproof match key —
    survives any client-side renaming because the tag rides along
    inside the job name itself."""
    uid = (ts_uid or "").strip().lower()
    if not uid:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM scene_grab_log
            WHERE LOWER(ts_uid) = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (uid,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def lookup_scene_grab_for_filename(filename: str) -> dict | None:
    """Find the most recent un-consumed scene_grab_log row whose
    `release_title` matches the file's name. Used by `process_single`
    on /scenes-initiated downloads to short-circuit phash-based
    matching when we already know the source scene from the grab.

    Match strategy (case-insensitive, all in one query):
      • exact full filename
      • the filename's stem (no extension)
      • the release_title is a prefix of the stem
      • the stem is a prefix of the release_title
    The prefix variants catch the common case where the client adds
    `.mkv` / `.mp4` to the release name, or where the unpack drops a
    `.s01e02-WRB.mkv` style suffix off a Movies-grabbed scene.

    Returns the most recently created un-consumed row or None.
    Consumed rows (already auto-filed) are excluded so a duplicate
    grab of the same release can't trigger a double-file."""
    fn = (filename or "").strip()
    if not fn:
        return None
    stem = Path(fn).stem
    fn_l = fn.lower()
    stem_l = stem.lower()
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM scene_grab_log
            WHERE consumed_at IS NULL
              AND (
                LOWER(release_title) = ?
                OR LOWER(release_title) = ?
                OR LOWER(?) LIKE LOWER(release_title) || '%'
                OR LOWER(release_title) LIKE LOWER(?) || '%'
              )
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (fn_l, stem_l, stem_l, stem_l),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def mark_scene_grab_consumed(grab_id: int) -> None:
    """Stamp `consumed_at` on a scene_grab_log row so it won't be
    re-applied to another file. Called after a successful auto-file
    from a grab tag."""
    if not grab_id:
        return
    with get_conn() as conn:
        conn.execute(
            "UPDATE scene_grab_log SET consumed_at = ? WHERE id = ?",
            (datetime.now().isoformat(timespec="seconds"), int(grab_id)),
        )
        conn.commit()


def purge_orphan_scene_grabs(max_age_days: int = 30) -> int:
    """Delete scene_grab_log rows that are either already consumed and
    older than 7 days, or unconsumed and older than `max_age_days`.
    Keeps the table from growing forever — pending grabs that never
    make it to disk fall off after a month, used grabs fall off
    quickly (the source_scene poster has already been displayed by
    then). Returns the number of rows deleted."""
    from datetime import timedelta
    now = datetime.now()
    consumed_cutoff = (now - timedelta(days=7)).isoformat(timespec="seconds")
    pending_cutoff = (now - timedelta(days=max_age_days)).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.execute(
            """
            DELETE FROM scene_grab_log
            WHERE (consumed_at IS NOT NULL AND consumed_at < ?)
               OR (consumed_at IS NULL AND created_at < ?)
            """,
            (consumed_cutoff, pending_cutoff),
        )
        conn.commit()
        return cur.rowcount or 0


def enrich_download_items_with_source_scene(items: list[dict]) -> None:
    """Attach a `source_scene` dict to each download item that we have
    a scene-grab record for.

    Match priority (strongest stable identifier first):
      1. `ts_uid` — the Top-Shelf uid we injected into the job name as
         `[ts-XXXXXXXX]` at grab time. Survives every client rename.
      2. `download_url` — the magnet / NZB URL we sent to the client,
         which the client preserves verbatim per job.
      3. `guid` — the prowlarr GUID, exposed by some clients.
      4. `release_title` — the job name (case-insensitive against the
         release_title we stored at grab time, plus stem/basename).

    Each download collector populates `it["source_url"]`, `it["guid"]`,
    and `it["ts_uid"]` (extracted from the name) where it can. Title
    fallback only fires when nothing stronger matched. Most-recent
    grab wins on collisions."""
    if not items:
        return
    uids: set[str] = set()
    title_keys: set[str] = set()
    guids: set[str] = set()
    urls: set[str] = set()
    for it in items:
        ux = (it.get("ts_uid") or "").strip().lower()
        if ux:
            uids.add(ux)
        n = (it.get("name") or "").strip()
        if n:
            title_keys.add(n.lower())
            title_keys.add(Path(n).stem.lower())
            title_keys.add(Path(n).name.lower())
        g = (it.get("guid") or "").strip()
        if g:
            guids.add(g)
        u = (it.get("source_url") or "").strip()
        if u:
            urls.add(u)
    if not (uids or title_keys or guids or urls):
        return
    rows: list[dict] = []
    with get_conn() as conn:
        if uids:
            placeholders = ",".join("?" for _ in uids)
            cur = conn.execute(
                f"""
                SELECT * FROM scene_grab_log
                WHERE LOWER(ts_uid) IN ({placeholders})
                ORDER BY created_at DESC
                """,
                list(uids),
            )
            rows.extend(dict(r) for r in cur.fetchall())
        if urls:
            placeholders = ",".join("?" for _ in urls)
            cur = conn.execute(
                f"""
                SELECT * FROM scene_grab_log
                WHERE download_url IN ({placeholders})
                ORDER BY created_at DESC
                """,
                list(urls),
            )
            rows.extend(dict(r) for r in cur.fetchall())
        if guids:
            placeholders = ",".join("?" for _ in guids)
            cur = conn.execute(
                f"""
                SELECT * FROM scene_grab_log
                WHERE guid IN ({placeholders})
                ORDER BY created_at DESC
                """,
                list(guids),
            )
            rows.extend(dict(r) for r in cur.fetchall())
        if title_keys:
            placeholders = ",".join("?" for _ in title_keys)
            cur = conn.execute(
                f"""
                SELECT * FROM scene_grab_log
                WHERE LOWER(release_title) IN ({placeholders})
                ORDER BY created_at DESC
                """,
                list(title_keys),
            )
            rows.extend(dict(r) for r in cur.fetchall())
    if not rows:
        return
    # Lookup tables keyed by each match identifier; first row per key
    # wins (rows already sorted DESC by created_at).
    by_uid:   dict[str, dict] = {}
    by_url:   dict[str, dict] = {}
    by_guid:  dict[str, dict] = {}
    by_title: dict[str, dict] = {}
    for r in rows:
        ux = (r.get("ts_uid") or "").lower()
        if ux and ux not in by_uid:
            by_uid[ux] = r
        u = (r.get("download_url") or "")
        if u and u not in by_url:
            by_url[u] = r
        g = (r.get("guid") or "")
        if g and g not in by_guid:
            by_guid[g] = r
        rt = (r.get("release_title") or "").lower()
        if rt and rt not in by_title:
            by_title[rt] = r
    for it in items:
        ux = (it.get("ts_uid") or "").strip().lower()
        u = (it.get("source_url") or "").strip()
        g = (it.get("guid") or "").strip()
        n = (it.get("name") or "").strip().lower()
        match = None
        if ux:
            match = by_uid.get(ux)
        if not match and u:
            match = by_url.get(u)
        if not match and g:
            match = by_guid.get(g)
        if not match and n:
            match = by_title.get(n) or by_title.get(Path(n).stem.lower())
        if not match:
            continue
        try:
            performers = json.loads(match.get("source_performers") or "[]")
        except Exception:
            performers = []
        it["source_scene"] = {
            "db":         match.get("source_db") or "",
            "id":         match.get("source_id") or "",
            "kind":       (match.get("kind") or "scene"),
            "title":      match.get("source_title") or "",
            "studio":     match.get("source_studio") or "",
            "performers": performers if isinstance(performers, list) else [],
            "poster_url": match.get("source_poster_url") or "",
            "date":       match.get("source_date") or "",
        }


# ---------------------------------------------------------------------------
# Prowlarr indexer cache
# ---------------------------------------------------------------------------

def get_cached_indexers() -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute("SELECT id, name, protocol FROM prowlarr_indexers ORDER BY id")
        return [{"id": r["id"], "name": r["name"], "protocol": r["protocol"]} for r in cur.fetchall()]


def cache_indexers(indexers: list[dict]) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM prowlarr_indexers")
        for idx in indexers:
            conn.execute(
                "INSERT INTO prowlarr_indexers (id, name, protocol, fetched_at) VALUES (?, ?, ?, ?)",
                (idx["id"], idx["name"], idx["protocol"], datetime.now().isoformat())
            )
        conn.commit()


def clear_indexer_cache() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM prowlarr_indexers")
        conn.commit()


# ---------------------------------------------------------------------------
# IAFD enrichment — persisted performer filmography + scene breakdowns.
#
# Write-through from the existing scrapers in main.py: whenever we hit
# IAFD and parse a performer's filmography or a film's scene breakdowns,
# we also upsert into these tables so the data accretes durably and
# survives restarts. Reads are opportunistic for now (step 1: tables +
# write-through only); a later step hooks library UI + background
# trickle worker off the same tables.
# ---------------------------------------------------------------------------


def iafd_upsert_film(url: str, title: str, year: str = "",
                     studio: str = "") -> None:
    """Insert or update a film record. last_scraped is touched only when
    we have real metadata (title + year) — thin placeholders inserted
    from a performer filmography row don't claim freshness, so the
    background worker still picks them up for full scraping later."""
    if not url or not title:
        return
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO iafd_films (url, title, year, studio, last_scraped)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title        = excluded.title,
                year         = COALESCE(NULLIF(excluded.year, ''), iafd_films.year),
                studio       = COALESCE(NULLIF(excluded.studio, ''), iafd_films.studio),
                last_scraped = excluded.last_scraped
            """,
            (url, title, year or "", studio or "", now),
        )
        conn.commit()


def iafd_upsert_performer_filmography(
    performer_url: str,
    performer_name: str,
    films: list[dict],
) -> None:
    """Save a performer's full filmography in one transaction. Replaces
    the existing performer→film links so stale entries disappear when
    IAFD rewrites a performer's page. Each ``films`` dict is
    {url, title, year, studio}."""
    if not performer_url or not performer_name:
        return
    now = datetime.now().isoformat(timespec="seconds")
    years = [f.get("year") for f in films if f.get("year")]
    first_year = min(years) if years else None
    last_year = max(years) if years else None
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO iafd_performers (url, name, last_scraped, film_count, first_year, last_year)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                name         = excluded.name,
                last_scraped = excluded.last_scraped,
                film_count   = excluded.film_count,
                first_year   = excluded.first_year,
                last_year    = excluded.last_year
            """,
            (performer_url, performer_name, now, len(films), first_year, last_year),
        )
        for f in films:
            fu = (f.get("url") or "").strip()
            ft = (f.get("title") or "").strip()
            if not fu or not ft:
                continue
            conn.execute(
                """
                INSERT INTO iafd_films (url, title, year, studio)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title  = excluded.title,
                    year   = COALESCE(NULLIF(excluded.year, ''), iafd_films.year),
                    studio = COALESCE(NULLIF(excluded.studio, ''), iafd_films.studio)
                """,
                (fu, ft, f.get("year") or "", f.get("studio") or ""),
            )
        # Rebuild the link set for this performer: delete existing, then
        # insert current. Films themselves are untouched so other
        # performers' joins aren't affected.
        conn.execute(
            "DELETE FROM iafd_performer_films WHERE performer_url = ?",
            (performer_url,),
        )
        for f in films:
            fu = (f.get("url") or "").strip()
            if not fu:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO iafd_performer_films (performer_url, film_url) VALUES (?, ?)",
                (performer_url, fu),
            )
        conn.commit()


def iafd_upsert_scene_breakdowns(film_url: str, scenes: list[dict]) -> None:
    """Persist a film's scene breakdown. Each ``scenes`` dict is
    {number, label, cast: [names...]}. Replaces the film's existing
    rows wholesale — breakdowns don't usually change post-release but
    IAFD does occasionally correct cast lists."""
    if not film_url:
        return
    import json as _json
    with get_conn() as conn:
        conn.execute("DELETE FROM iafd_film_scenes WHERE film_url = ?", (film_url,))
        for sc in scenes or []:
            number = sc.get("number")
            if number is None:
                continue
            label = sc.get("label") or f"Scene {number}"
            cast = [n for n in (sc.get("cast") or []) if n]
            conn.execute(
                """
                INSERT INTO iafd_film_scenes (film_url, scene_number, label, cast_json)
                VALUES (?, ?, ?, ?)
                """,
                (film_url, int(number), label, _json.dumps(cast, ensure_ascii=False)),
            )
        conn.commit()


def iafd_get_filmography(performer_url: str) -> list[dict]:
    """Return a performer's stored filmography as
    ``[{url, title, year, studio}, ...]`` ordered by year desc then
    title asc. Empty list when the performer hasn't been scraped yet."""
    if not performer_url:
        return []
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT f.url, f.title, f.year, f.studio
            FROM iafd_performer_films pf
            JOIN iafd_films f ON f.url = pf.film_url
            WHERE pf.performer_url = ?
            ORDER BY COALESCE(f.year, '') DESC, f.title COLLATE NOCASE ASC
            """,
            (performer_url,),
        )
        return [
            {"url": r["url"], "title": r["title"],
             "year": r["year"] or "", "studio": r["studio"] or ""}
            for r in cur.fetchall()
        ]


def iafd_get_scene_breakdowns(film_url: str) -> list[dict]:
    """Return a film's stored scene breakdowns as
    ``[{number, label, cast:[...]}, ...]``. Empty list when the film
    hasn't been scraped yet or had no breakdown section."""
    if not film_url:
        return []
    import json as _json
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT scene_number, label, cast_json
            FROM iafd_film_scenes
            WHERE film_url = ?
            ORDER BY scene_number ASC
            """,
            (film_url,),
        )
        out = []
        for r in cur.fetchall():
            try:
                cast = _json.loads(r["cast_json"] or "[]")
            except Exception:
                cast = []
            out.append({
                "number": int(r["scene_number"]),
                "label": r["label"] or "",
                "cast": cast if isinstance(cast, list) else [],
            })
        return out


def iafd_get_performer_by_url(performer_url: str) -> dict | None:
    if not performer_url:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT url, name, last_scraped, film_count, first_year, last_year "
            "FROM iafd_performers WHERE url = ?",
            (performer_url,),
        )
        r = cur.fetchone()
        return dict(r) if r else None


def iafd_get_performer_by_name(name: str) -> dict | None:
    if not name:
        return None
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT url, name, last_scraped, film_count, first_year, last_year "
            "FROM iafd_performers WHERE name = ? COLLATE NOCASE",
            (name,),
        )
        r = cur.fetchone()
        return dict(r) if r else None


def iafd_trickle_candidates(stale_threshold_iso: str) -> list[dict]:
    """Return the list of library performers the trickle worker can
    consider scraping next, left-joined against iafd_performers so the
    worker can tell scraped / stale / never-scraped apart.

    ``stale_threshold_iso`` is an ISO timestamp; performers scraped
    at-or-before this count as stale. Use (now - refresh_days) to get
    the usual refresh rule.

    Returns dicts shaped::

        {"folder_name": "Cytherea", "last_scraped": "...", "is_scraped": bool}

    Ordered: never-scraped first (NULL last_scraped sorts first with the
    CASE trick), then stalest first. Caller decides whether to promote
    queue-mentioned rows to the top.
    """
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT fe.folder_name AS folder_name,
                   ip.last_scraped AS last_scraped,
                   ip.url          AS iafd_url
            FROM favourite_entities fe
            LEFT JOIN iafd_performers ip
              ON ip.name = fe.folder_name COLLATE NOCASE
            WHERE fe.kind = 'performer'
              AND (ip.last_scraped IS NULL OR ip.last_scraped <= ?)
            ORDER BY
                CASE WHEN ip.last_scraped IS NULL THEN 0 ELSE 1 END ASC,
                ip.last_scraped ASC,
                fe.folder_name COLLATE NOCASE ASC
            """,
            (stale_threshold_iso,),
        )
        return [
            {
                "folder_name": r["folder_name"],
                "last_scraped": r["last_scraped"] or "",
                "iafd_url": r["iafd_url"] or "",
                "is_scraped": bool(r["last_scraped"]),
            }
            for r in cur.fetchall()
        ]


def iafd_performer_count() -> int:
    """Total performers persisted in the enrichment table — used for
    status readouts / UI."""
    with get_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) AS c FROM iafd_performers")
        r = cur.fetchone()
        return int(r["c"]) if r else 0
