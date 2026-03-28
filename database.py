"""
SQLite database for Top-Shelf phash filer.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import os

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
    "media_scan_enabled": "true",
    "submit_phash_enabled": "true",
    "manual_submit_stashdb_default": "false",
    "media_scan_debounce_mins": "5",
    "folder_watch_enabled":    "true",
    "folder_watch_hold_secs":  "60",
    "download_watch_enabled":  "false",
    "download_watch_dir":      "/downloads/complete" if _in_docker else "/path/to/your/downloads/complete",
    "download_watch_hold_secs": "300",
    "download_client_path_prefix": "",
    "download_local_path_prefix": "",
    "download_import_remove_client": "false",
    "favourites_scan_enabled": "false",
    "favourites_scan_hour":    "3",
    "site_abbreviations":      '{"brcc": "Backroom Casting Couch", "excogi": "Exploited College Girls", "nvg": "Net Video Girls", "brf": "Backroom Facials", "hmf": "Hot MILFs Fuck", "bex": "BrazzersExxtra", "ps": "PropertySex", "rk": "Reality Kings", "atk": "ATK", "ftv": "FTV Girls", "bangcasting": "Bang! Casting", "tonightsgf": "Tonights Girlfriend", "2drops": "2 Drops", "hookhot": "Hookup Hotshot", "spacejunk": "Space Junk", "shoplyfter": "Shoplyfter", "painal": "Painal", "wasteland": "Wasteland Ultra", "xxxjob": "XXX Job Interviews", "brandnew": "Brand New Amateurs", "czechcast": "Czech Casting", "czechstreet": "Czech Streets", "escortcast": "Escort Casting", "fuckt5": "Fuck Team Five", "hoby": "Hoby Buchanon", "perv": "Perv Principal"}',
    "site_rename_map":         '{"Shoplyfter Mylf": "Shoplyfter", "Net Girl": "Net Video Girls", "Tonights Girlfriend": "Tonights Girlfriend", "BANG! Casting": "Bang! Casting", "Manyvids 2 Drops Studio": "2 Drops"}',
    "filename_patterns":       "pipe|dot_date|dot_date_short|bracket_site|dash_date",
    "filename_strip_words":    "fuckingsession.com,lulustream.com,xvideoscom,xvideos.com,pornhub.com",
    "min_file_size_mb":        "10",
    "tag_blacklist":           "",
}

DEFAULT_PERFORMER_DIRS = [
    {"path": "/library/Stars"   if _in_docker else "/path/to/your/library/Stars",   "label": "Stars",   "rank": 1},
    {"path": "/library/Erotica" if _in_docker else "/path/to/your/library/Erotica", "label": "Erotica", "rank": 2},
    {"path": "/library/E-Girls" if _in_docker else "/path/to/your/library/E-Girls", "label": "E-Girls", "rank": 3},
]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
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

def get_settings() -> dict:
    with get_conn() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}


def save_settings(data: dict) -> None:
    with get_conn() as conn:
        for key, value in data.items():
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value))
            )
        conn.commit()


def mark_download_import_done(client_item_id: str) -> None:
    """Record a successful manual or automatic import so we do not auto-import twice."""
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
    """Performers and studios: alphabetical by folder name within each kind."""
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
                scanned_at, updated_at
            ) VALUES (?, ?, ?, ?, 0, NULL, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                kind = excluded.kind,
                folder_name = excluded.folder_name,
                root_label = excluded.root_label,
                gender_filters_json = excluded.gender_filters_json,
                scanned_at = excluded.scanned_at,
                updated_at = excluded.updated_at
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


def favourite_overwrite_matches(
    row_id: int,
    *,
    image_url: str | None,
    aliases_json: str | None,
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


def favourite_set_star(row_id: int, is_favourite: bool) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE favourite_entities SET is_favourite = ?, updated_at = ? WHERE id = ?",
            (1 if is_favourite else 0, now, row_id),
        )
        conn.commit()


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
    """Clear TPDB / StashDB / FansDB match columns and sort key; keep image and aliases."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE favourite_entities SET
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
    """Clear a single source's match fields. source: TPDB, STASHDB, or FANSDB."""
    u = (source or "").strip().upper()
    if u == "TPDB":
        clause = "match_tpdb_id = NULL, match_tpdb_name = NULL"
    elif u in ("STASHDB", "STASH"):
        clause = "match_stashdb_id = NULL, match_stashdb_name = NULL"
    elif u in ("FANSDB", "FANS"):
        clause = "match_fansdb_id = NULL, match_fansdb_name = NULL"
    else:
        raise ValueError("source must be TPDB, StashDB, or FansDB")
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            f"UPDATE favourite_entities SET {clause}, updated_at = ? WHERE id = ?",
            (now, row_id),
        )
        conn.commit()


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
    """Resolve library folder via entity index (Favourites DB table) by TPDB / StashDB / FansDB performer id."""
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
            f"SELECT path, folder_name FROM favourite_entities WHERE kind = 'performer' AND ({where}) LIMIT 1",
            args,
        ).fetchone()
        return dict(row) if row else None


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
    with get_conn() as conn:
        cur = conn.execute("SELECT id FROM processed_files WHERE filename = ?", (filename,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur = conn.execute(
            "INSERT INTO processed_files (filename, status) VALUES (?, 'pending')", (filename,)
        )
        conn.commit()
        return cur.lastrowid


def get_phash(filename: str) -> str | None:
    """Return cached phash for a filename, or None if not yet computed."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT phash FROM processed_files WHERE filename = ? AND phash IS NOT NULL",
            (filename,)
        ).fetchone()
        return row["phash"] if row else None


def update_file(filename, status, phash=None, match_source=None, match_title=None,
                match_studio=None, match_date=None, performers=None,
                destination=None, error=None):
    fields = {"status": status, "processed_at": datetime.now().isoformat(timespec="seconds")}
    for k, v in [("phash", phash), ("match_source", match_source),
                 ("match_title", match_title), ("match_studio", match_studio),
                 ("match_date", match_date), ("performers", performers),
                 ("destination", destination), ("error", error)]:
        if v is not None:
            fields[k] = v
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [filename]
    with get_conn() as conn:
        conn.execute(f"UPDATE processed_files SET {set_clause} WHERE filename = ?", values)
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
                SUM(CASE WHEN status='no_dir'    THEN 1 ELSE 0 END) AS no_dir
            FROM processed_movies WHERE status != 'pending'
        """)
        row = dict(cur.fetchone())
    return {k: 0 if row[k] is None else row[k] for k in row}


def save_setting(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()


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
                SUM(CASE WHEN status='no_dir'    THEN 1 ELSE 0 END) AS no_dir
            FROM processed_files WHERE status != 'pending'
        """)
        return dict(cur.fetchone())


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
    if not token:
        return False
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT expires_at FROM sessions WHERE token = ?", (token,)
        )
        row = cur.fetchone()
        if not row:
            return False
        if datetime.fromisoformat(row["expires_at"]) < datetime.now():
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            return False
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
