"""
SQLite database for Top-Shelf phash filer.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import os
DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "phash_filer.db")))

# Container paths are used when running in Docker; dev paths used otherwise
_in_docker = Path("/app/database").exists()

DEFAULTS = {
    "source_dir":        "/downloads/Top-Shelf/Namer/Series-Process-Manual" if _in_docker else "/home/mjm/NAS Downloads/Top-Shelf/Namer/Series-Process-Manual",
    "series_dir":        "/library/Series" if _in_docker else "/home/mjm/Top-Shelf/Series",
    "pattern_series":    "{studio} - S{year}E{month}{day} - {title}",
    "pattern_performer": "{performer} - S{year}E{month}{day} - {studio} - {title}",
    "api_key_stashdb":   "",
    "api_key_tpdb":      "",
    "api_key_fansdb":    "",
    "retry_enabled":      "true",
    "retry_hour":         "1",
    "retry_frequency_h":  "24",
    "features_dir":       "/library/Features" if _in_docker else "/home/mjm/Top-Shelf/Features",
    "api_key_tmdb":       "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxOWYxY2VlNDA5YmMzYzRlYWFlZDM3ZDcyZGI5NDE0YiIsIm5iZiI6MTUzMTg1Njg5MC4zNDYsInN1YiI6IjViNGU0N2ZhOTI1MTQxN2QwZDA0ZWIyMiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.ZVFb3f-ogfF4XugJekeQ0ToR6HrovCl3veAHed04ej0",
    "stash_url":          "http://172.16.0.6:35969",
    "stash_api_key":      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJtam0iLCJzdWIiOiJBUElLZXkiLCJpYXQiOjE3NzM2MDQ4ODB9.f3Ah8XSfhPHrDJVCZe2j1sNqyRZTLuxt1bLfP4EmOhM",
    "jellyfin_url":       "",
    "jellyfin_api_key":   "",
    "plex_url":           "",
    "plex_token":         "",
    "emby_url":           "",
    "emby_api_key":       "",
    "media_scan_enabled": "true",
    "media_scan_debounce_mins": "5",
}

DEFAULT_PERFORMER_DIRS = [
    {"path": "/library/Stars"   if _in_docker else "/home/mjm/Top-Shelf/Stars",   "label": "Stars",   "rank": 1},
    {"path": "/library/Erotica" if _in_docker else "/home/mjm/Top-Shelf/Erotica", "label": "Erotica", "rank": 2},
    {"path": "/library/E-Girls" if _in_docker else "/home/mjm/Top-Shelf/E-Girls", "label": "E-Girls", "rank": 3},
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
            CREATE TABLE IF NOT EXISTS processed_movies (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                filename      TEXT NOT NULL,
                tmdb_id       TEXT,
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
                rank  INTEGER NOT NULL DEFAULT 0
            )
        """)
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
                    "INSERT INTO directories (type, path, label, rank) VALUES ('performer', ?, ?, ?)",
                    (d["path"], d["label"], d["rank"])
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


# --- Directories ---

def get_directories(dir_type: str = None) -> list[dict]:
    with get_conn() as conn:
        if dir_type:
            rows = conn.execute(
                "SELECT * FROM directories WHERE type = ? ORDER BY rank ASC",
                (dir_type,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM directories ORDER BY type, rank ASC").fetchall()
        return [dict(r) for r in rows]


def save_directories(dirs: list[dict]) -> None:
    """Replace all directory entries with the provided list."""
    with get_conn() as conn:
        conn.execute("DELETE FROM directories")
        for d in dirs:
            conn.execute(
                "INSERT INTO directories (type, path, label, rank) VALUES (?, ?, ?, ?)",
                (d["type"], d["path"], d["label"], int(d["rank"]))
            )
        conn.commit()


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


def update_movie(filename: str, status: str, tmdb_id: str = None, title: str = None,
                 year: str = None, overview: str = None, poster_url: str = None,
                 destination: str = None, error: str = None) -> None:
    fields = {"status": status, "processed_at": datetime.now().isoformat(timespec="seconds")}
    for k, v in [("tmdb_id", tmdb_id), ("title", title), ("year", year),
                 ("overview", overview), ("poster_url", poster_url),
                 ("destination", destination), ("error", error)]:
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


def get_movie_stats() -> dict:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='filed'   THEN 1 ELSE 0 END) AS filed,
                SUM(CASE WHEN status='error'   THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped
            FROM processed_movies WHERE status != 'pending'
        """)
        return dict(cur.fetchone())


def get_retry_files() -> list[str]:
    """Return filenames of all non-filed, non-pending records that still exist in the source dir."""
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
