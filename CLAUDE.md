# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**Top-Shelf** is a self-hosted media filing tool and Stash companion. It uses perceptual hashing (phash) to automatically identify adult video files by matching against StashDB, ThePornDB, and FansDB, then organizes them into a structured library with Kodi-compatible NFO metadata.

## Running the Application

```bash
# Development (requires Python 3.12+ and FFmpeg installed)
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8891 --reload

# Production
docker compose up -d

# Test environment
docker compose -f docker-compose.test.yml up -d
```

There is no automated test suite. Testing is done manually via the web UI at `http://localhost:8891` or direct API calls.

## Key Files

| File | Route / Purpose |
|---|---|
| `main.py` | ~14,000 lines — all backend logic, endpoints, scheduling, file watching |
| `database.py` | DB layer — SQLite WAL, schema, migrations, CRUD |
| `static/scenes.html` | `/scenes` |
| `static/queue.html` | `/queue` and settings modal |
| `static/downloads.html` | `/downloads` Discover page |
| `static/favourites.html` | `/library` |
| `static/movies.html` | `/movies` |
| `static/history.html` | `/history` |
| `static/library.html` | `/health` |
| `static/app-shell.css` | Global shared styles |

## Architecture

### Backend: Two-File Monolith

All Python logic lives in two files — `main.py` and `database.py`. **Do not split into modules; the project is intentionally monolithic.**

SQLite runs in WAL mode. No ORM; direct `sqlite3` with prepared statements. Schema migrations are inline in `database.py`.

**All settings are stored in SQLite**, not config files. The only environment variable is `DB_PATH`.

### Processing Pipeline

```
Source folder (Watchdog)
  → FFmpeg phash (25 concurrent seeks)
  → StashDB → ThePornDB → FansDB (GraphQL fallback chain)
  → File routing (Studio Series or Performer path)
  → File move + NFO + poster/fanart download
  → Debounced media server scan
```

### Frontend: Vanilla JS

Static files in `/static/`. No framework, no build step, no npm. `ts-utils.js` contains shared utilities. Design tokens in `app-shell.css`.

### Threading Model

- Main thread: FastAPI async event loop
- APScheduler daemon: background retry/sync jobs
- Watchdog thread: filesystem event monitoring
- `ThreadPoolExecutor`: phash and video probing
- Library indexing DB writes are **sequential** to avoid SQLite lock contention

### Key Integrations

| Service | Protocol | Purpose |
|---|---|---|
| StashDB / ThePornDB / FansDB | GraphQL | Scene metadata |
| TMDB | REST | Movie metadata |
| Prowlarr | REST | Indexer search/grab |
| NZBGet, SABnzbd, qBittorrent, Transmission, Deluge | REST | Download clients |
| Stash, Jellyfin, Plex, Emby | REST | Library scan triggers |

## Design System

**Read `DESIGN.md` before making any visual changes.** It is the single source of truth for all UI patterns (gitignored, not version-controlled).

Key constants:

- Card resting border: `rgba(192,132,252,0.14)`
- Card hover: `outline: 1px solid rgba(192,132,252,0.35)` + `border-color: rgba(192,132,252,0.55)`
  - **Always use `outline` not `box-shadow` for hover rings** — `box-shadow` is clipped by `overflow:hidden` on card containers
- Name labels below posters: Seconds font, `rgba(30,24,44,0.98)` bg, `rgba(192,132,252,0.20)` top border
- Matched DB source pills: pink (`#f472b6`, `rgba(244,114,182,0.14)` bg)
- Panel glass: `linear-gradient(160deg, rgba(36,36,48,0.88) 0%, rgba(18,18,26,0.78) 100%)`

## Settings Modal Structure

10 nav categories in this order:
**Security → Directories → Content Filters → Pipeline → Databases → Downloads → RSS Feeds → Media Servers → Spotlight → Library Health**

Settings modal width: 1240px. Nav labels in Seconds font.

## Spotlight Row (`/scenes`)

The spotlight performer row sits above the detail panel in the right column of `/scenes`. It fetches from `/api/metadata/spotlight-performers`.

**JS flow** (`scenes.html`):
1. `loadSpotlightRow()` fetches the endpoint
2. If `performers.length === 0`, exits silently (no error shown)
3. Sets `display:flex` on `#spotlightRow`
4. Renders tiles and calls `spotlightTileClick(0)` to auto-load first performer

**Backend** (`main.py`):
- `STASHDB_PERFORMER_QUERY` — the GraphQL query
- `_fetch_spotlight_performers()` — reads settings, calls StashDB, filters results
- Minimum filter: at least one image AND `scene_count > 5`
- `/api/metadata/spotlight-performers` returns `{"performers": [...], "count": N}` or `{"performers": [], "error": "..."}`

**Relevant settings keys** (stored in DB):
- `api_key_stashdb` — must be set for spotlight to work
- `spotlight_count`, `spotlight_sort`, `spotlight_direction`
- `spotlight_excl_birthdate`, `spotlight_excl_career_start`, `spotlight_excl_height`, `spotlight_excl_measurements`, `spotlight_excl_breast_type`, `spotlight_excl_country`, `spotlight_excl_ethnicity`, `spotlight_excl_eye_color`, `spotlight_excl_hair_color`
- `spotlight_tattoos`, `spotlight_piercings` — values: `any` / `yes` / `no`

**Sort behaviour:**
- Default sort is `SCENE_COUNT DESC` with a random page (1–150) so different performers appear each load
- **Do not change the default back to `CREATED_AT`** — StashDB's newest entries all have `scene_count=0` (profiles are created before scenes are matched), so `CREATED_AT DESC` always returns zero results after filtering

**Diagnosing a blank spotlight row:**
1. Hit `/api/metadata/spotlight-performers` directly — if `{"performers": [], "error": "no_stashdb_key"}`, the API key is not set
2. Check browser console for `Spotlight: N performers (raw from StashDB: M)` log line
3. If `raw_count=0`, the StashDB API call is failing (bad key or network)
4. If `raw_count>0` but `count=0`, the exclusion filters are too aggressive

**Confirmed working GraphQL query:**
```graphql
query QueryPerformers($input: PerformerQueryInput!) {
  queryPerformers(input: $input) {
    count
    performers {
      id name images { url } scene_count
      birthdate career_start_year height measurements
      breast_type country ethnicity eye_color hair_color
      tattoos { location } piercings { location }
    }
  }
}
```
Variables: `{ "input": { "page": 1, "per_page": 30, "sort": "CREATED_AT", "direction": "DESC" } }`

## Packaging

```bash
cd /home/claude/top-shelf-new
zip -qr /home/claude/top-shelf-patched-vXX.zip top-shelf/ \
  --exclude "*.bak" --exclude "*__pycache__*" --exclude "*.pyc"
```

## CI/CD

GitHub Actions (`.github/workflows/docker-build.yml`) builds and pushes multi-platform images (`linux/amd64`, `linux/arm64`) to Docker Hub (`thefilthycount/top-shelf`) on push to `main` or version tags.
