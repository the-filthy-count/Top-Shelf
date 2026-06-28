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
| `static/scenes.html` | `/scenes` — **feed grid only** (Movies / Performers / Studios / Vices tabs). Browse the existing catalog. No search, no spotlight. |
| `static/discover.html` | `/discover` — **search + spotlight + detail panel**. Find new content. Was originally part of `/scenes`; the search/spotlight half was split out. Spotlight performer row lives here, not on `/scenes`. |
| `static/scenes-common.js` | Shared JS bundle for `/scenes` and `/discover`. Init gates by DOM probe: `#scenesGrid` runs the feed loader, `#spotlightGrid` runs the spotlight loader. |
| `static/queue.html` | `/queue` and settings modal |
| `static/downloads.html` | `/downloads` Discover page |
| `static/favourites.html` | `/library` |
| `static/movies.html` | `/movies` |
| `static/history.html` | `/history` |
| `static/library.html` | `/health` |
| `static/app-shell.css` | Global shared styles |

**`/scenes` vs `/discover` — disambiguation rule:** if the user mentions movies/performers/studios/vices browsing tabs or "Latest Movies", they mean `/scenes`. If they mention search results, scene lookup, or the spotlight performer tiles, they mean `/discover`. Both render with the same header/nav/glass chrome, so a screenshot alone can be ambiguous — confirm via the visible panel title or active tab.

## Architecture

### Backend: `main.py` + `database.py` + slim `app/` package

The bulk of the FastAPI app still lives in **`main.py`** (endpoints, scheduling, file watching, manual filing, suggestion logic) and **`database.py`** (SQLite schema, migrations, CRUD). Treat these as the primary surface for new work.

A small **`app/`** package holds extracted pure helpers that the processing pipeline needs and that have no `main.py`-only dependencies:

| Module | Contents |
|---|---|
| `app/utils/text.py` | `UPPERCASE_KEYWORDS`, `apply_caps`, `render_pattern`, `_sanitize_fs_component` |
| `app/utils/images.py` | `best_image_url` |
| `app/utils/fs.py` | `safe_move`, `_ensure_library_filed_permissions`, `LIBRARY_FILED_MODE_DEFAULT` |
| `app/core/phash.py` | `compute_phash`, `get_video_duration`, `probe_video_stream_meta`, `build_sprite`, sprite constants, `_phash_semaphore` |
| `app/core/matching.py` | `_score_name_match`, `_build_file_meta`, `_strip_ts_uid`, filename-cleaning regexes + tokenisers |
| `app/core/filing.py` | `normalise`, `_stashbox_normalize_match_source`, `_performer_weight`, `_studio_crosswalk_ids`, `_library_index_queue_matching_enabled` |
| `app/core/pipeline.py` | `ProcessingPipeline` class, `VIDEO_EXTENSIONS`, `_scene_dict_from_grab_row` |
| `app/integrations/stashbox.py` | **Async** stash-box queries (`httpx`) for the pipeline |

**Rules of thumb:**

- `main.py` imports from `app/`. **`app/` must not import from `main.py`** — circular.
- Only extract a helper into `app/` if it's **pure** (no `emit`, no `_log`, no calls into other `main.py` symbols). Otherwise keep it in `main.py`.
- The biggest non-extractable functions (`_scene_filing_plan`, `_match_vice_by_scene_tags`, `find_studio_dir`, `find_performer_dir`, `_merge_filename_performers_into_scene`, `_find_performer_dir_with_aliases`, `_find_performer_dir_via_library_index`) **stay in `main.py`** because they use module-level globals (`emit`, vice helpers, etc.).
- `query_stashbox` / `query_with_fallback` exist in **both** `main.py` (sync, `requests`-based, used by manual flows) and `app/integrations/stashbox.py` (async, `httpx`-based, used by `ProcessingPipeline`). The sync/async split is deliberate — keep them in sync semantically; do not "deduplicate" by deleting one.

SQLite runs in WAL mode. No ORM; direct `sqlite3` with prepared statements. Schema migrations are inline in `database.py`.

**All settings are stored in SQLite**, not config files. The only environment variable is `DB_PATH`.

### Processing Pipeline

```
Source folder (Watchdog)
  → FFmpeg phash (2 concurrent seeks; see _PHASH_CONCURRENCY)
  → StashDB → ThePornDB → FansDB → JAVStash (GraphQL fallback chain)
  → File routing (Studio Series, Performer, or Vice path)
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

## Chrome conventions

- **Logo home:** all linked header logos go to `/queue` (processing hub). `markActiveNav()` in `app-shell.js` sets `active-nav` on nav buttons — do not hardcode `active-nav` in page HTML.
- **Header nav:** ≥901px shows icon + short label in the segmented strip; ≤900px collapses into a hamburger menu with a 2-column labeled grid (`syncHeaderNavLayout` in `app-shell.js`).
- **Page title bars:**
  - **Browse pages** (`/scenes`, `/library`, `/queue`, `/news`, …): standalone `ts-page-head` sticky bar (reference: `/library` `fav-sticky`).
  - **Tool / split panels** (`/discover`, embedded panel heads): flat in-panel header (`discover-panel-header` — title row inside the panel, no extra glass pill).
- **Horizontal gutter:** `--shell-page-padding` (24px) on `.app` and body-level sections (`#scenesSection`, `#newsSection`, …).
- **Nav injection:** `app-shell.js` appends Queue (`data-ts-nav-queue`) and Download Indexer (`data-ts-nav-index`) when missing from static markup — templates do not need duplicate links.
- **`/history`:** redirects to `/health` (Library Health). The **History** stat on `/health` is filing-history rows in the DB, not the legacy route.

## Prowlarr UI (which surface to use)

| Surface | When |
|---|---|
| `openProwlarrSearchPopup()` (`ts-utils.js`) | Scene/movie title search from cards, wanted rows, downloads — full overlay with scene/movie toggle and filter chips. |
| `mountEmbeddedProwlarrSearch()` (`ts-utils.js`) | Inline deduped row list inside a panel (performer popup **Prowlarr** tab). |
| `openPerformerProwlarrSearchPopup()` (`performer-popup.js`) | Legacy full-screen tile modal by performer name (studio panel, etc.). Prefer the embed in the performer popup when that context is already open. |

**Long-term:** New work should use `openProwlarrSearchPopup` (global overlay) or `mountEmbeddedProwlarrSearch` (inline rows). Retire `openPerformerProwlarrSearchPopup` once remaining studio-only call sites move to one of those two.

**Performer popup scroll:** Films (Prowlarr / IAFD) and the scenes grid use `.pp-scroll-wrap` + a bottom fade hint when content overflows (`wirePpScrollAffordance` in `performer-popup.js`).

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
**Security → Directories → Content Filters → Pipeline → Databases → Downloads → RSS Feeds → Media Servers → Library Health → Appearance**

Settings modal width: 1240px. Nav labels in Seconds font.

The Spotlight settings panel was removed — the `/discover` spotlight row now runs entirely on the backend defaults / persisted DB values, with no UI to configure it.

## Spotlight Row (`/discover`)

The spotlight performer row sits above the detail panel in the right column of `/discover` (not `/scenes` — they were split). It fetches from `/api/metadata/spotlight-performers`.

**JS flow** (`scenes-common.js`, shared bundle; markup lives in `discover.html` — `#spotlightGrid`):
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
