# Top-Shelf

A self-hosted media filing tool that uses perceptual hashing (phash) to automatically identify and organise adult content scenes by matching against [StashDB](https://stashdb.org), [ThePornDB](https://theporndb.net), and [FansDB](https://fansdb.cc).

![Top-Shelf Dashboard](docs/screenshot.png)

## What it does

Top-Shelf sits at the end of a processing pipeline (e.g. FileFlows → download folder) and:

- Computes a perceptual hash from each video file using Stash's exact algorithm
- Queries StashDB, ThePornDB, and FansDB in order, stopping at the first match
- Routes matched files to the correct library folder:
  - **Series routing** — matches studio name against your Series directory
  - **Performer routing** — matches the first female performer against Stars / Erotica / E-Girls directories (or whatever you configure)
- Generates an NFO file and downloads a thumbnail for each filed scene
- Caches phashes in SQLite so reruns are fast
- Retries unmatched files automatically on a configurable schedule
- Provides a manual search interface for anything that doesn't match automatically

## Requirements

- Docker and Docker Compose
- API keys for at least one of: StashDB, ThePornDB, FansDB
- An existing library folder structure (Series, Stars, etc.)

## Getting API keys

| Service | URL |
|---------|-----|
| StashDB | https://stashdb.org → Settings → API Keys |
| ThePornDB | https://theporndb.net → Account → API Keys |
| FansDB | https://fansdb.cc → Settings → API Keys |

## Quick start

1. Copy `docker-compose.yml` to your server and edit the volume paths
2. Create the config directory: `mkdir -p /your/config/path`
3. Copy `logo.png`, `background.jpg`, and `favicon.ico` into `static/` (optional — see [Customisation](#customisation))
4. Start the container: `docker compose up -d`
5. Open `http://<your-server-ip>:8891`
6. Go to **Settings** and enter your API keys and directory paths

## docker-compose.yml

```yaml
services:
  top-shelf:
    image: yourdockerhubusername/top-shelf:latest
    container_name: top-shelf
    restart: unless-stopped
    ports:
      - "8891:8891"
    volumes:
      # App config and database — persistent storage
      - /your/config/path:/app/data
      # Media library root — Series, Stars, Erotica, E-Girls etc. live here
      - /your/library/path:/library
      # Source folder — files to be processed
      - /your/downloads/path:/downloads
    environment:
      - TZ=Europe/London
```

Adjust the host paths on the left side of each volume mount. The container paths on the right should stay as-is.

## Directory structure

Top-Shelf expects your library to look roughly like this:

```
/library/
  Series/
    ATK Girlfriends/
    Dare Dorm/
    ...
  Stars/
    Performer Name/
    ...
  Erotica/
    Performer Name/
    ...
  E-Girls/
    Performer Name/
    ...
```

The exact folder names under Series are matched against studio names from the databases (normalised, exact match). Performer folders are matched in order across whichever directories you configure in Settings.

## File naming

Files are named using configurable patterns. The defaults are:

**Series:** `{studio} - S{year}E{month}{day} - {title}`

**Performer:** `{performer} - S{year}E{month}{day} - {studio} - {title}`

Available tokens: `{title}` `{studio}` `{performer}` `{performers}` `{year}` `{month}` `{day}` `{date}` `{source}`

Files are placed inside a `Season YYYY` subfolder automatically.

## Settings

All settings are stored in the database and editable via the web UI:

| Setting | Description |
|---------|-------------|
| Source directory | Where incoming files land (maps to your `/downloads` mount) |
| Series directory | Root of your Series library (under `/library`) |
| Performer directories | Ranked list of performer library roots |
| Naming patterns | Separate patterns for Series and Performer routing |
| API keys | StashDB, ThePornDB, FansDB |
| Auto retry | On/off toggle, run hour, and frequency |

## Customisation

Place these files in the `static/` folder (next to `main.py`) before building, or mount them as a volume:

| File | Purpose |
|------|---------|
| `logo.png` | Replaces the header logo |
| `background.jpg` | Full-screen background image |
| `favicon.ico` | Browser tab icon |

## Building from source

```bash
git clone https://github.com/yourusername/top-shelf
cd top-shelf
docker compose up --build -d
```

## Tech stack

- **Backend:** Python, FastAPI, APScheduler
- **Database:** SQLite
- **Phash:** FFmpeg + ImageHash, compatible with Stash's implementation
- **Frontend:** Vanilla HTML/CSS/JS

## Acknowledgements

- [Stash](https://github.com/stashapp/stash) for the phash algorithm
- [StashDB](https://stashdb.org), [ThePornDB](https://theporndb.net), [FansDB](https://fansdb.cc) for the scene databases

## Licence

MIT
