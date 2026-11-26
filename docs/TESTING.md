# Tests

Use an isolated environment to keep FastAPI, Pydantic and pydantic-core compatible:

```sh
python3 -m venv /tmp/top-shelf-tests
/tmp/top-shelf-tests/bin/python -m pip install -r requirements/test.txt
/tmp/top-shelf-tests/bin/python -m pip check
/tmp/top-shelf-tests/bin/python -m unittest discover -s tests -v
```

The constraints capture the dependency set verified with Python 3.14. Production uses Python 3.12; verify that runtime separately before changing its dependency pins. Tests use temporary databases and mocked integrations.

# Timing diagnostics

Authenticated GET `/api/performance` returns process-local counts, error counts, mean, last and recent p95 timings. Request timings also appear in the `Server-Timing` response header. Cache rebuilds are grouped by cache name; filmstrip frame cache hits, generations and failures have separate counters. Samples are bounded to 256 per metric, reset on restart, and contain no filenames, search text or external telemetry. Request timings measure server response preparation, not browser image decoding or streaming transfer.

# Download recovery

Failed NZBGet and SABnzbd history jobs have an explicit retry button. Other client failures still show details and alerts. Retry follows the clients' APIs:
- https://nzbget.com/documentation/api/editqueue/ (`HistoryRedownload`)
- https://sabnzbd.org/wiki/configuration/5.1/api (`mode=retry`)

Alerts are deduplicated in SQLite independently of dismissal; a successful observed state resets the failure so a later failure can alert again.

# Migration regression checks

Run the Python suite above plus `node --test tests/*.cjs`. The migration tests
exercise temporary databases and media files: stable identities, restart recovery,
bounded retries, cancellation, retained removals, restore/purge, collision and
rollback behaviour, delayed manual actions, and TPDB canonical performer routing.
Do not start the application's watchers against live settings for verification.
The migration has also been tested with the production Python 3.12 runtime.

# Replacement matching engine

The replacement engine's behavioral tests are in
`tests/test_video_matching_rebuild.py`. Run these with production Python 3.12:

```sh
python -m unittest discover -s tests -p 'test_video_matching_rebuild.py' -v
```

They cover provider response reuse, failures versus misses, content changes,
renames, credential changes, explicit rescan generations, source-ID precedence,
movie/JAV namespaces, stale-result rejection, interrupted provider cancellation,
and additive/idempotent schema installation. Tests use temporary SQLite databases
and mocked provider responses. No live library or API keys are required.

Validation on 2026-11-26: the focused matching/queue regression run passed 186
checks; a final 36-check run passed after the cancellation and rescan changes.
The full application run exercised 680 checks. Its remaining recovery, fixture
and UI failures were reproduced against the untouched pre-rebuild revision;
the full suite is therefore not a clean deployment gate yet. In particular,
legacy restoration tests still expect recoverable deletion, while the current
delete service permanently removes files. Live-container verification is separate
from these isolated tests.
