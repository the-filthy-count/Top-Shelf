# Test workflows

Run from the project root in an isolated environment:

```sh
python3.12 -m venv /tmp/top-shelf-tests
/tmp/top-shelf-tests/bin/pip install -c requirements-production-lock.txt -r requirements.txt
/tmp/top-shelf-tests/bin/pip check
/tmp/top-shelf-tests/bin/python -m unittest discover -s tests -v
```

`requirements-production-lock.txt` is the dependency set verified on Python 3.12,
which Docker now installs using constraints. Update the lock deliberately and
rerun the suite before rebuilding production.

## Browser workflows

```sh
/tmp/top-shelf-tests/bin/pip install -r requirements-browser.txt
/tmp/top-shelf-tests/bin/python -m playwright install chromium
/tmp/top-shelf-tests/bin/python tests/browser/smoke.py
```

These tests load the actual frontend files in Chromium. All API responses use
isolated fixtures: no live library, external indexer or download job is modified.
They cover atomic bio saves, headshot refresh in the library, download removal
and retry, cached filmstrips, search counts above 50, and per-file image validation.
They complement the backend tests; they do not replace live integration checks.

## Upload limits

Local image uploads are limited to 20 MiB per file. The server reads at most
20 MiB + 1 byte rather than trusting client metadata. Empty uploads are rejected.
The image manager reports filenames for rejected selections and keeps valid ones.

## Search completeness

Prowlarr searches follow the indexer's Newznab offset/total pagination and stop
when an indexer repeats a page. The former 25-per-protocol backend cap and
50-result UI caps are removed. Performer matching, blacklist rules, category
selection and optional duplicate grouping still affect displayed counts.
Pagination reference: https://newznab.readthedocs.io/en/latest/misc/api.html


## Audit regression checks (13 September 2026)

`python tests/browser/audit_fixes.py` checks safe login return paths, native bio
Escape handling, out-of-order full-library responses, Prowlarr continuation,
and cached missing headshots on both queue and index pages. Existing modal,
library-refresh and smoke suites remain complementary.

`test_audit_recovery.py` exercises interrupted copies, DB failure, replay and
transaction rollback using temporary folders and SQLite. `test_search_runtime.py`
checks pagination budgets, deadlines, admission control and return-path validation.

Run `python tests/integration_app.py` only in an ephemeral production-dependency
container with no live data/media mounts. It creates a temporary database and
exercises real startup, middleware, routes and shutdown. External cache warmup
is stubbed and external Requests traffic is disabled. It also checks that a
blocked cleanup worker does not block the health endpoint.

Performer merges now retain source files while verified copies are made. They
need free destination space for those copies. Journals live under metadata/merge-jobs;
retrying the identical merge request resumes a failed operation. Database changes
commit together before cleanup; changed or newly added source files are retained
with warnings. Symlinked source trees are rejected rather than followed.

Prowlarr uses eight shared workers, four admitted searches, a 45-second budget,
a ten-page per-indexer batch and bounded response/result sizes. Partial responses
include warnings and continuation offsets. Continue search requests the next batch
and retains/deduplicates previously displayed results.
