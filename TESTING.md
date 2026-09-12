# Tests

Use an isolated environment to keep FastAPI, Pydantic and pydantic-core compatible:

```sh
python3 -m venv /tmp/top-shelf-tests
/tmp/top-shelf-tests/bin/python -m pip install -r requirements-test.txt
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
