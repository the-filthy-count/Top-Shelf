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
