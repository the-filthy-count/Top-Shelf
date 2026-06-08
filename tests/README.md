# Tests

Stdlib `unittest` only — no new dependencies. Run from the project root:

    python -m unittest discover tests -v

The suite is intentionally narrow. It covers the parts of the metadata
identification pipeline that are most painful when they regress silently:
specifically, the StashDB → TPDB → FansDB GraphQL fallback chain inside
`main.query_with_fallback`. Most other code paths have no automated
coverage and rely on manual UI testing per CLAUDE.md.

The tests **do not** require Prowlarr, Stash, or any external service.
HTTP and DB calls are mocked at the module-level with `unittest.mock.patch`.

`DB_PATH` is pointed at a throwaway tempfile before `main` is imported
so module-level startup doesn't touch the real SQLite database.
