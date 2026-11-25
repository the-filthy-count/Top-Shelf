# Queue changes

The queue has one current desired-state record per stable file ID in
`queue_changes`. Its revision, action, desired payload, execution state, result
and error are separate from the observed file path and technical metadata.
`queue_change_history` retains every revision. `file_events` also records requests
and outcomes alongside existing path history.

## Audit and changes

| Entry point | Execution path |
| --- | --- |
| Automatic scene/movie/JAV matching and retries | The canonical change owns claims, attempts and retry delays; async scene work carries the revision into worker threads. |
| Manual scene, folder, vice and IAFD filing | Persisted manual jobs reference a change revision; superseded requests cannot run. |
| Single-file scene/movie processing | Persist before admission to the manual worker. |
| Manual movie selection | Persist the selected database ID and file identity; resolve the current path again in the worker. |
| Suggestion acceptance and matched scene filing | Save selected scene and source before executing; record the resolved destination before moving. |
| Removal, including duplicate cleanup | Save removal intent before the existing recoverable archive operation. |
| Queue pre-sorting and folder tags | Save source/destination pairs, including sidecars; roll back moves if the queue rename fails. |
| Search-detail editing | Save draft search metadata only. Editing a search hint does not approve a scene or replace an already accepted filing request. |

The list-check button in `/unmatched` switches between tiles and the change list.
The list includes unapproved files as drafts, desired destinations, errors,
revision-checked retry/cancel controls, editing and history. It uses pages of 100
and refreshes while open. This view includes file commands, download-client actions, recovery actions and
library maintenance commands. Non-file resources have stable command subjects
in the same current-state and history tables.

The existing bounded workers remain execution adapters. SQLite claims provide
per-file exclusion; a separate OS process is not required. A newer request can
wait while an older revision runs, but an older completion cannot overwrite it.
Successful manual jobs are retained instead of deleted.

## Restart and recovery

Pending manual work survives restart. Legacy pending payloads acquire stable
file IDs and revisions where their queue identity is available. Interrupted
matching that has not started effects can retry. Interrupted effects are blocked
for review, backed by the existing filesystem/recovery journals. No blanket
replay of an interrupted move or deletion is performed. Recovery storage remains
recoverable; this change does not introduce permanent deletion.

No production queue is rewritten by development tests. Tests use temporary
SQLite databases and media fixtures.

## Execution authority and remaining scope

Manual workers claim the canonical revision and load its payload in one SQLite
transaction. Legacy manual-job payloads and statuses cannot override it. Worker
projections are recreated from pending commands if lost. Automatic readiness,
attempt counts and retry delays also come from the canonical record; processing
jobs retain progress display and compatibility data.

Top-level scene filing, queue relocation and recoverable queue removal dispatch
through registered action names using saved JSON. Callbacks are allowed only for
internal steps of an already owned command. Manual filing uses that same lease.

The additional named service adapters in `main.py` cover download removal and
retry, completed-download imports, queue cleanup, library removal, indexing,
phash rescans, reshelving, poster backfills, image synchronization, performer
merges and recovery restore/purge. All use `queue_changes` and
`queue_change_history`; `command_subjects` only supplies resource identities.

These adapters save serializable inputs before submitting to a separate worker
pool. Workers reload the saved inputs and resolve current settings; credentials
are never persisted. Pending commands resume at startup. Interrupted effects
are blocked for review. Nested service steps stay inside their parent command.
The original validation, partial-download exclusions and filesystem journals
remain in force. HTTP error responses retain their status and record failure.

The list provides history, cancellation and revision-checked retry for these
commands as well as file commands. Authentication, settings edits, read-only
queries and caches are outside the media-action queue.

## One automatic matching pipeline

`ProcessingPipeline` is the only automatic matching dispatcher. Scene and movie
watchers, vice/JAV imports, retry schedules and suggestion requests register work
against the stable file ID, then wake this dispatcher. They do not own separate
matching loops or suggestion-search pools.

The worker reads `queue_changes` joined to `file_assets` and the observed video
record. After every job it reads the list again, so new work, renames and newer
requests take effect without rebuilding an in-memory batch. Claims verify both
the file ID and revision. The shared pipeline lock serializes matching and filing
with existing manual filing and maintenance operations.

For each video, the work order is:

1. Resolve the current path and confirm the download is complete.
2. Reuse validated media observations; probe or fingerprint only missing data.
3. Check the library for an existing copy.
4. Reuse a verified saved match, otherwise resolve the grab identity or search
   the appropriate scene/movie databases.
5. File verified matches. Persist suggestion searches as the next job for misses;
   these run after ready matching jobs, using the saved search fields.

Scenes, movies, JAV and vices use the same work list. Their provider queries and
filing destinations remain distinct. `processing_jobs` is a progress projection,
not a second scheduling authority. Watcher hold timers only govern admission of
new downloads; durable retry delays live in `queue_changes`.

`queue_media.read` is shared by matching, scoring, metadata displays, filmstrip
and preview generation, and source collision checks. Observations and verified
match snapshots are invalidated when device/inode, size or modification time
changes. Rename/chmod-only ctime changes retain analysis. Movie fingerprints are
now stored too, and automatic movie filing reuses them after the move. A new
explicit match selection clears an older automatic match checkpoint.

Deferred downloads and temporary failures retain their next operation and
options across retries. A service rate limit waits and retries the current
fingerprint request. Restart recovery resumes safe lookup/suggestion work while
interrupted filesystem effects still require review. Per-job import cleanup
options prevent an import batch from suppressing cleanup for unrelated work.

Before storing a lookup or beginning filing, the worker verifies that its claimed
revision is still current. Beginning automatic filesystem effects is fenced in a
transaction: a newer choice wins before that boundary; after it, requests must
wait for filing to finish. Interrupted fenced effects require review on restart.
Match checkpoints require the original lookup signature, even if another reader
has refreshed the record meanwhile. Each successful probe is committed separately
before hashing, and progress counts all ready jobs rather than the read window.
