# Architecture migration

This is an incremental migration of the existing app. Existing performer-ID filing
rules and candidate scoring remain authoritative. Changes must preserve those
rules and the shared-root/partial-download protections.

## Implemented foundation

- `app/core/file_history.py` owns durable file identities and append-only events.
  Scene and movie queue rows receive independent IDs. Renames retain the ID;
  removing a queue record retains its history. Migrated paths are marked as first
  observed, not claimed to be the original download name.
- `app/core/jobs.py` owns persistent scene-pipeline work (also used by movie/JAV workers), atomic claims, phase
  checkpoints, delayed retries, and restart recovery. The work list is saved
  before processing. Failures get three attempts per run; partial downloads wait
  without consuming an attempt. Explicitly running a terminal job starts a new
  attempt budget. Automatic retries respect `retry_enabled`.
- `app/utils/fs.py` journals `safe_move` before touching media, using a separate
  database connection. A failed journal preparation prevents the move. Failure
  to confirm an already-completed move leaves a prepared entry for investigation;
  it does not falsely report that the move itself failed.
- Queue snapshots no longer group/tag media, purge queue records or dispatch
  import jobs. Folder maintenance and missed inbox-event recovery run on their
  own schedule. Derived metadata and thumbnail caching run in background snapshot builders.
  Queue GETs only read published snapshots; a cold GET returns a loading state.
  Movie-history reconciliation runs in scheduled maintenance.
- `app/integrations/scene_search.py` isolates joint-cast database discovery, while
  preserving exact, source-local performer-ID resolution and cast validation.
- Queue payloads expose `file_id` for registered files. The authenticated
  `/api/files/{file_id}/history?after=0&limit=100` endpoint returns identity, events
  and job state. Events use an ID cursor and a maximum page size of 500.

Startup acquires the existing exclusive source locks before recovering jobs.
The migration adds tables and triggers; it does not rename or relocate media.

## Recoverable queue removal

Queue removal now uses `app/core/queue_recovery.py` and the shared
`app/core/recovery.py` executor. Video and associated sidecars move into
`file-recovery` beside the application database. Recovery storage must be outside
configured media scan roots. Removal retains disk usage; the recovery panel offers
explicit permanent deletion when the user wants to reclaim it. Shared sidecars
remain beside another same-stem video.

The executor records its plan before moving files, publishes without overwriting
existing paths, and rolls completed moves back if another move or the database
update fails. Restore verifies retained files, configured inboxes and unoccupied
original paths. Queue identity and previous status survive restore, including
restoration after queue-history cleanup. Interrupted ambiguous operations are
retained for review rather than guessed or replayed; interrupted permanent
purges can be explicitly retried.

The queue sends stable IDs for removal actions. The server resolves the current
queue key and never falls back from a missing ID to a new file at the old name.
A newly admitted file reusing a removed filename receives a fresh identity and
job; the removed asset retains its recovery history. Scenes, Vice, JAV and movie
inbox routing remain distinct. `queue-history.js`
provides one accessible dialog component for paginated file history and recovery,
using text nodes for filenames and errors. Native dialog focus and Escape handling
keep it usable by keyboard. File history is available from the card footer;
recovery is available in the queue toolbar.

## Library entity recovery

Performer and studio entity removal now uses `app/core/library_recovery.py` with
the shared recovery executor. It snapshots the full entity record, edited
biography fields, library file index rows, and directory structure. All regular
files inside the selected folder are retained; metadata caches outside that tree
are left in place. Removal only drops index records after all files are retained
and the now-empty original directories have been removed. A newly arrived file
makes that operation fail and roll back instead of deleting the arrival.

Restoration preserves record IDs and empty directories. It refuses occupied
folders, conflicting records, changed roots, and symlinks. A failed database
restore rolls moved files back into recovery. The existing recovery panel lists
library bundles alongside queue entries and supports restoration or permanent
purge. UI wording explicitly explains that removal retains disk usage.

Vice entities use `app/core/vice_recovery.py`, preserving their settings-backed
identity, enriched monitored tags and index rows. Restore merges with unrelated
Vices added since removal and rejects reused IDs/names. Cache metadata outside
the library tree stays in place. Filesystem and settings/index updates share the
recovery transaction boundary; partial downloads remain protected.

`tests/test_library_recovery.py` and `tests/test_library_recovery_api.py` cover
real temporary-file round trips, database rollback, concurrent arrivals,
conflicting destinations/records, literal path prefixes, empty folders, retained
biographies and the actual HTTP routes without starting app watchers.

## Individual video and duplicate recovery

`app/core/video_recovery.py` now retains individual library videos removed through
Health or duplicate cleanup. It preserves known sidecars, leaves shared sidecars
beside alternate video extensions, snapshots index rows, and marks associated
processing records removed without discarding their identity. Restore verifies
those records are unchanged, restores their prior status and index IDs, and
refuses occupied destinations. Pending processing is cancelled on removal.

Duplicate cleanup still verifies the surviving keeper before removing inferior
copies; those removed copies now enter recovery and report zero freed bytes.
The quality gate uses `app/core/filing_recovery.py` to retain same-path incumbents
before exclusive publication of the new video. A failed move restores the old
bundle; failed NFO publication restores the inbox then the incumbent. Successful
upgrades retain the old copy for explicit recovery/purge. Inferior incoming copies
also enter recovery and can be restored as pending work.
The Health UI describes retention, and permanent purge remains an explicit action
in the shared recovery panel. Missing videos do not cause record deletion.

## Persistent workers and identity-aware actions

`app/core/movie_jobs.py` wraps movie/JAV processing with durable claims,
checkpoints, deferred partial-download handling and bounded retries. Batch movie
work is registered before processing; scheduled recovery handles pending movie
and JAV work after restart. Scene and movie namespaces remain distinct.

Queue deletion, suggestion acceptance/rejection and manual movie selection carry
file identities. Manual movie actions resolve them again when the worker starts.
Health video removal carries the library index ID and resolves the current path;
a missing identity never falls back to another file at the supplied old path.
Filename/path request fields remain compatible with existing API clients.

Manual scene, metadata, Vice and bulk filing also bind a stable identity when
queued, and resolve it again when the worker runs. The single-file UI carries the
identity captured when the dialog opens. Renaming the source cannot redirect the
job to a later file reusing that name. Missing sources and failed filing remain
failed jobs rather than disappearing as successful work. Late worker callbacks
cannot reactivate cancelled jobs or modify an already-scheduled retry.

## TPDB filing identity

Accepting a TPDB suggestion retrieves the exact selected scene's cast before
filing, including when a movie breakdown picker supplied names without IDs.
TPDB's explicit canonical `parent` performer ID takes precedence over its
site-specific credit ID. This never authorizes a name/alias fallback. An
unavailable authoritative scene leaves the file and suggestions in place for
retry. An unavailable library destination returns an actionable explanation.
Candidate discovery and scoring are unchanged.

## Compatibility boundaries

This is an incremental migration, not a replacement of every existing subsystem.
Legacy filename-keyed tables and API clients remain supported; not every endpoint
has been converted to an ID-only contract. Provider helpers, older frontend code,
and unrelated routes remain in `main.py` alongside the extracted focused modules.
The ordinary move journal is an audit trail, not a generic undo facility for
arbitrary historical renames. Recovery is available for the migrated removal and
quality-gate flows; destructive administrative maintenance outside these paths
has not been converted into a universal filesystem transaction system.

## Verification

`tests/test_file_history.py` covers migration, identity retention, transactional
rollback and retry limits. `tests/test_file_lifecycle.py` exercises real temporary
files through moving, rekeying, restart recovery and filing, plus journal failures.
Existing identity, matching, partial-download, collision and scheduling tests must
continue to pass. Use the isolated environment described in `TESTING.md`; never
start `main.py` against a live library to test a migration.

Recovery tests also exercise scene/movie/JAV restoration, occupied destinations,
partial downloads, database failures, cross-device moves, cursor stability,
permanent deletion and stale filename requests through the actual HTTP handlers.
The browser preview uses synthetic data and does not run application watchers.
