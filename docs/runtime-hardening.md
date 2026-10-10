# Runtime hardening

These changes reduce application load and failure impact. They do not establish
or fix the cause of the NAS kernel panics.

## File safety

Cross-filesystem moves copy to a hidden `.top-shelf-*.partial` file alongside the
destination, verify source identity/size/timestamps and copied size, sync the
copy, publish by atomic rename, sync the destination directory, then remove the
source and sync its directory. Failed copies leave the source and previous
final destination untouched. A 64 MiB free-space margin is required.

A crash before publication may leave a partial copy. A crash after publication
but before source deletion may leave both copies. Keep the source until recovery
is verified; do not blindly delete either. There is no content checksum or
transaction spanning filesystem publication and SQLite metadata. Filesystem and
storage hardware must honor fsync for its durability guarantees to hold.

Configured source roots receive an advisory `.top-shelf-processing.lock`, held
until the process exits. A second cooperating instance using the exact same root
fails startup rather than processing the same files. This requires writable roots
and working advisory locks; ancestor/descendant paths and external programs are
not coordinated. Restart after changing source directories to acquire ownership
for the new configuration. The lock file remaining on disk is normal.

## Workload bounds

- Two concurrent media jobs, including duration/stream probes, hash generation,
  filmstrips and interactive captures. Interactive captures have priority.
- Media commands have deadlines, bounded captured output and tracked children
  killed on shutdown. Output is spooled to temporary files, checked every 100 ms;
  it can transiently exceed the 16 MiB threshold on disk before termination.
- Persistent thumbnail, discovery and maintenance executors reject excess work
  rather than accumulating unbounded futures. Rejected thumbnails can retry on
  subsequent requests; a maintenance request may need retrying when busy.
- Library indexing submits a sliding window of tasks. Reconciliation rotates
  through 500 records per pass plus already pending changes. It still reads the
  library row list, so database/list memory is not constant with library size.
- Boot maintenance and warmup stages use separate single-worker queues. Selected
  recurring content refreshes coalesce concurrent startup/scheduler triggers.
- Scan debounce replaces one scheduled job rather than starting sleeping threads.
- Warm-cache refreshes retain the last valid snapshot and space attempts apart.
- Activity persistence retains at most 2,000 pending messages and flushes every
  five seconds. Each live viewer has a 256-message queue; older messages can be
  dropped under sustained overload. Database log retention remains configurable.
- Managed image downloads (poster/logo, discovery cache, spotlight) enforce
  25 MiB transfer and 40 million raster-pixel limits. This does not cover every
  legacy network call or make SVG rendering a security sandbox.
- SQLite uses WAL with synchronous FULL for filing and other durable records.
  Rebuildable filename, matching and resolver caches use NORMAL for their cache
  updates only, restoring FULL afterwards so page reads do not pay a disk flush
  per cached field.

Health diagnostics show queued background work and pending activity messages.
`/api/performance` also exposes media/activity/startup information.

## Page-loading follow-up

Home reads filed database history and recorded library removal flags; it does
not stat every imported video. Folder ancestry is matched in memory, history is
read in indexed batches, and background Home polling cannot overlap requests.
Active cold snapshot caches publish their first completed scan even when a
watcher invalidates during the scan, leaving a background refresh pending.
Lazy library caches retain their stricter edit/invalidation behavior. Missing
vice logos can be repaired locally in an HTTP worker without waiting for boot
maintenance or remote tag lookup.

## Shutdown and cancellation

Shutdown stops watchers, cancels queued executor work, stops media children and
flushes pending logs. Running Python threads and third-party HTTP calls cannot
be forcibly interrupted safely; they may finish at their own timeout. Existing
search deadline/admission controls remain in place. This is not universal
request-disconnect cancellation, durable job resumption, or migration checkpointing.

## Deployment

Rebuild/pull the complete application image; the new Python modules must accompany
main.py. The production Compose example removes source-file overrides and sets
configurable defaults of 6 GiB RAM, 2 CPUs, 256 processes, 60 seconds stop grace,
and three 10 MiB Docker log files. Set TOP_SHELF_MEMORY_LIMIT, TOP_SHELF_CPUS and
TOP_SHELF_PIDS_LIMIT for the actual NAS. These only apply when the deployment is
recreated using this configuration; updating the image alone does not change an
existing container's limits.

The test Compose example uses separate library/download/database directories and
smaller limits. Populate it with disposable fixtures, never production inputs.
The NAS deployment has not been changed by these repository edits.

## Verification

Run `python -m unittest discover -s tests -p 'test_*.py' -q` with the pinned test
dependencies. Failure-path tests cover copy failure/source mutation, executor
admission, activity overflow, media timeout/output limits, image transfer limits,
and second-process source ownership. Unit tests do not simulate power loss or
prove NAS filesystem durability. Test the built image on disposable data before
production use.
