# FileFlows completion and library reconciliation

Top-Shelf keeps the pre-processing fingerprint (`phash_1`) and computes the
post-processing fingerprint (`phash_2`) independently. Fingerprint 2 is tied to
the output file size and nanosecond modification time. Do not reuse fingerprint 1
as fingerprint 2 after encoding.

## Automatic fallback

Settings → Schedule → Post-processing library checks defaults to two minutes.
The check inspects registered files rather than recursively scanning the library.
Changed files must have identical size and modification time on observations at
least 60 seconds apart. Up to four files are hashed per pass. Failed files back
off for one hour. Hashing shares the app's two-decoder resource budget.

If a registered file disappears, the fallback accepts an extension change only
when there is exactly one same-stem video in the same directory and exactly one
matching library record. Ambiguous matches are left alone. It also checks removed
records so a repair scan during encoding cannot permanently lose the association.

A stability interval is a heuristic: use the callback for explicit completion,
especially if encoding can pause for long periods or changes the filename.

## FileFlows completion request

Configure a success-only HTTP request at the END of the FileFlows flow, after
output is finalized. Configure `TOP_SHELF_POST_PROCESS_TOKEN` in the Top-Shelf
container with a long random secret and restart the app. Use the same secret in
the request header. Do not expose this token in URLs or logs. Use HTTPS or a trusted
private network.

POST `/api/library/post-process`

Headers:

```
Authorization: Bearer YOUR_SECRET
Content-Type: application/json
```

JSON body (substitute FileFlows' actual original and final paths):

```json
{
  "original_path": "/library/performer/example.mp4",
  "output_path": "/library/performer/example.mkv"
}
```

Paths must be expressed as seen INSIDE the Top-Shelf container. Original must
identify an indexed record; output must exist within that record's library root.
The response contains `queued: true` and its unchanged `row_id`. The scheduled
check waits for stability, computes the hash, then atomically updates the library
record and scene/movie pipeline destination references. It rejects a destination
already owned by a different library record. No video is moved or deleted.

The endpoint also accepts normal app authentication. Without a configured token,
there is no special authentication exemption for this endpoint.

## Full Index

Index still discovers unregistered files and repairs missing records. It now
recomputes fingerprint 2 only when missing or its recorded size/mtime differs.
Existing fingerprint 2 values have no provenance until refreshed once after this
upgrade, so they will require a one-time recalculation. Fingerprint 1 is preserved.
Results are discarded if the video changes while hashing/probing. An obsolete
fingerprint 3 is cleared when fingerprint 2 is refreshed.

The automatic check and callback are implemented in Top-Shelf; this change does
not configure your FileFlows flow or deploy the running container.
