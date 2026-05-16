# smols3 TODO

## Known bugs / leaks

### Orphan bytes on overwrite & mode switch

PUTs that replace an existing key don't clean up the prior storage. The index record is overwritten in place, the old `StorageRef` is dropped on the floor, and no bookkeeping happens against the prior bytes.

| Scenario | Result | Recoverable by |
| --- | --- | --- |
| Same-key PUT in **file** mode (same `shard_dir_depth`) | Atomic rename overwrites the file. No leak. | n/a |
| Same-key PUT in **concat** mode | Old pack slice is unreferenced. `PackFileRecord.LiveBytes` is inflated by the dead bytes. | `compact` (eventually — but inflated LiveBytes makes the pack look healthier than it is, so compact is *less likely to trigger* on it) |
| PUT in concat then PUT in file for the same key | Pack slice leaks identically. | `compact` |
| PUT in file then PUT in concat for the same key | 1:1 file leaks. **Permanent** under current tooling. | nothing — `fsck` only walks index→disk, never disk→index |

Two fixes needed:

1. **PUT-side cleanup.** Before committing the new `ObjectRecord`, look up the existing record for the key. If present, in the same bolt tx:
   - pack ref → decrement `PackFileRecord.LiveBytes` for the old pack
   - file ref → `os.Remove` the old file (after the rename for same-path case is a no-op)
2. **Disk-walking fsck.** Add `fsck --reap-orphans` that walks `<data_dir>/<bucket>/**`, builds a set of paths referenced by the index, and offers to remove unreferenced files (with `--dry-run` by default). Slower full-disk scan, but it's the only way to find file-mode orphans from mode-switch overwrites.

Order: fix (1) first — it prevents new leaks. Then (2) cleans up historical ones.
