# carboot — productize design

Turn carboot from "serve a prebuilt index" into a self-contained container that
indexes a directory of CAR files on startup, keeps the index in sync, walks
subdirectories, and logs CAR access through the gateway.

Status: design approved 2026-06-11. Builds on the original design
(`2026-06-10-carboot-design.md`).

## Why

Today the index is built by a separate Python tool (`car_index.py`) run offline,
and the gateway serves a static, prebuilt SQLite index. To run carboot as a
turnkey service you have to build the index by hand first. This work makes the
container self-sufficient: point it at a directory of CAR files and it indexes
them itself, picks up new/changed files, and is observable (per-request access
logs).

## Terminology change: "shard" -> "car"

The original code calls each indexed CAR file a *shard*, which is a Storacha-ism
(a CAR that is one piece of a sharded upload). Productized carboot points at any
directory of CAR files, so the general term is **car**. Rename throughout:

- DB: `shards` table -> `cars` (`id, path, mtime, size`); `blk.shard` -> `blk.car`
- Code: `shardMap`->`carMap`, `shardPath`->`carPath`, etc.
- Access log field: `cars:[...]`

`reindex` migrates an existing old-schema index in place (SQLite `ALTER TABLE
... RENAME`, add `mtime`/`size` columns, then a `stat` pass to populate them) —
no block-data re-read, no full rebuild.

## Binary structure

One static Go binary `carboot` with subcommands; the current `cmd/cargw` and
`cmd/carpni` fold in:

- **`carboot gateway`** — serve (today's cargw) + `--reindex-on-start` + access log.
- **`carboot reindex`** — build/update the index from a CAR dir (the Python
  `car_index.py` ported to Go). Run standalone, on startup, or live via `exec`.
- **`carboot advertise`** — IPNI advertiser (today's carpni).

`car_index.py` stays in the repo under `tools/` as the reference origin; the
image and runtime use the Go `reindex`.

## reindex

- **Recursive scan**: `filepath.WalkDir` over `--cars-dir` and all subdirs,
  matching `*.car`. A car's identity is its **path relative to `--cars-dir`**,
  so `sub/dir/x.car` is distinct and resolvable.
- **mtime/size sync**, each run:
  - new (path not in `cars`) -> index it
  - changed (stored mtime or size differs) -> delete its `blk` rows, re-index
  - removed (in `cars`, absent on disk) -> prune its `blk` + `cars` rows
  - unchanged -> skip
- **CAR parsing in Go**: a streaming CARv1 reader mirroring `car_index.py`:
  varint frame -> CID -> record `multihash -> (car id, data offset, length)`.
  Truncated CARs are indexed up to the break and flagged.
- Writes the same SQLite schema (`blk` + `cars`). Idempotent and restartable.
- **Single-pass by default.** For a very large *initial* build, an optional
  `--jobs N` parallel mode (each worker writes its own DB, then an internal
  merge dedupes into the final index) is carried over from `car_index.py`.
  (Flag is `--jobs`, not `--shard` — "shard" now means nothing in carboot.)
  Incremental/live runs are always single-pass.

## Startup & live updates

- **On startup**: `carboot gateway --reindex-on-start` (default true) runs a
  `reindex` pass to completion before serving, so the gateway opens a current
  DB. Disable with `--reindex-on-start=false` / `CARBOOT_REINDEX_ON_START=0`.
  For an already-built DB this is quick (only new/changed/removed files).
- **Live reindex** (`podman exec carboot carboot reindex`): a second process
  writes the DB while the gateway serves. Two changes let the running gateway
  pick up changes without a restart:
  1. The gateway opens the index **`mode=ro`** (WAL), not `immutable=1`, so it
     sees the reindexer's committed writes.
  2. The in-memory **car-id->path map refreshes lazily on a miss**: a lookup
     returning an unknown car id reloads the map and retries. New cars are
     picked up on first access — no signal/coordination.
- SQLite WAL supports one writer + many readers; the reindexer uses
  `busy_timeout`. Serving continues during a live reindex.

## Access logging

A **request-scoped collector** threaded through `context`:

- The gateway HTTP middleware puts an empty collector in the request context.
- `CarIndexBlockstore.Get`, for each block read, adds the **car path** (and
  bytes) to the collector from the context.
- After the response, the middleware emits **one JSON line to stdout** per
  request: `{ts, remote_addr, method, cid, format, status, bytes, cars:[...]}` —
  the distinct CAR files that request touched.

One line per request (not per block): a big DAG fetch is a single line listing
every car it spanned. Identity-CID and index-miss reads touch no car, so those
lines show `cars:[]`.

## Container, config, volumes

- **Dockerfile**: same multi-stage static build, now producing one `carboot`
  binary. `ENTRYPOINT ["/usr/local/bin/carboot"]`, default `CMD ["gateway"]`.
- **Config** via flags + env (env for container ergonomics): `--cars-dir`
  (`CARBOOT_CARS_DIR`), `--index` (`CARBOOT_INDEX`), `--listen`,
  `--reindex-on-start`. The advertiser keeps its flags.
- **Volumes**: CAR dir mounted read-only; the index DB on a writable volume so
  it persists and `reindex` can write it. The gateway is the default command;
  `reindex`/`advertise` run as subcommands (startup, `exec`, or a sidecar).

## Error handling

- Truncated/empty CARs: indexed up to the break / skipped, recorded, never fatal.
- A car file that disappears between scan and read: gateway returns the normal
  not-found / read error for that block, logged with the car path; does not crash.
- Live reindex write contention: `busy_timeout` retries; reads never block on it.
- Old-schema DB without `cars`/mtime: migrated on first `reindex`.

## Testing

- **reindex unit**: nested-subdir temp tree of CAR files; assert recursive
  discovery, correct `(mh->car,off,len)` rows, mtime/size sync (add / change /
  prune), round-trip (read block at offset, hash matches).
- **migration test**: open an old-schema (`shards`/`shard`) DB, run reindex,
  assert migration to `cars`/`car` + populated mtime/size without re-reading
  block data.
- **access-log test**: drive the gateway handler, assert one JSON line per
  request with the right `cars:[...]`, bytes, status (incl. `cars:[]` for
  identity/miss).
- **live-update test**: open the gateway `mode=ro`, reindex a new CAR into the
  DB in-process, assert a fetch for a new block triggers the lazy car-map
  refresh and succeeds.

## Out of scope

- Filesystem watch / auto-indexing (trigger is explicit: startup or `exec`).
- Per-block or per-shard-counter access logging (chose per-request JSON).
- CARv2 / persistent sidecar indexes (the SQLite index is the index).
- Changing the advertiser beyond the rename and folding it into the binary.
