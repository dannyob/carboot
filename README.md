# carboot

> **Alpha, work in progress.** Not production-ready; expect rough edges and
> breaking changes to flags and behaviour. The `gateway` subcommand is validated
> end-to-end against real CAR files (serves raw blocks and reconstructs full DAGs
> as CARs). `advertise` builds and is wired, but its live `cid.contact` ingestion
> has not yet been validated. No releases, no stability promises.

Point carboot at a directory of CAR files and it serves them as an IPFS
trustless gateway, indexing the files itself and keeping the index in sync. No
re-import into an IPFS blockstore; the CAR files stay on disk, read-only.

carboot was built to keep ~4.96 TB of Storacha export CARs (64k files)
retrievable after Storacha shuts down. A SQLite index maps each block's
multihash to a `(car, offset, length)`, and carboot reads block bytes straight
out of the CAR files with positional reads.

One static Go binary, `carboot`, with three subcommands:

```console
$ carboot reindex --index index.db --cars-dir /store/cars
$ carboot gateway --index index.db --cars-dir /store/cars --listen :3747 &
$ curl -s 'localhost:3747/ipfs/bafybeiamqt...ipvlrmy?format=raw' | wc -c
113
$ curl -so out.car 'localhost:3747/ipfs/bafybeiamqt...ipvlrmy?format=car&dag-scope=all'
$ wc -c < out.car      # the full UnixFS DAG; the car on disk is 100035 bytes
100035
```

- **`carboot reindex`** scans `--cars-dir` recursively for `*.car`, parses each
  CARv1 file, and writes `(multihash -> car, data offset, length)` rows into the
  SQLite index. It is incremental: a file unchanged since the last run (same
  mtime and size) is skipped, a changed file is re-indexed, a deleted file is
  pruned. Truncated CARs are indexed up to the break and flagged.
- **`carboot gateway`** is a read-only [boxo](https://github.com/ipfs/boxo)
  trustless gateway. It serves `GET /ipfs/{cid}?format=raw` and
  `?format=car&dag-scope=all` by looking the CID's multihash up in the index and
  `ReadAt`-ing the bytes from the CAR file. By default it runs a `reindex` pass
  before serving (`--reindex-on-start`, on by default), and it emits one JSON
  access-log line per request.
- **`carboot advertise`** is an
  [index-provider](https://github.com/ipni/index-provider) advertiser. It
  streams every multihash from the index into a signed IPNI advertisement chain
  announced to `cid.contact`, with the gateway's public URL as the provider
  address over the `ipfs-gateway-http` transport.

Lookups are by multihash, so the raw (`0x55`) vs dag-pb (`0x70`) codec of a CID
never matters. Index schema:

```sql
CREATE TABLE cars(id INTEGER PRIMARY KEY, path TEXT UNIQUE,
                  mtime INTEGER, size INTEGER, status TEXT, detail TEXT);
CREATE TABLE blk(mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER);
```

`mh` is the raw multihash; `off`/`len` point at the block's **data** bytes (not
the CID) within the CAR file named in `cars.path` (relative to `--cars-dir`). A
pre-existing index built by the old Python `car_index.py` (with `shards`/`shard`
naming) is migrated in place on the first `reindex` run.

`tools/car_index.py` is kept as the reference origin of the indexing logic. The
container and runtime use the Go `reindex`; the Python script is not part of the
build.

## Build

Pure Go, no cgo (the SQLite driver is `modernc.org/sqlite`), so it cross-compiles
to a static Linux binary:

```sh
go build ./...

# static linux/amd64 build for the storage host
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o carboot ./cmd/carboot
```

## Run

```sh
# build/update the index, then serve
carboot reindex --index /store/_index/index.db --cars-dir /store/cars
carboot gateway --index /store/_index/index.db --cars-dir /store/cars --listen :3747
```

```sh
curl 'http://localhost:3747/ipfs/<cid>?format=raw'
curl 'http://localhost:3747/ipfs/<cid>?format=car&dag-scope=all' -o out.car
```

The advertiser (announce the same blocks to IPNI, pointing at the gateway's
public URL):

```sh
carboot advertise \
    --index /store/_index/index.db \
    --public-addr http://gw.example.org:3747 \
    --publisher-addr http://gw.example.org:3104 \
    --listen 0.0.0.0:3104 \
    --datastore ~/.carboot/adstore \
    --identity ~/.carboot/key
```

`--public-addr` is the gateway's public URL (the content provider address that
gets advertised). `--publisher-addr` is this process's own public URL, where the
indexer pulls the advertisement chain from. It must be publicly reachable, and
is distinct from the bind address in `--listen` (which may be `0.0.0.0`).

`advertise` generates and persists an Ed25519 identity key on first run (default
`~/.carboot/key`) and stores the advertisement chain in a leveldb datastore
(default `~/.carboot/adstore`). Keep both stable across restarts, or cid.contact
treats the provider as new and loses ad-chain continuity.

### Flags and environment

gateway and reindex accept env fallbacks for container ergonomics:

| flag | env | default | meaning |
| --- | --- | --- | --- |
| `--index` | `CARBOOT_INDEX` | (required) | path to the SQLite index |
| `--cars-dir` | `CARBOOT_CARS_DIR` | (required) | directory of CAR files (scanned recursively) |
| `--listen` | | `:3747` | gateway HTTP listen address |
| `--reindex-on-start` | `CARBOOT_REINDEX_ON_START` | `true` | gateway runs a reindex pass before serving |

advertise:

| flag | default | meaning |
| --- | --- | --- |
| `--index` | (required) | path to the SQLite index |
| `--public-addr` | (required) | gateway's public URL, advertised as the content provider |
| `--publisher-addr` | (required) | this process's public URL, where the indexer fetches the ad chain |
| `--listen` | `0.0.0.0:3104` | bind address for the ad-chain HTTP server |
| `--announce-url` | `https://cid.contact/ingest/announce` | indexer announce endpoint |
| `--datastore` | `~/.carboot/adstore` | persistent ad-chain datastore path |
| `--identity` | `~/.carboot/key` | persisted Ed25519 key path |

## Container

The Dockerfile produces one static `carboot` binary on a distroless base. The
entrypoint is `carboot`; the default command is `gateway`.

```sh
podman build --platform linux/amd64 -t carboot:dev .
podman run --rm \
    -e CARBOOT_INDEX=/index/index.db \
    -e CARBOOT_CARS_DIR=/cars \
    -v /store/cars:/cars:ro \
    -v /store/_index:/index \
    -p 3747:3747 \
    --name carboot carboot:dev gateway --listen :3747
```

Mount the CAR directory read-only and the index on a writable volume so it
persists and `reindex` can write it.

The image runs as root (uid 0). Under rootless podman that maps to the host user
who started podman, so a host-owned index volume is writable as-is — no `--user`
flag or `chown`. Under Docker, or if the index is owned by some other uid, pass
`--user <owner-uid>:<owner-gid>` so the container can write the index (the
gateway needs it too: a read-only WAL reader still creates the `-shm` sidecar).

To update the index of a running gateway without restarting it, run `reindex`
as a second process in the same container:

```sh
podman exec carboot carboot reindex --index /index/index.db --cars-dir /cars
```

The gateway opens the index `mode=ro` over WAL and refreshes its car-id map
lazily on a miss, so it picks up newly-indexed CAR files on first access.

## Access log

The gateway writes one JSON line per request to stdout:

```json
{"ts":"2026-06-11T12:00:00Z","remote_addr":"10.0.0.2:51000","method":"GET","cid":"bafy...","format":"raw","status":200,"bytes":113,"cars":["sub/x.car"]}
```

`cars` is the distinct set of CAR files that request touched. A big DAG fetch is
a single line listing every CAR it spanned. Identity-CID reads and index misses
touch no CAR file, so those lines show `"cars":[]`.

## Scope

carboot is not a trusted/full gateway: it does no UnixFS file reassembly for
humans. Clients (ipfs.io via Lassie, kubo via HTTP block retrieval) reconstruct
content from the raw blocks and CARs it serves. Blocks absent from the index
(including known-bad empty/truncated CARs) simply 404.

Each block fetch is one SQLite lookup plus one `ReadAt`, so a large DAG drives
many random reads over the CAR storage. Correct, not fast: the storage-speed
question is a later optimization.

See `docs/specs/2026-06-11-carboot-productize-design.md` for the productize
design and `docs/specs/2026-06-10-carboot-design.md` for the original.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
