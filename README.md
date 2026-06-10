# carboot

Serve a directory of CAR shards as an IPFS trustless gateway, and advertise
their blocks to IPNI so kubo and the public gateways can retrieve them — without
re-importing the data into an IPFS blockstore.

carboot was built to keep ~4.96 TB of Storacha export CARs (64k shards)
retrievable after Storacha shuts down. The shards stay on disk, read-only; a
SQLite index maps each block's multihash to a `(shard, offset, length)`, and
carboot reads block bytes straight out of the CAR files with positional reads.

Two binaries, with the SQLite index as the only contract between them:

- **cargw** — a read-only [boxo](https://github.com/ipfs/boxo) trustless gateway.
  Serves `GET /ipfs/{cid}?format=raw` and `?format=car&dag-scope=…` by looking
  the CID's multihash up in the index and `ReadAt`-ing the bytes from the shard.
- **carpni** — an [index-provider](https://github.com/ipni/index-provider)
  advertiser. Streams every multihash from the index into a signed IPNI
  advertisement chain announced to `cid.contact`, with the gateway's public URL
  as the provider address over the `ipfs-gateway-http` transport.

The index itself is produced upstream by a separate Python tool (`car_index.py`)
and is not part of carboot. Its merged schema:

```sql
CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER);
CREATE TABLE shards(id INTEGER PRIMARY KEY, name TEXT);
```

`mh` is the raw multihash; `off`/`len` point at the block's **data** bytes (not
the CID) within the shard file named in `shards.name`. Lookups are by multihash,
so the raw (`0x55`) vs dag-pb (`0x70`) codec of a CID never matters.

## Build

Pure Go, no cgo (the SQLite driver is `modernc.org/sqlite`), so it cross-compiles
to a static Linux binary:

```sh
go build ./...

# static linux/amd64 build for the storage host
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cargw  ./cmd/cargw
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o carpni ./cmd/carpni
```

## Run

Both binaries are long-running and read-only over the index and the CAR
directory.

The gateway:

```sh
cargw --index /store/cars/_index/index.db \
      --cars-dir /store/cars \
      --listen :3747
```

```sh
curl 'http://localhost:3747/ipfs/<cid>?format=raw'
curl 'http://localhost:3747/ipfs/<cid>?format=car&dag-scope=all' -o out.car
```

The advertiser (announce the same blocks to IPNI, pointing at the gateway's
public URL):

```sh
carpni --index /store/cars/_index/index.db \
       --public-addr https://gw.example.org \
       --listen 0.0.0.0:3104 \
       --announce-url https://cid.contact/ingest/announce \
       --identity ~/.carboot/key
```

`carpni` generates and persists an Ed25519 identity key on first run (default
`~/.carboot/key`). Keep it stable across restarts, or cid.contact treats the
provider as new and loses the ad-chain continuity.

### Flags

cargw:

| flag | default | meaning |
| --- | --- | --- |
| `--index` | (required) | path to the SQLite index |
| `--cars-dir` | (required) | directory of CAR shard files |
| `--listen` | `:3747` | HTTP listen address |

carpni:

| flag | default | meaning |
| --- | --- | --- |
| `--index` | (required) | path to the SQLite index |
| `--public-addr` | (required) | gateway's public URL, advertised to IPNI |
| `--listen` | `0.0.0.0:3104` | where the ad-chain handler is served |
| `--announce-url` | `https://cid.contact/ingest/announce` | indexer announce endpoint |
| `--identity` | `~/.carboot/key` | persisted Ed25519 key path |

## Scope

carboot is not a trusted/full gateway: it does no UnixFS file reassembly for
humans. Clients (ipfs.io via Lassie, kubo via HTTP block retrieval) reconstruct
content from the raw blocks and CARs it serves. Blocks absent from the index —
including the known-bad empty/truncated shards — simply 404.

Each block fetch is one SQLite lookup plus one `ReadAt`, so a large DAG drives
many random reads over the shard storage. Correct, not fast: the storage-speed
question is a later optimization.

See `docs/specs/2026-06-10-carboot-design.md` for the full design.

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
