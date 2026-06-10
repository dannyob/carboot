# carboot

> **Alpha, work in progress.** Not production-ready; expect rough edges and
> breaking changes to flags and behaviour. Current state: `cargw` (the gateway)
> is validated end-to-end against real CAR shards (serves raw blocks and
> reconstructs full DAGs as CARs). `carpni` (the IPNI advertiser) builds and is
> wired, but its live `cid.contact` ingestion has not yet been validated. No
> releases, no stability promises. Use at your own risk.

Serve a directory of CAR shards as an IPFS trustless gateway, and advertise
their blocks to IPNI so kubo and the public gateways can retrieve them, without
re-importing the data into an IPFS blockstore.

carboot was built to keep ~4.96 TB of Storacha export CARs (64k shards)
retrievable after Storacha shuts down. The shards stay on disk, read-only; a
SQLite index maps each block's multihash to a `(shard, offset, length)`, and
carboot reads block bytes straight out of the CAR files with positional reads.

Here it is serving a real shard: a single raw block, then the whole DAG
reassembled into a CAR straight from the index, byte-for-byte the original:

```console
$ cargw --index index.db --cars-dir /store/cars --listen :3747 &
$ curl -s 'localhost:3747/ipfs/bafybeiamqt...ipvlrmy?format=raw' | wc -c
113
$ curl -so out.car 'localhost:3747/ipfs/bafybeiamqt...ipvlrmy?format=car&dag-scope=all'
$ wc -c < out.car      # the full UnixFS DAG; the shard on disk is 100035 bytes
100035
```

Two binaries, with the SQLite index as the only contract between them.

`cargw` is a read-only [boxo](https://github.com/ipfs/boxo) trustless gateway. It
serves `GET /ipfs/{cid}?format=raw` and `?format=car&dag-scope=all` by looking the
CID's multihash up in the index and `ReadAt`-ing the bytes from the shard.

`carpni` is an [index-provider](https://github.com/ipni/index-provider)
advertiser. It streams every multihash from the index into a signed IPNI
advertisement chain announced to `cid.contact`, with the gateway's public URL as
the provider address over the `ipfs-gateway-http` transport.

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

`carpni` generates and persists an Ed25519 identity key on first run (default
`~/.carboot/key`) and stores the advertisement chain in a leveldb datastore
(default `~/.carboot/adstore`). Keep both stable across restarts, or cid.contact
treats the provider as new and loses ad-chain continuity.

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
| `--public-addr` | (required) | gateway's public URL, advertised as the content provider |
| `--publisher-addr` | (required) | this process's public URL, where the indexer fetches the ad chain |
| `--listen` | `0.0.0.0:3104` | bind address for the ad-chain HTTP server |
| `--announce-url` | `https://cid.contact/ingest/announce` | indexer announce endpoint |
| `--datastore` | `~/.carboot/adstore` | persistent ad-chain datastore path |
| `--identity` | `~/.carboot/key` | persisted Ed25519 key path |

## Scope

carboot is not a trusted/full gateway: it does no UnixFS file reassembly for
humans. Clients (ipfs.io via Lassie, kubo via HTTP block retrieval) reconstruct
content from the raw blocks and CARs it serves. Blocks absent from the index
(including the known-bad empty/truncated shards) simply 404.

Each block fetch is one SQLite lookup plus one `ReadAt`, so a large DAG drives
many random reads over the shard storage. Correct, not fast: the storage-speed
question is a later optimization.

See `docs/specs/2026-06-10-carboot-design.md` for the full design.

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
