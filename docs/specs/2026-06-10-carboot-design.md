# carboot — design

Serve a directory of CAR shards as an IPFS trustless gateway, and advertise
their blocks to IPNI so kubo and the public gateways can find them. Built to
keep ~4.96 TB of Storacha export CARs (64k shards) retrievable after Storacha
shuts down, without re-importing the data into an IPFS blockstore.

Status: design approved 2026-06-10. Part of the Storacha Transition work.

## Why this exists

Storacha is winding down. ~64,000 CAR shards (4.96 TB) sit on a host (`fatfil`,
`/store/cars`), named `<root-cid>.shard-<N>.car`. The data needs to stay
retrievable by **kubo** and the **public IPFS gateways** (ipfs.io etc.).

Importing 4.96 TB into kubo's flatfs blockstore was rejected: the disk does
~1 MB/s of random writes (weeks), and it duplicates all the data. We proved
instead that an **HTTP trustless-gateway provider announced to IPNI** is
retrievable by kubo (HTTP block retrieval, default-on since kubo 0.36, whose
delegated router resolves to `cid.contact`) and by ipfs.io (Lassie, CAR
retrieval). No Bitswap, no duplication.

Frisbii proved that path cheaply but does not scale: its store does an O(N)
linear scan over all loaded CARs per block lookup and re-reads every CAR at
startup. Storacha's own Freeway gateway is a Cloudflare Worker coupled to R2 +
KV + an external content-claims service, and no index artifacts for these
shards exist. So we build a small, purpose-built tool.

## What it is not (scope)

- **Not** a trusted/full gateway: no UnixFS file reassembly for humans. Clients
  (ipfs.io, kubo) reconstruct from the raw blocks / CARs we serve.
- **Not** an index re-builder rewrite: the index is built by an existing
  tested Python tool (`car_index.py`), already running. carboot consumes its
  SQLite output. (A Go port of the builder is possible later for a
  single-binary story; out of scope for v1.)
- **Not** responsible for the 13 known-bad uploads (empty/truncated shards);
  those are simply absent and 404.

## Components

Two independent binaries; the SQLite index is the only contract between them.

```
  /store/cars/*.shard-N.car          (read-only, the 4.96 TB)
  /store/cars/_index/index.db        (multihash -> shard, offset, length)
        |                                   |
   ┌────┴─────────────┐            ┌────────┴─────────────┐
   │ cargw  (gateway) │            │ carpni (advertiser)  │
   └────┬─────────────┘            └────────┬─────────────┘
   GET /ipfs/{cid}                    publishes signed ad chain
   ?format=raw|car                    -> cid.contact ingests
        ▲                                      |
   kubo (raw), ipfs.io/Lassie (CAR) ◄──── discover via IPNI
```

### The index (input, produced upstream)

SQLite `index.db`, table `blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER,
len INTEGER)` and `shards(id INTEGER PRIMARY KEY, name TEXT)`. `mh` is the raw
multihash; `off`/`len` point at the block's **data** bytes within the named
shard file. Deduplicated across shards (one location per multihash). ~25M rows,
~1.5 GB. Keyed by multihash so raw-vs-dag-pb codec differences don't matter.

### cargw — the gateway

The only custom code is a read-only blockstore; `boxo/gateway` does the rest.

- **`CarIndexBlockstore`** implements boxo's `Blockstore`:
  - `Get(cid)`: `mh := cid.Hash()` → `SELECT shard,off,len FROM blk WHERE mh=?`
    → positional `ReadAt(off, len)` on the shard file → return the block.
    Miss → `format.ErrNotFound`.
  - **Open-file LRU cache** keyed by shard (cap ~256 fds), using `ReadAt`
    (thread-safe, no seek races), so we neither open/close per request nor
    exceed the fd limit across 64k shards.
  - SQLite via **`modernc.org/sqlite`** (pure Go, no cgo — static build),
    opened read-only, prepared lookup statement, `database/sql` pool.
- **Wiring:** `CarIndexBlockstore` → `blockservice` with an **offline
  exchange** → `boxo/gateway.NewBlocksBackend` → `gateway.Handler`. That gives
  `/ipfs/{cid}` with `?format=raw`, `?format=car&dag-scope=…`, UnixFS
  traversal, and response verification, all from the library.
- Listens on a plain HTTP port (v1).

### carpni — the IPNI advertiser

- Built on the **`index-provider` engine** (setup cribbed from Frisbii's
  `cmd/frisbii/main.go`).
- A **`MultihashLister`** streams **all** multihashes from the index
  (`SELECT mh FROM blk` cursor). The engine chunks them into EntryChunks,
  builds a **signed advertisement chain**, serves `/ipni/v1/ad/…`, and POSTs an
  announce to `cid.contact`.
- Advertised provider address = **cargw's public URL**, transport
  `ipfs-gateway-http`, so cid.contact maps every block multihash → our gateway.
- Needs a small persistent datastore (leveldb) for the ad chain and a stable
  Ed25519 identity key.
- Advertising **all** multihashes (not just roots) gives per-block
  discoverability — the "every CID accessible" requirement.

## Data flow (a retrieval)

1. Client resolves `<cid>` via its routing → cid.contact returns our gateway
   (because carpni advertised that multihash).
2. Client requests `GET /ipfs/<cid>?format=raw` (kubo) or
   `?format=car&dag-scope=all` (Lassie) from cargw.
3. boxo asks `CarIndexBlockstore.Get` for each needed block; we look up the
   multihash, read the bytes from the shard, return them; boxo frames the
   response and verifies.

## Error handling

- Block not in index → `ErrNotFound` → gateway 404.
- Shard file missing / read error → 502-class error, logged with the shard
  name; does not crash the server.
- Empty/truncated shards are absent from the index, so they 404 naturally.
- Advertiser persists its ad chain; a restart resumes rather than re-announcing
  from scratch.

## Performance notes / known limits

- Each block fetch = one SQLite lookup + one `ReadAt`. A CAR/dag-scope request
  drives one `Get` per block in the DAG → **many random reads** on `/store`,
  which is seek-hostile (~75 ms/seek). Large DAGs serve slowly until the data
  (or a hot cache) lives on faster storage. Correct, not fast. The SSD question
  belongs here, as a later optimization, not in v1.
- Index lookups are fast (indexed SQLite, in-memory after warmup).

## Deployment

- Two long-running binaries on fatfil (tmux or systemd), read-only over
  `index.db` + `/store/cars`.
- cargw on a public HTTP port; carpni advertises that address.
- Replaces kubo for serving these CARs. kubo can run alongside for its own pins
  / `dob-pin-fatfil`; different ports, both may announce to IPNI.
- HTTPS via the stack.fil.org caddy + a fil.org subdomain is a later hardening,
  deferred (plain HTTP already works end-to-end via IPNI + ipfs.io).

## Testing

- **Blockstore unit:** `Get` round-trips against a small index (read block,
  hash matches CID).
- **Gateway integration:** run cargw against the (partial, accumulating) index;
  `curl` a raw block and a `?format=car&dag-scope=all` DAG, verify bytes; point
  a kubo at it for real retrieval.
- **Advertiser:** announce a small subset to cid.contact, confirm it lists the
  multihashes, fetch via ipfs.io.
- **Acceptance:** one CID, served by cargw + advertised by carpni, retrieved by
  kubo (raw via IPNI) and ipfs.io (CAR via Lassie).

## Build order

1. `CarIndexBlockstore` + cargw on boxo gateway → raw + CAR working, verified
   against the real index and a kubo. (Testable now against the partial index.)
2. carpni on index-provider → advertise all multihashes, verified via
   cid.contact + ipfs.io.

## Open / deferred

- HTTPS + fil.org hostname (deferred; plain HTTP for v1).
- Faster storage for serving (random-read performance) — revisit after v1.
- Re-fetch the 13 empty-shard uploads from source (separate task).
- Possible Go port of the index builder for a single-binary distribution.
