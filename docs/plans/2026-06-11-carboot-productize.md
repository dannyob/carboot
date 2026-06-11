# carboot Productize Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn carboot into one self-contained Go binary (`carboot {gateway,reindex,advertise}`) that indexes a directory of CAR files (recursively, with mtime/size sync), serves them, logs per-request CAR access, and renames the Storacha-ism "shard" to "car".

**Architecture:** Move logic out of `package main` into testable `internal/` packages. A shared `carindex` schema/open layer (cars + blk tables, with old-schema migration and a `mode=ro` vs read-write open). A new `reindex` package ports the Python `car_index.py` (streaming CARv1 parse → `multihash → (car, off, len)`), with incremental mtime/size sync. The gateway gains a context-scoped access-log collector and `--reindex-on-start`. A single `cmd/carboot` dispatches subcommands.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (pure-Go), `boxo/gateway`, `index-provider`, `multiformats/go-multihash`. Build/test on the Mac (`go test ./...`), cross-compile `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`.

**Working dir:** `/Users/danny/Public/src/carboot`. The spec is `docs/specs/2026-06-11-carboot-productize-design.md`; read it first.

---

## File structure (end state)

```
cmd/carboot/main.go            # subcommand dispatch + per-subcommand flags
internal/carindex/schema.go    # schema, Open (ro/rw), migrateShardToCar
internal/carindex/blockstore.go# CarIndexBlockstore (car-named, mode=ro, lazy carMap refresh, access hook)
internal/carindex/access.go    # request-scoped access collector (context)
internal/reindex/car.go        # streaming CARv1 reader: iterate (mh, dataOff, len)
internal/reindex/reindex.go    # recursive scan + mtime/size sync into the index
internal/gateway/gateway.go    # Handler() wiring (blockstore->offline->boxo gateway) + access-log middleware + Serve
internal/advertise/advertise.go# IPNI advertiser (today's carpni, car-renamed)
tools/car_index.py             # reference Python builder (moved from upstream)
Dockerfile                     # builds one carboot binary
```
Delete `cmd/cargw/` and `cmd/carpni/` (logic moves to internal/ + cmd/carboot).

---

## Task 1: Shared schema/open layer with migration

**Files:**
- Create: `internal/carindex/schema.go`
- Create: `internal/carindex/schema_test.go`

The index schema, keyed by multihash, mapping to a CAR file. Renames `shards`→`cars`, `blk.shard`→`blk.car`, adds `cars.mtime`,`cars.size`. Migrates an existing old-schema DB in place.

```go
// internal/carindex/schema.go
package carindex

import (
	"database/sql"
	"fmt"
)

// Schema (current): keyed by multihash.
//   cars(id INTEGER PRIMARY KEY, path TEXT UNIQUE, mtime INTEGER, size INTEGER, status TEXT, detail TEXT)
//   blk(mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER)
const createSchema = `
CREATE TABLE IF NOT EXISTS cars(
  id INTEGER PRIMARY KEY, path TEXT UNIQUE,
  mtime INTEGER, size INTEGER, status TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS blk(
  mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER);
`

// OpenRW opens the index read-write (for reindex), creating/migrating schema.
func OpenRW(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(createSchema); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// OpenRO opens the index read-only but WAL-aware (mode=ro, NOT immutable), so a
// gateway sees a concurrent reindexer's committed writes.
func OpenRO(path string) (*sql.DB, error) {
	return sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
}

// migrate upgrades a legacy {shards, blk.shard} index to {cars, blk.car} in
// place: rename table/column, add mtime/size. No block-data is touched.
func migrate(db *sql.DB) error {
	var hasShards int
	db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='shards'`).Scan(&hasShards)
	if hasShards == 0 {
		return nil
	}
	var hasCars int
	db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='cars'`).Scan(&hasCars)
	if hasCars > 0 {
		return nil // both exist: a prior partial build; leave cars as the live table
	}
	stmts := []string{
		`ALTER TABLE shards RENAME TO cars`,
		`ALTER TABLE cars RENAME COLUMN name TO path`,
		`ALTER TABLE blk RENAME COLUMN shard TO car`,
		`ALTER TABLE cars ADD COLUMN mtime INTEGER`,
		`ALTER TABLE cars ADD COLUMN size INTEGER`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("carindex: migrate %q: %w", s, err)
		}
	}
	return nil
}
```

- [ ] **Step 1: Write failing test** — `internal/carindex/schema_test.go`

```go
package carindex

import (
	"path/filepath"
	"testing"
)

func TestOpenRWCreatesSchema(t *testing.T) {
	p := filepath.Join(t.TempDir(), "i.db")
	db, err := OpenRW(p)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	for _, tbl := range []string{"cars", "blk"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&n); err != nil || n != 1 {
			t.Fatalf("table %s missing (n=%d err=%v)", tbl, n, err)
		}
	}
}

func TestMigrateLegacySchema(t *testing.T) {
	p := filepath.Join(t.TempDir(), "old.db")
	// build a legacy index: shards(id,name), blk(mh,shard,off,len)
	raw, err := openRaw(p) // helper below opens without migration
	if err != nil { t.Fatal(err) }
	raw.Exec(`CREATE TABLE shards(id INTEGER PRIMARY KEY, name TEXT)`)
	raw.Exec(`CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER)`)
	raw.Exec(`INSERT INTO shards VALUES(1,'x.car')`)
	raw.Exec(`INSERT INTO blk VALUES(x'1220aa', 1, 98, 10)`)
	raw.Close()

	db, err := OpenRW(p) // triggers migration
	if err != nil { t.Fatal(err) }
	defer db.Close()
	var path string
	if err := db.QueryRow(`SELECT path FROM cars WHERE id=1`).Scan(&path); err != nil || path != "x.car" {
		t.Fatalf("migrated cars.path = %q err=%v", path, err)
	}
	var car int
	if err := db.QueryRow(`SELECT car FROM blk LIMIT 1`).Scan(&car); err != nil || car != 1 {
		t.Fatalf("migrated blk.car = %d err=%v", car, err)
	}
	// mtime/size columns exist (nullable, unpopulated until reindex stat pass)
	if _, err := db.Exec(`SELECT mtime,size FROM cars`); err != nil {
		t.Fatalf("mtime/size columns missing: %v", err)
	}
}
```

Add a tiny test helper in `schema_test.go`:
```go
import "database/sql"
func openRaw(path string) (*sql.DB, error) { return sql.Open("sqlite", "file:"+path) }
```
(import `_ "modernc.org/sqlite"` is already pulled in via the package; if the test file needs it, add the blank import.)

- [ ] **Step 2: Run, verify it fails** — `go test ./internal/carindex/ -run 'OpenRW|Migrate' -v` → FAIL (undefined `OpenRW`).
- [ ] **Step 3: Write `internal/carindex/schema.go`** (code above). Ensure `import _ "modernc.org/sqlite"` exists in the package (it's currently in blockstore.go — fine).
- [ ] **Step 4: Run, verify pass** — `go test ./internal/carindex/ -run 'OpenRW|Migrate' -v` → PASS.
- [ ] **Step 5: Commit** — `git add internal/carindex/schema.go internal/carindex/schema_test.go && git commit -m "carindex: shared schema/open with cars terminology + legacy migration"`

---

## Task 2: Rename shard→car in the blockstore, open mode=ro, lazy carMap refresh

**Files:**
- Modify: `internal/carindex/blockstore.go`
- Modify: `internal/carindex/blockstore_test.go`

Changes:
1. Rename everywhere: `shardMap`→`carMap`, `shardPath`→`carPath`, `lookup` returns `car` (col `car`), SQL uses `blk`/`car`/`cars`/`path`.
2. `Open(indexPath, carsDir)` now calls `OpenRO(indexPath)` (was `immutable=1`).
3. The car-id→path map (`carMap`) is loaded under a mutex and **reloaded on a miss**: if `carPath(id)` finds no entry, reload `carMap` from the DB once and retry; only then error.
4. The shards/cars SELECT for the map: `SELECT id, path FROM cars`.

Implementation of the lazy refresh (replace the current `shardPath`):
```go
// carPath resolves a car id to its on-disk path, reloading the map once on a
// miss so a live reindexer's newly-added cars are picked up without a restart.
func (cb *CarIndexBlockstore) carPath(car int64) (string, error) {
	cb.mapMu.RLock()
	name, ok := cb.carMap[car]
	cb.mapMu.RUnlock()
	if !ok {
		if err := cb.reloadCarMap(); err != nil {
			return "", err
		}
		cb.mapMu.RLock()
		name, ok = cb.carMap[car]
		cb.mapMu.RUnlock()
	}
	if !ok {
		return "", fmt.Errorf("carindex: unknown car id %d", car)
	}
	return filepath.Join(cb.carsDir, name), nil
}

func (cb *CarIndexBlockstore) reloadCarMap() error {
	m, err := loadCarMap(cb.db) // SELECT id,path FROM cars
	if err != nil { return err }
	cb.mapMu.Lock()
	cb.carMap = m
	cb.mapMu.Unlock()
	return nil
}
```
Add fields `carMap map[int64]string` and `mapMu sync.RWMutex` to the struct; build `loadCarMap` from the existing `loadShardMap` (renamed, querying `cars`).

- [ ] **Step 1: Update existing tests** — in `blockstore_test.go`, change the schema built by `buildIndex` to `cars(id,path,mtime,size,status,detail)` + `blk(mh,car,off,len)`, and inserts accordingly. The existing `TestGetRoundTrip`, `TestHasAndGetSize`, `TestMissReturnsNotFound`, `TestReadOnlyMethods`, `TestGatewayServesRaw/CAR`, `TestIdentityCID` should still pass after the rename.
- [ ] **Step 2: Add a failing lazy-refresh test:**

```go
func TestLazyCarMapRefresh(t *testing.T) {
	bs, _ := setup(t) // opens the blockstore over an existing index
	ctx := context.Background()
	// simulate a live reindex: a writer adds a new car + block to the SAME db.
	db, err := OpenRW(bs.indexPath()) // add an accessor returning the db path
	if err != nil { t.Fatal(err) }
	// write a new car file + matching index row; reuse writeCARv1/buildIndex helpers
	dir := bs.carsDir() // add accessor
	recs := writeCARv1(t, filepath.Join(dir, "new.car"), [][]byte{[]byte("freshly added block")})
	db.Exec(`INSERT INTO cars(path) VALUES('new.car')`)
	var carID int64
	db.QueryRow(`SELECT id FROM cars WHERE path='new.car'`).Scan(&carID)
	db.Exec(`INSERT INTO blk(mh,car,off,len) VALUES(?,?,?,?)`, []byte(recs[0].cid.Hash()), carID, recs[0].off, recs[0].len)
	db.Close()
	// gateway (opened before the write) must serve the new block via lazy refresh
	blk, err := bs.Get(ctx, recs[0].cid)
	if err != nil { t.Fatalf("Get after live reindex: %v", err) }
	if !bytes.Equal(blk.RawData(), recs[0].data) { t.Error("data mismatch after refresh") }
}
```
Add unexported accessors `indexPath()`/`carsDir()` to the blockstore (return stored fields) for the test.

- [ ] **Step 3: Run, verify the lazy-refresh test fails** — `go test ./internal/carindex/ -run TestLazyCarMapRefresh -v` → FAIL (blockstore opened immutable, never sees the new row; or unknown car id).
- [ ] **Step 4: Implement** the rename + `OpenRO` + `carPath`/`reloadCarMap` (above). Note `Get`/`Has`/`GetSize` keep the identity-CID short-circuit from the current code.
- [ ] **Step 5: Run all carindex tests** — `go test ./internal/carindex/ -count=1 -v` → all PASS.
- [ ] **Step 6: Commit** — `git commit -am "carindex: rename shard->car, open mode=ro, lazy car-map refresh for live reindex"`

---

## Task 3: Streaming CARv1 reader (Go)

**Files:**
- Create: `internal/reindex/car.go`
- Create: `internal/reindex/car_test.go`

Port the CARv1 iteration from `car_index.py`. Yields each block's multihash and its **data** location.

```go
// internal/reindex/car.go
package reindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/multiformats/go-multihash"
)

// Block is one CAR block's index entry: where its DATA bytes live in the file.
type Block struct {
	MH       multihash.Multihash
	DataOff  int64
	DataLen  int64
}

// ErrTruncated is returned (with the count of blocks read so far) when a CAR
// ends mid-frame; the blocks read before the break are still valid.
type ErrTruncated struct{ N int; Msg string }
func (e *ErrTruncated) Error() string { return fmt.Sprintf("truncated after %d blocks: %s", e.N, e.Msg) }

// ScanCar streams a CARv1 file, calling fn for each block. fn must copy MH if it
// retains it. Returns *ErrTruncated for a truncated file (after delivering the
// good blocks), nil on clean EOF.
func ScanCar(path string, fn func(Block) error) error {
	f, err := os.Open(path)
	if err != nil { return err }
	defer f.Close()
	fi, _ := f.Stat()
	size := fi.Size()
	r := bufio.NewReaderSize(f, 1<<20)

	// header: uvarint length + CBOR header bytes; we only need to skip it.
	hlen, err := binary.ReadUvarint(r)
	if err != nil { return fmt.Errorf("read header len: %w", err) }
	off := int64(uvarintLen(hlen)) + int64(hlen)
	if _, err := r.Discard(int(hlen)); err != nil { return &ErrTruncated{0, "header past EOF"} }

	n := 0
	for {
		blen, err := binary.ReadUvarint(r)
		if err == io.EOF { return nil }
		if err != nil { return &ErrTruncated{n, "section length"} }
		secStart := off + int64(uvarintLen(blen))
		if secStart+int64(blen) > size { return &ErrTruncated{n, "section past EOF"} }
		// parse CID prefix to learn its byte length, then mh.
		mh, cidLen, err := readCID(r)
		if err != nil { return &ErrTruncated{n, "cid: " + err.Error()} }
		dataOff := secStart + int64(cidLen)
		dataLen := int64(blen) - int64(cidLen)
		if err := fn(Block{MH: mh, DataOff: dataOff, DataLen: dataLen}); err != nil { return err }
		// advance past the data (we already consumed the cid via readCID)
		if _, err := r.Discard(int(dataLen)); err != nil { return &ErrTruncated{n, "data past EOF"} }
		off = secStart + int64(blen)
		n++
	}
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 { x >>= 7; n++ }
	return n
}

// readCID reads a binary CID from r, returning its multihash and total CID byte
// length. Handles CIDv0 (0x12 0x20 + 32) and CIDv1 (<ver><codec><mhcode><mhlen><digest>).
func readCID(r *bufio.Reader) (multihash.Multihash, int, error) {
	b0, err := r.ReadByte(); if err != nil { return nil, 0, err }
	if b0 == 0x12 { // CIDv0: 0x12 0x20 + 32-byte digest
		b1, err := r.ReadByte(); if err != nil { return nil, 0, err }
		if b1 != 0x20 { return nil, 0, fmt.Errorf("bad cidv0") }
		dig := make([]byte, 32)
		if _, err := io.ReadFull(r, dig); err != nil { return nil, 0, err }
		mh, _ := multihash.Encode(dig, multihash.SHA2_256)
		return mh, 34, nil
	}
	// CIDv1: b0 is the version varint's first byte (0x01). codec, then multihash.
	// Read codec varint.
	if b0 != 0x01 { return nil, 0, fmt.Errorf("unsupported cid version byte 0x%02x", b0) }
	consumed := 1
	codec, cn, err := readUvarintCounted(r); if err != nil { return nil, 0, err }; _ = codec; consumed += cn
	mhcode, mn, err := readUvarintCounted(r); if err != nil { return nil, 0, err }; consumed += mn
	mhlen, ln, err := readUvarintCounted(r); if err != nil { return nil, 0, err }; consumed += ln
	dig := make([]byte, mhlen)
	if _, err := io.ReadFull(r, dig); err != nil { return nil, 0, err }
	consumed += int(mhlen)
	mh, err := multihash.Encode(dig, mhcode)
	if err != nil { return nil, 0, err }
	return mh, consumed, nil
}

func readUvarintCounted(r *bufio.Reader) (uint64, int, error) {
	var x uint64; var s uint; n := 0
	for {
		b, err := r.ReadByte(); if err != nil { return 0, 0, err }
		n++
		if b < 0x80 { return x | uint64(b)<<s, n, nil }
		x |= uint64(b&0x7f) << s; s += 7
		if s > 63 { return 0, 0, fmt.Errorf("varint too long") }
	}
}
```

- [ ] **Step 1: Write failing test** — `internal/reindex/car_test.go`. Build a CARv1 in memory (helper mirroring the blockstore test's `writeCARv1`: uvarint header + per-block `uvarint(len(cid+data))|cid|data` with raw CIDv1), then assert `ScanCar` yields the right MHs and that data at `(DataOff, DataLen)` re-hashes to each MH. Also test a truncated copy returns `*ErrTruncated` after the good blocks.

```go
func TestScanCarRoundTrip(t *testing.T) {
	dir := t.TempDir(); p := filepath.Join(dir, "x.car")
	datas := [][]byte{[]byte("alpha block here"), []byte("second"), bytes.Repeat([]byte{7}, 500)}
	writeTestCAR(t, p, datas) // helper builds CARv1 with raw CIDv1 blocks
	raw, _ := os.ReadFile(p)
	var got [][]byte
	err := ScanCar(p, func(b Block) error {
		// verify the recorded location and hash
		sum := sha256.Sum256(raw[b.DataOff : b.DataOff+b.DataLen])
		mh, _ := multihash.Encode(sum[:], multihash.SHA2_256)
		if !bytes.Equal(mh, b.MH) { t.Errorf("mh mismatch at off %d", b.DataOff) }
		got = append(got, raw[b.DataOff:b.DataOff+b.DataLen])
		return nil
	})
	if err != nil { t.Fatal(err) }
	if len(got) != len(datas) { t.Fatalf("got %d blocks want %d", len(got), len(datas)) }
}

func TestScanCarTruncated(t *testing.T) {
	dir := t.TempDir(); p := filepath.Join(dir, "t.car")
	writeTestCAR(t, p, [][]byte{[]byte("aaa"), []byte("bbb")})
	full, _ := os.ReadFile(p)
	os.WriteFile(p, full[:len(full)-2], 0644) // chop last 2 bytes
	n := 0
	err := ScanCar(p, func(Block) error { n++; return nil })
	var trunc *ErrTruncated
	if !errors.As(err, &trunc) { t.Fatalf("want ErrTruncated, got %v", err) }
}
```
Write `writeTestCAR` in the test file (copy the CARv1-builder logic from `internal/carindex/blockstore_test.go:writeCARv1`, adapted to raw CIDv1).

- [ ] **Step 2: Run, verify fail** — `go test ./internal/reindex/ -run TestScanCar -v` → FAIL (no package).
- [ ] **Step 3: Write `internal/reindex/car.go`** (code above).
- [ ] **Step 4: Run, verify pass** — `go test ./internal/reindex/ -run TestScanCar -v` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "reindex: streaming CARv1 reader yielding (mh, data offset, len)"`

---

## Task 4: Recursive reindex with mtime/size sync

**Files:**
- Create: `internal/reindex/reindex.go`
- Create: `internal/reindex/reindex_test.go`

```go
// internal/reindex/reindex.go
package reindex

import (
	"database/sql"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dannyob/carboot/internal/carindex"
)

// Run walks carsDir recursively, syncing the index at dbPath: new files indexed,
// changed files (mtime or size) re-indexed, removed files pruned.
func Run(dbPath, carsDir string) error {
	db, err := carindex.OpenRW(dbPath)
	if err != nil { return err }
	defer db.Close()

	onDisk, err := scanDir(carsDir) // relpath -> (mtime,size)
	if err != nil { return err }
	inDB, err := loadCars(db) // relpath -> (id,mtime,size)
	if err != nil { return err }

	for rel, fi := range onDisk {
		cur, exists := inDB[rel]
		if exists && cur.mtime == fi.mtime && cur.size == fi.size {
			continue // unchanged
		}
		if exists {
			if _, err := db.Exec(`DELETE FROM blk WHERE car=?`, cur.id); err != nil { return err }
		}
		if err := indexOne(db, carsDir, rel, fi.mtime, fi.size); err != nil {
			log.Printf("reindex: %s: %v", rel, err)
		}
	}
	// prune removed
	for rel, cur := range inDB {
		if _, ok := onDisk[rel]; !ok {
			db.Exec(`DELETE FROM blk WHERE car=?`, cur.id)
			db.Exec(`DELETE FROM cars WHERE id=?`, cur.id)
		}
	}
	return nil
}

type finfo struct{ mtime, size int64 }
type carrow struct{ id, mtime, size int64 }

func scanDir(root string) (map[string]finfo, error) {
	out := map[string]finfo{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil { return err }
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".car") { return nil }
		info, err := d.Info(); if err != nil { return err }
		rel, _ := filepath.Rel(root, p)
		out[rel] = finfo{mtime: info.ModTime().Unix(), size: info.Size()}
		return nil
	})
	return out, err
}

func loadCars(db *sql.DB) (map[string]carrow, error) {
	rows, err := db.Query(`SELECT id, path, COALESCE(mtime,-1), COALESCE(size,-1) FROM cars`)
	if err != nil { return nil, err }
	defer rows.Close()
	out := map[string]carrow{}
	for rows.Next() {
		var r carrow; var path string
		if err := rows.Scan(&r.id, &path, &r.mtime, &r.size); err != nil { return nil, err }
		out[path] = r
	}
	return out, rows.Err()
}

// indexOne (re)indexes a single car: upsert the cars row, then insert its blocks.
func indexOne(db *sql.DB, carsDir, rel string, mtime, size int64) error {
	res, err := db.Exec(`INSERT INTO cars(path,mtime,size,status) VALUES(?,?,?,'indexing')
	                     ON CONFLICT(path) DO UPDATE SET mtime=excluded.mtime,size=excluded.size,status='indexing'`,
		rel, mtime, size)
	_ = res
	if err != nil { return err }
	var carID int64
	if err := db.QueryRow(`SELECT id FROM cars WHERE path=?`, rel).Scan(&carID); err != nil { return err }

	tx, err := db.Begin(); if err != nil { return err }
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO blk(mh,car,off,len) VALUES(?,?,?,?)`)
	if err != nil { tx.Rollback(); return err }
	status, detail := "done", ""
	scanErr := ScanCar(filepath.Join(carsDir, rel), func(b Block) error {
		_, e := stmt.Exec([]byte(b.MH), carID, b.DataOff, b.DataLen)
		return e
	})
	if te, ok := scanErr.(*ErrTruncated); ok {
		status, detail = "truncated", te.Error()
	} else if scanErr != nil {
		status, detail = "error", scanErr.Error()
	}
	if err := tx.Commit(); err != nil { return err }
	_, _ = db.Exec(`UPDATE cars SET status=?, detail=? WHERE id=?`, status, detail, carID)
	return nil
}
```
(Empty/0-byte `.car` files: `ScanCar` returns an error reading the header → recorded as status `error`, no blocks — matching today's behavior. That's acceptable.)

- [ ] **Step 1: Write failing test** — `internal/reindex/reindex_test.go`: create `root/a.car` and `root/sub/b.car` (use `writeTestCAR`). Run `Run(db, root)`. Assert: both cars present (recursive), blk rows for both, `cars.mtime/size` populated, and a round-trip (block at recorded off re-hashes). Then: (a) add `root/c.car`, re-Run, assert c indexed and a/b untouched; (b) `os.Chtimes(a.car)` + rewrite it bigger, re-Run, assert a re-indexed; (c) delete `b.car`, re-Run, assert b's rows pruned.
- [ ] **Step 2: Run, verify fail** — `go test ./internal/reindex/ -run TestReindex -v` → FAIL.
- [ ] **Step 3: Write `internal/reindex/reindex.go`** (code above).
- [ ] **Step 4: Run, verify pass** — `go test ./internal/reindex/ -count=1 -v` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "reindex: recursive scan with mtime/size add/change/prune sync"`

---

## Task 5: Access-log collector (context)

**Files:**
- Create: `internal/carindex/access.go`
- Create: `internal/carindex/access_test.go`
- Modify: `internal/carindex/blockstore.go` (record reads into the collector)

```go
// internal/carindex/access.go
package carindex

import (
	"context"
	"sort"
	"sync"
)

type ctxKey struct{}

// AccessCollector accumulates, per request, the distinct car files read and the
// total bytes served. Safe for concurrent block reads within one request.
type AccessCollector struct {
	mu    sync.Mutex
	cars  map[string]struct{}
	bytes int64
}

func NewAccessCollector() *AccessCollector { return &AccessCollector{cars: map[string]struct{}{}} }

// WithCollector returns a context carrying c, for the gateway middleware.
func WithCollector(ctx context.Context, c *AccessCollector) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}
func collectorFrom(ctx context.Context) *AccessCollector {
	c, _ := ctx.Value(ctxKey{}).(*AccessCollector)
	return c
}

func (c *AccessCollector) record(carPath string, n int64) {
	c.mu.Lock(); defer c.mu.Unlock()
	c.cars[carPath] = struct{}{}
	c.bytes += n
}

// Cars returns the sorted distinct car paths read. Bytes returns total bytes.
func (c *AccessCollector) Cars() []string {
	c.mu.Lock(); defer c.mu.Unlock()
	out := make([]string, 0, len(c.cars))
	for k := range c.cars { out = append(out, k) }
	sort.Strings(out)
	return out
}
func (c *AccessCollector) Bytes() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.bytes }
```

In `blockstore.go` `Get`, after a successful shard read (not for identity CIDs, not on miss), record into the collector. The car path is what `carPath(car)` returned; record the relative name (strip `carsDir`) and `length`:
```go
// inside Get, after a successful ReadAt and before returning blk:
if c := collectorFrom(ctx); c != nil {
	rel, _ := filepath.Rel(cb.carsDir, path) // path is the car file path
	c.record(rel, length)
}
```

- [ ] **Step 1: Write failing test** — `access_test.go`: a collector records two distinct cars + bytes; `Cars()` is sorted-distinct, `Bytes()` sums. (Pure unit, no DB.)
- [ ] **Step 2: Run, verify fail** → FAIL (no package symbols).
- [ ] **Step 3: Write `access.go`**; wire `record` into `Get`.
- [ ] **Step 4: Run** — `go test ./internal/carindex/ -count=1` → PASS (existing blockstore tests still green; identity/miss don't record).
- [ ] **Step 5: Commit** — `git commit -am "carindex: per-request access collector (cars touched, bytes)"`

---

## Task 6: Gateway package + access-log middleware + --reindex-on-start

**Files:**
- Create: `internal/gateway/gateway.go`
- Create: `internal/gateway/gateway_test.go`
- (logic moves out of `cmd/cargw/main.go`)

`gateway.go` exposes:
```go
// Handler builds the boxo trustless-gateway http.Handler over the index, wrapped
// in the access-log middleware (one JSON line per request to logw).
func Handler(bs blockstore.Blockstore, logw io.Writer) (http.Handler, error)

// Serve opens the index (OpenRO), optionally runs reindex first, and serves until ctx done.
func Serve(ctx context.Context, opt Options) error
type Options struct {
	Index, CarsDir, Listen string
	ReindexOnStart bool
	LogOut io.Writer
}
```
- `Handler`: the current cargw wiring (offline exchange → blockservice → `gateway.NewBlocksBackend` → `gateway.NewHandler`, Config `{DeserializedResponses:false, NoDNSLink:true}`), wrapped by middleware that: creates an `AccessCollector`, puts it in the request context via `carindex.WithCollector`, captures status+bytes with a `responseWriter` wrapper, then writes one JSON line: `{ts, remote_addr, method, cid, format, status, bytes, cars}`. Extract `cid` from the path (`/ipfs/{cid}/…`) and `format` from the query.
- `Serve`: if `opt.ReindexOnStart`, call `reindex.Run(opt.Index, opt.CarsDir)` before opening; then `carindex.Open(opt.Index, opt.CarsDir)`, build `Handler`, run an `http.Server` with graceful shutdown on ctx.

- [ ] **Step 1: Write failing test** — `gateway_test.go`: build a tiny index+car (reuse helpers), build `Handler(bs, &buf)`, drive it with `httptest` for `/ipfs/<cid>?format=raw`. Assert 200 + body, and that `buf` got one JSON line whose `cars` array contains the car file and `bytes` == block size; assert a miss logs `cars:[]` and status 404; assert `/ipfs/bafkqaaa?format=raw` (identity) is 200 with `cars:[]`.
- [ ] **Step 2: Run, verify fail** → FAIL (no package).
- [ ] **Step 3: Implement `gateway.go`.** Move the wiring from `cmd/cargw/main.go`. The middleware JSON struct:
```go
type accessLine struct {
	TS string `json:"ts"`; Remote string `json:"remote_addr"`; Method string `json:"method"`
	CID string `json:"cid"`; Format string `json:"format"`; Status int `json:"status"`
	Bytes int64 `json:"bytes"`; Cars []string `json:"cars"`
}
```
(Use `time.Now().UTC().Format(time.RFC3339)` for ts — note `time.Now()` is fine in real runtime; only the workflow-script sandbox forbids it.)
- [ ] **Step 4: Run** — `go test ./internal/gateway/ -count=1 -v` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "gateway: package with access-log middleware and reindex-on-start"`

---

## Task 7: Advertise package

**Files:**
- Create: `internal/advertise/advertise.go`
- (logic moves out of `cmd/carpni/main.go`, incl. its `main_test.go` iterator test → `advertise_test.go`)

Move `cmd/carpni/main.go` logic into `advertise.Run(ctx, Options)` verbatim except: the SQLite read query becomes `SELECT mh FROM blk` (already), and any "shard" wording → "car". Keep the engine setup (WithHttpPublisherWithoutServer + GetPublisherHttpFunc mounted on an http mux at `/ipni/`, empty handler-path, leveldb datastore, ed25519 identity, NotifyPut). Move the `sqliteMHIterator` test to `advertise_test.go`.

- [ ] **Step 1: Move** `cmd/carpni/main.go` → `internal/advertise/advertise.go` (package `advertise`, exported `Run(ctx, Options)` + `Options` struct of the flags). Move `main_test.go` → `advertise_test.go`, package `advertise`.
- [ ] **Step 2: Run** — `go test ./internal/advertise/ -count=1 -v` → the iterator test PASS.
- [ ] **Step 3: Commit** — `git commit -am "advertise: package (moved from cmd/carpni)"`

---

## Task 8: Unified cmd/carboot dispatch; delete old cmds

**Files:**
- Create: `cmd/carboot/main.go`
- Delete: `cmd/cargw/`, `cmd/carpni/`

`main.go`: `switch os.Args[1] { case "gateway": …; case "reindex": …; case "advertise": …; default: usage }`. Each branch parses its own `flag.FlagSet` (gateway: `--index`,`--cars-dir`,`--listen`,`--reindex-on-start` + env fallbacks `CARBOOT_INDEX`/`CARBOOT_CARS_DIR`; reindex: `--index`,`--cars-dir`; advertise: existing carpni flags) and calls the package `Run`/`Serve`. Identity comment headers stay BSD-3-Clause.

- [ ] **Step 1: Write `cmd/carboot/main.go`.**
- [ ] **Step 2: Delete** `cmd/cargw`, `cmd/carpni`. `go build ./...` → builds. `go vet ./...` clean.
- [ ] **Step 3: End-to-end smoke (real binary, per CLAUDE.md):**
```bash
go build -o /tmp/carboot ./cmd/carboot
mkdir -p /tmp/cb/cars/sub
# write a couple of tiny CARv1 files into /tmp/cb/cars and /tmp/cb/cars/sub (use a small go/py helper or the test fixtures)
/tmp/carboot reindex --index /tmp/cb/i.db --cars-dir /tmp/cb/cars
/tmp/carboot gateway --index /tmp/cb/i.db --cars-dir /tmp/cb/cars --listen 127.0.0.1:3799 --reindex-on-start=false &
curl -s 'http://127.0.0.1:3799/ipfs/<known-cid>?format=raw' | wc -c   # expect block bytes
curl -s 'http://127.0.0.1:3799/ipfs/bafkqaaa?format=raw' -o /dev/null -w '%{http_code}\n'  # expect 200
# observe one JSON access line per request on the gateway's stdout
```
Verify `go test ./... -count=1` and `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` both pass.
- [ ] **Step 4: Commit** — `git commit -am "cmd/carboot: single binary with gateway/reindex/advertise subcommands; remove cargw/carpni"`

---

## Task 9: Dockerfile + tools + docs

**Files:**
- Modify: `Dockerfile`
- Create: `tools/car_index.py` (copy the existing Python builder here, as reference)
- Modify: `README.md` (subcommands, env vars, `podman exec … reindex`, access-log format)

- [ ] **Step 1: Dockerfile** — build the one `carboot` binary; `COPY --from=build /out/carboot /usr/local/bin/`; `ENTRYPOINT ["/usr/local/bin/carboot"]`; `CMD ["gateway"]`. Drop the two-binary build.
- [ ] **Step 2: `tools/car_index.py`** — `cp ~/tmp/car_index.py tools/car_index.py` (the reference Python builder). Add a one-line note at its top that the Go `carboot reindex` is the runtime indexer and this is kept for reference.
- [ ] **Step 3: README** — document `carboot gateway|reindex|advertise`, env (`CARBOOT_INDEX`, `CARBOOT_CARS_DIR`, `CARBOOT_REINDEX_ON_START`), the volume layout (cars ro, index rw), `podman exec carboot carboot reindex` for live updates, and the JSON access-log line shape. Run through the writing-readmes self-edit (no em-dashes/marketing).
- [ ] **Step 4: Build image** — `podman build --platform linux/amd64 -t carboot:dev .` → succeeds; `podman run --rm carboot:dev gateway --help` (or a quick reindex+gateway over a temp mount) works.
- [ ] **Step 5: Commit** — `git commit -am "docker: single carboot image; tools/car_index.py reference; README"`

---

## Notes for the implementer

- Module path is `github.com/dannyob/carboot`.
- Keep every `.go` file's `// SPDX-License-Identifier: BSD-3-Clause` header.
- Run `gofmt -w` before each commit; `go vet ./...` must be clean.
- The blockstore's identity-CID short-circuit (Task 2 keeps it) is load-bearing — do not drop it; `TestIdentityCID` guards it.
- After all tasks: redeploying to fatfil (rebuild image, `podman load`, recreate `carboot-cargw` as `carboot gateway`, run `carboot advertise`) is a separate deploy step, not part of this plan.
