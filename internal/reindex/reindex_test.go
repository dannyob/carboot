// SPDX-License-Identifier: BSD-3-Clause

package reindex

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"
)

// carID looks up a car row id by relative path.
func carID(t *testing.T, db *sql.DB, rel string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`SELECT id FROM cars WHERE path=?`, rel).Scan(&id); err != nil {
		t.Fatalf("car %q not in cars: %v", rel, err)
	}
	return id
}

// blkCount counts blk rows for a given car id.
func blkCount(t *testing.T, db *sql.DB, id int64) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM blk WHERE car=?`, id).Scan(&n); err != nil {
		t.Fatalf("count blk for car %d: %v", id, err)
	}
	return n
}

func TestReindexRecursiveSync(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "i.db")

	if err := os.MkdirAll(filepath.Join(root, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(root, "a.car")
	bPath := filepath.Join(root, "sub", "b.car")
	aData := [][]byte{[]byte("alpha one"), []byte("alpha two")}
	bData := [][]byte{[]byte("beta single block")}
	writeTestCAR(t, aPath, aData)
	writeTestCAR(t, bPath, bData)

	if err := Run(dbPath, root); err != nil {
		t.Fatalf("Run: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Both cars present (recursive), with mtime/size populated.
	for _, rel := range []string{"a.car", filepath.Join("sub", "b.car")} {
		var mtime, size sql.NullInt64
		if err := db.QueryRow(`SELECT mtime,size FROM cars WHERE path=?`, rel).Scan(&mtime, &size); err != nil {
			t.Fatalf("car %q missing: %v", rel, err)
		}
		if !mtime.Valid || !size.Valid {
			t.Errorf("car %q mtime/size not populated: mtime=%v size=%v", rel, mtime, size)
		}
	}
	idA := carID(t, db, "a.car")
	idB := carID(t, db, filepath.Join("sub", "b.car"))
	if got := blkCount(t, db, idA); got != len(aData) {
		t.Errorf("a.car blk count = %d, want %d", got, len(aData))
	}
	if got := blkCount(t, db, idB); got != len(bData) {
		t.Errorf("b.car blk count = %d, want %d", got, len(bData))
	}

	// Round-trip: the block at the recorded offset re-hashes to its multihash.
	rawA, _ := os.ReadFile(aPath)
	rows, err := db.Query(`SELECT mh,off,len FROM blk WHERE car=?`, idA)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var mh []byte
		var off, length int64
		if err := rows.Scan(&mh, &off, &length); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(rawA[off : off+length])
		want, _ := multihash.Encode(sum[:], multihash.SHA2_256)
		if !bytes.Equal(want, mh) {
			t.Errorf("round-trip mismatch at off %d", off)
		}
	}
	rows.Close()

	// (a) Add c.car, re-Run: c indexed, a/b untouched.
	cPath := filepath.Join(root, "c.car")
	cData := [][]byte{[]byte("gamma block")}
	writeTestCAR(t, cPath, cData)
	if err := Run(dbPath, root); err != nil {
		t.Fatalf("Run after add: %v", err)
	}
	idC := carID(t, db, "c.car")
	if got := blkCount(t, db, idC); got != len(cData) {
		t.Errorf("c.car blk count = %d, want %d", got, len(cData))
	}
	if carID(t, db, "a.car") != idA {
		t.Errorf("a.car id changed after add")
	}
	if got := blkCount(t, db, idA); got != len(aData) {
		t.Errorf("a.car blk count changed after add: %d", got)
	}

	// (b) Change a.car (newer mtime + bigger), re-Run: a re-indexed.
	aData2 := [][]byte{[]byte("alpha one"), []byte("alpha two"), []byte("alpha three added")}
	writeTestCAR(t, aPath, aData2)
	future := time.Now().Add(2 * time.Hour)
	if err := os.Chtimes(aPath, future, future); err != nil {
		t.Fatal(err)
	}
	if err := Run(dbPath, root); err != nil {
		t.Fatalf("Run after change: %v", err)
	}
	idA2 := carID(t, db, "a.car")
	if got := blkCount(t, db, idA2); got != len(aData2) {
		t.Errorf("a.car blk count after change = %d, want %d", got, len(aData2))
	}

	// (c) Delete b.car, re-Run: b's rows pruned.
	if err := os.Remove(bPath); err != nil {
		t.Fatal(err)
	}
	if err := Run(dbPath, root); err != nil {
		t.Fatalf("Run after delete: %v", err)
	}
	var nCars int
	if err := db.QueryRow(`SELECT count(*) FROM cars WHERE path=?`, filepath.Join("sub", "b.car")).Scan(&nCars); err != nil {
		t.Fatal(err)
	}
	if nCars != 0 {
		t.Errorf("b.car not pruned from cars (n=%d)", nCars)
	}
	if got := blkCount(t, db, idB); got != 0 {
		t.Errorf("b.car blk rows not pruned (n=%d)", got)
	}
}

// TestReindexLegacyBackfillNoReindex proves that running Run over a freshly
// migrated legacy index populates cars.mtime/size from disk stat and TRUSTS the
// existing blk rows instead of re-reading every CAR. The legacy index is seeded
// with a sentinel blk row that does NOT correspond to a.car's real blocks; if
// Run re-indexed the file, that sentinel would be deleted and replaced.
func TestReindexLegacyBackfillNoReindex(t *testing.T) {
	dir := t.TempDir()
	carsDir := filepath.Join(dir, "cars")
	if err := os.MkdirAll(carsDir, 0755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(carsDir, "a.car")
	writeTestCAR(t, aPath, [][]byte{[]byte("real block one"), []byte("real block two")})

	dbPath := filepath.Join(dir, "index.db")
	// Build a legacy {shards, blk.shard} index by hand, with a sentinel blk row.
	raw, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Mirror the real legacy schema the Python builder produced.
	raw.Exec(`CREATE TABLE shards(id INTEGER PRIMARY KEY, name TEXT, nblocks INTEGER, status TEXT, detail TEXT)`)
	raw.Exec(`CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER)`)
	raw.Exec(`INSERT INTO shards(id,name,nblocks,status) VALUES(1,'a.car',2,'done')`)
	sentinel := append([]byte{0x12, 0x20}, bytes.Repeat([]byte{0xAB}, 32)...)
	if _, err := raw.Exec(`INSERT INTO blk(mh,shard,off,len) VALUES(?,1,999999,42)`, sentinel); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	if err := Run(dbPath, carsDir); err != nil {
		t.Fatalf("Run: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// mtime/size populated from disk stat (no longer NULL).
	fi, _ := os.Stat(aPath)
	var mtime, size sql.NullInt64
	if err := db.QueryRow(`SELECT mtime,size FROM cars WHERE path='a.car'`).Scan(&mtime, &size); err != nil {
		t.Fatal(err)
	}
	if !mtime.Valid || mtime.Int64 != fi.ModTime().Unix() {
		t.Errorf("mtime not backfilled: %+v want %d", mtime, fi.ModTime().Unix())
	}
	if !size.Valid || size.Int64 != fi.Size() {
		t.Errorf("size not backfilled: %+v want %d", size, fi.Size())
	}

	// Sentinel survives => a.car was NOT re-indexed.
	idA := carID(t, db, "a.car")
	var off int64
	var n int
	db.QueryRow(`SELECT count(*) FROM blk WHERE car=?`, idA).Scan(&n)
	if n != 1 {
		t.Fatalf("a.car blk rows = %d, want 1 (sentinel preserved, no re-index)", n)
	}
	db.QueryRow(`SELECT off FROM blk WHERE car=?`, idA).Scan(&off)
	if off != 999999 {
		t.Errorf("sentinel blk off = %d, want 999999 (file was re-indexed!)", off)
	}

	// A genuinely new car is still indexed on a later run; a.car stays untouched.
	bPath := filepath.Join(carsDir, "b.car")
	writeTestCAR(t, bPath, [][]byte{[]byte("brand new block")})
	if err := Run(dbPath, carsDir); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := blkCount(t, db, carID(t, db, "b.car")); got == 0 {
		t.Error("b.car was not indexed on second run")
	}
	db.QueryRow(`SELECT off FROM blk WHERE car=?`, idA).Scan(&off)
	if off != 999999 {
		t.Errorf("after run 2, a.car sentinel off = %d, want 999999", off)
	}
}
