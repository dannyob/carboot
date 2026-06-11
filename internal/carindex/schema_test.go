// SPDX-License-Identifier: BSD-3-Clause

package carindex

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func openRaw(path string) (*sql.DB, error) { return sql.Open("sqlite", "file:"+path) }

func TestOpenRWCreatesSchema(t *testing.T) {
	p := filepath.Join(t.TempDir(), "i.db")
	db, err := OpenRW(p)
	if err != nil {
		t.Fatal(err)
	}
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
	raw, err := openRaw(p) // helper opens without migration
	if err != nil {
		t.Fatal(err)
	}
	raw.Exec(`CREATE TABLE shards(id INTEGER PRIMARY KEY, name TEXT)`)
	raw.Exec(`CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER)`)
	raw.Exec(`INSERT INTO shards VALUES(1,'x.car')`)
	raw.Exec(`INSERT INTO blk VALUES(x'1220aa', 1, 98, 10)`)
	raw.Close()

	db, err := OpenRW(p) // triggers migration
	if err != nil {
		t.Fatal(err)
	}
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
