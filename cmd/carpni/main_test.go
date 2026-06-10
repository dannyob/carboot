// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/multiformats/go-multihash"

	_ "modernc.org/sqlite"
)

// TestSQLiteMHIterator builds a tiny index and checks the cursor iterator
// streams every stored multihash in rowid order and terminates with io.EOF,
// without materialising the set.
func TestSQLiteMHIterator(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "index.db")
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER)`); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// Insert N blocks with real sha2-256 multihashes; remember insertion order.
	var want []multihash.Multihash
	for i := 0; i < 5; i++ {
		sum := sha256.Sum256([]byte{byte(i)})
		mh, err := multihash.Encode(sum[:], multihash.SHA2_256)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO blk(mh,shard,off,len) VALUES(?,?,?,?)`, []byte(mh), 0, int64(i), 1); err != nil {
			t.Fatalf("insert: %v", err)
		}
		want = append(want, mh)
	}

	it, err := newSQLiteMHIterator(context.Background(), db)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	var got []multihash.Multihash
	for {
		mh, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, mh)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d multihashes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].B58String() != want[i].B58String() {
			t.Errorf("row %d: got %s, want %s", i, got[i].B58String(), want[i].B58String())
		}
	}

	// A second call returns a fresh cursor (the engine may re-iterate).
	it2, err := newSQLiteMHIterator(context.Background(), db)
	if err != nil {
		t.Fatalf("second iterator: %v", err)
	}
	n := 0
	for {
		_, err := it2.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("second Next: %v", err)
		}
		n++
	}
	if n != len(want) {
		t.Errorf("second pass yielded %d, want %d", n, len(want))
	}
}
