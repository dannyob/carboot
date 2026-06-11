// SPDX-License-Identifier: BSD-3-Clause

package carindex

import (
	"database/sql"
	"fmt"
)

// Schema (current): keyed by multihash.
//
//	cars(id INTEGER PRIMARY KEY, path TEXT UNIQUE, mtime INTEGER, size INTEGER, status TEXT, detail TEXT)
//	blk(mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER)
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
