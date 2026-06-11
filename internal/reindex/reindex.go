// SPDX-License-Identifier: BSD-3-Clause

package reindex

import (
	"database/sql"
	"io/fs"
	"log"
	"path/filepath"
	"strings"

	"github.com/dannyob/carboot/internal/carindex"
)

// Run walks carsDir recursively, syncing the index at dbPath: new files indexed,
// changed files (mtime or size) re-indexed, removed files pruned.
func Run(dbPath, carsDir string) error {
	db, err := carindex.OpenRW(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	onDisk, err := scanDir(carsDir) // relpath -> (mtime,size)
	if err != nil {
		return err
	}
	inDB, err := loadCars(db) // relpath -> (id,mtime,size)
	if err != nil {
		return err
	}

	for rel, fi := range onDisk {
		cur, exists := inDB[rel]
		if exists && cur.mtime == fi.mtime && cur.size == fi.size {
			continue // unchanged
		}
		if exists {
			if _, err := db.Exec(`DELETE FROM blk WHERE car=?`, cur.id); err != nil {
				return err
			}
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
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".car") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		out[rel] = finfo{mtime: info.ModTime().Unix(), size: info.Size()}
		return nil
	})
	return out, err
}

func loadCars(db *sql.DB) (map[string]carrow, error) {
	rows, err := db.Query(`SELECT id, path, COALESCE(mtime,-1), COALESCE(size,-1) FROM cars`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]carrow{}
	for rows.Next() {
		var r carrow
		var path string
		if err := rows.Scan(&r.id, &path, &r.mtime, &r.size); err != nil {
			return nil, err
		}
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
	if err != nil {
		return err
	}
	var carID int64
	if err := db.QueryRow(`SELECT id FROM cars WHERE path=?`, rel).Scan(&carID); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO blk(mh,car,off,len) VALUES(?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
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
	if err := tx.Commit(); err != nil {
		return err
	}
	_, _ = db.Exec(`UPDATE cars SET status=?, detail=? WHERE id=?`, status, detail, carID)
	return nil
}
