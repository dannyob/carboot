// SPDX-License-Identifier: BSD-3-Clause

// Package carindex provides a read-only boxo Blockstore backed by a SQLite
// index (multihash -> car,offset,length) and a directory of CAR files.
package carindex

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/ipfs/boxo/blockstore"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver
)

// defaultFDCacheSize is the number of open car file descriptors kept hot.
const defaultFDCacheSize = 256

// errReadOnly is returned by all mutating methods.
var errReadOnly = errors.New("carindex: blockstore is read-only")

// CarIndexBlockstore is a read-only blockstore.Blockstore backed by a SQLite
// index and a directory of CAR files. Lookups are by raw multihash, so the
// codec (raw 0x55 vs dag-pb 0x70) of the requested CID does not matter.
type CarIndexBlockstore struct {
	db          *sql.DB
	stmt        *sql.Stmt
	dbPath      string
	carsDirPath string

	mapMu  sync.RWMutex
	carMap map[int64]string // car id -> filename (loaded at Open, refreshed on miss)

	fdCache *fdCache
}

var _ blockstore.Blockstore = (*CarIndexBlockstore)(nil) // compile-time interface check

// Open opens the SQLite index at dbPath (read-only, WAL-aware) and prepares the
// blockstore to read CAR files from carsDir.
func Open(dbPath, carsDir string) (*CarIndexBlockstore, error) {
	// OpenRO opens mode=ro (NOT immutable=1), so a live reindexer's committed
	// writes become visible to the running gateway.
	db, err := OpenRO(dbPath)
	if err != nil {
		return nil, fmt.Errorf("carindex: open db: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("carindex: ping db: %w", err)
	}

	carMap, err := loadCarMap(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	stmt, err := db.Prepare(`SELECT car, off, len FROM blk WHERE mh = ?`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("carindex: prepare lookup: %w", err)
	}

	return &CarIndexBlockstore{
		db:          db,
		stmt:        stmt,
		dbPath:      dbPath,
		carsDirPath: carsDir,
		carMap:      carMap,
		fdCache:     newFDCache(defaultFDCacheSize),
	}, nil
}

// DB exposes the underlying *sql.DB for the IPNI advertiser's cursor lister.
// It shares the same read-only connection pool.
func (cb *CarIndexBlockstore) DB() *sql.DB { return cb.db }

// indexPath returns the on-disk path of the SQLite index.
func (cb *CarIndexBlockstore) indexPath() string { return cb.dbPath }

// carsDir returns the directory of CAR files this blockstore reads from.
func (cb *CarIndexBlockstore) carsDir() string { return cb.carsDirPath }

func loadCarMap(db *sql.DB) (map[int64]string, error) {
	rows, err := db.Query(`SELECT id, path FROM cars`)
	if err != nil {
		return nil, fmt.Errorf("carindex: query cars: %w", err)
	}
	defer rows.Close()

	m := make(map[int64]string)
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			return nil, fmt.Errorf("carindex: scan car: %w", err)
		}
		m[id] = path
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("carindex: iterate cars: %w", err)
	}
	return m, nil
}

// Close releases the prepared statement, open car fds, and the db handle.
func (cb *CarIndexBlockstore) Close() error {
	var errs []error
	if cb.fdCache != nil {
		errs = append(errs, cb.fdCache.closeAll())
	}
	if cb.stmt != nil {
		errs = append(errs, cb.stmt.Close())
	}
	if cb.db != nil {
		errs = append(errs, cb.db.Close())
	}
	return errors.Join(errs...)
}

// identityData returns (data, true) when c uses the identity multihash, whose
// digest IS the block data, so it is served inline without an index lookup.
// This matters beyond correctness: httpnet's connect probe (used by kubo HTTP
// retrieval, ipfs.io, and check.ipfs.network) requests the empty identity CID
// bafkqaaa and requires a 2xx, so a gateway that 404s identity CIDs is rejected
// by every httpnet client before any real retrieval.
func identityData(c cid.Cid) ([]byte, bool) {
	dmh, err := multihash.Decode(c.Hash())
	if err != nil || dmh.Code != multihash.IDENTITY {
		return nil, false
	}
	return dmh.Digest, true
}

// lookup resolves a multihash to (car id, offset, length).
func (cb *CarIndexBlockstore) lookup(ctx context.Context, c cid.Cid) (car, off, length int64, err error) {
	mh := []byte(c.Hash()) // BLOB binding; never bind as string (binds as TEXT, silently misses)
	err = cb.stmt.QueryRowContext(ctx, mh).Scan(&car, &off, &length)
	return
}

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
	return filepath.Join(cb.carsDirPath, name), nil
}

func (cb *CarIndexBlockstore) reloadCarMap() error {
	m, err := loadCarMap(cb.db) // SELECT id,path FROM cars
	if err != nil {
		return err
	}
	cb.mapMu.Lock()
	cb.carMap = m
	cb.mapMu.Unlock()
	return nil
}

// Get returns the block for c, reading its data bytes from the car file.
func (cb *CarIndexBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if data, ok := identityData(c); ok {
		return blocks.NewBlockWithCid(data, c)
	}
	car, off, length, err := cb.lookup(ctx, c)
	if err == sql.ErrNoRows {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, fmt.Errorf("carindex: index lookup for %s: %w", c, err)
	}

	path, err := cb.carPath(car)
	if err != nil {
		return nil, err
	}

	f, err := cb.fdCache.get(path)
	if err != nil {
		return nil, fmt.Errorf("carindex: open car %s: %w", path, err)
	}

	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("carindex: read %s at %d (%d bytes): %w", path, off, length, err)
	}

	// NewBlockWithCid verifies the data hashes to the given CID.
	blk, err := blocks.NewBlockWithCid(buf, c)
	if err != nil {
		return nil, fmt.Errorf("carindex: block hash mismatch for %s: %w", c, err)
	}

	if col := collectorFrom(ctx); col != nil {
		rel, _ := filepath.Rel(cb.carsDirPath, path) // path is the car file path
		col.record(rel, length)
	}
	return blk, nil
}

// Has reports whether the multihash of c is in the index.
func (cb *CarIndexBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if _, ok := identityData(c); ok {
		return true, nil
	}
	_, _, _, err := cb.lookup(ctx, c)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("carindex: index lookup for %s: %w", c, err)
	}
	return true, nil
}

// GetSize returns the stored data length for c.
func (cb *CarIndexBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if data, ok := identityData(c); ok {
		return len(data), nil
	}
	_, _, length, err := cb.lookup(ctx, c)
	if err == sql.ErrNoRows {
		return 0, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return 0, fmt.Errorf("carindex: index lookup for %s: %w", c, err)
	}
	return int(length), nil
}

// AllKeysChan streams every CID in the index. The index stores raw multihashes,
// so reconstructed CIDs use the raw codec; this is sufficient for the gateway's
// trustless paths, which never exercise this method.
func (cb *CarIndexBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	rows, err := cb.db.QueryContext(ctx, `SELECT mh FROM blk`)
	if err != nil {
		return nil, fmt.Errorf("carindex: query keys: %w", err)
	}
	ch := make(chan cid.Cid)
	go func() {
		defer close(ch)
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return
			}
			c := cid.NewCidV1(cid.Raw, raw)
			select {
			case ch <- c:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// DeleteBlock is a read-only no-op that reports the store cannot mutate.
func (cb *CarIndexBlockstore) DeleteBlock(context.Context, cid.Cid) error { return errReadOnly }

// Put is read-only.
func (cb *CarIndexBlockstore) Put(context.Context, blocks.Block) error { return errReadOnly }

// PutMany is read-only.
func (cb *CarIndexBlockstore) PutMany(context.Context, []blocks.Block) error { return errReadOnly }

// fdCache is a bounded LRU of open car files keyed by path. ReadAt is safe
// for concurrent use, so file handles are shared across goroutines.
type fdCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List               // front = most recently used
	items map[string]*list.Element // path -> element
}

type fdEntry struct {
	path string
	file *os.File
}

func newFDCache(capacity int) *fdCache {
	if capacity < 1 {
		capacity = 1
	}
	return &fdCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

// get returns an open file for path, opening and caching it if necessary.
func (c *fdCache) get(path string) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[path]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*fdEntry).file, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	el := c.ll.PushFront(&fdEntry{path: path, file: f})
	c.items[path] = el

	for c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest == nil {
			break
		}
		c.ll.Remove(oldest)
		ent := oldest.Value.(*fdEntry)
		delete(c.items, ent.path)
		ent.file.Close()
	}
	return f, nil
}

func (c *fdCache) closeAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, el := range c.items {
		errs = append(errs, el.Value.(*fdEntry).file.Close())
	}
	c.items = make(map[string]*list.Element)
	c.ll.Init()
	return errors.Join(errs...)
}
