// SPDX-License-Identifier: BSD-3-Clause

package gateway

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/dannyob/carboot/internal/carindex"

	_ "modernc.org/sqlite"
)

// blockRec records a block's CID and where its data bytes land in the car.
type blockRec struct {
	cid  cid.Cid
	data []byte
	off  int64
	len  int64
}

func writeUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

// writeCARv1 writes a minimal CARv1 file containing the given data blocks (raw
// CIDv1) and returns the data location of each block within the file.
func writeCARv1(t *testing.T, path string, datas [][]byte) []blockRec {
	t.Helper()
	var buf bytes.Buffer
	header := []byte{
		0xa2,
		0x65, 'r', 'o', 'o', 't', 's', 0x80,
		0x67, 'v', 'e', 'r', 's', 'i', 'o', 'n', 0x01,
	}
	writeUvarint(&buf, uint64(len(header)))
	buf.Write(header)

	var recs []blockRec
	for _, data := range datas {
		mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("multihash.Sum: %v", err)
		}
		c := cid.NewCidV1(cid.Raw, mh)
		cidBytes := c.Bytes()
		writeUvarint(&buf, uint64(len(cidBytes)+len(data)))
		buf.Write(cidBytes)
		dataOff := int64(buf.Len())
		buf.Write(data)
		recs = append(recs, blockRec{cid: c, data: data, off: dataOff, len: int64(len(data))})
	}
	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("write CAR: %v", err)
	}
	return recs
}

// buildIndex creates a SQLite index (blk + cars) pointing at the car file's
// block locations.
func buildIndex(t *testing.T, dbPath, carName string, recs []blockRec) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open index for write: %v", err)
	}
	defer db.Close()
	schema := `
CREATE TABLE blk(mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER);
CREATE TABLE cars(id INTEGER PRIMARY KEY, path TEXT UNIQUE, mtime INTEGER, size INTEGER, status TEXT, detail TEXT);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cars(id, path) VALUES (1, ?)`, carName); err != nil {
		t.Fatalf("insert car: %v", err)
	}
	for _, r := range recs {
		mh := []byte(r.cid.Hash())
		if _, err := db.Exec(`INSERT INTO blk(mh, car, off, len) VALUES (?, 1, ?, ?)`, mh, r.off, r.len); err != nil {
			t.Fatalf("insert blk: %v", err)
		}
	}
}

// setupBS builds a CAR + index in a temp dir and returns the open blockstore,
// the records, and the car file name.
func setupBS(t *testing.T) (*carindex.CarIndexBlockstore, []blockRec, string) {
	t.Helper()
	dir := t.TempDir()
	carName := "test.car"
	carPath := filepath.Join(dir, carName)
	dbPath := filepath.Join(dir, "index.db")

	datas := [][]byte{
		[]byte("hello trustless gateway"),
		bytes.Repeat([]byte{0xAB}, 1024),
	}
	recs := writeCARv1(t, carPath, datas)
	buildIndex(t, dbPath, carName, recs)

	bs, err := carindex.Open(dbPath, dir)
	if err != nil {
		t.Fatalf("Open blockstore: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs, recs, carName
}

// lastJSONLine parses the final JSON line written to buf.
func lastJSONLine(t *testing.T, buf *bytes.Buffer) accessLine {
	t.Helper()
	var line accessLine
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	var last []byte
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			last = append([]byte(nil), sc.Bytes()...)
		}
	}
	if last == nil {
		t.Fatalf("no JSON access line written; buf=%q", buf.String())
	}
	if err := json.Unmarshal(last, &line); err != nil {
		t.Fatalf("unmarshal access line %q: %v", last, err)
	}
	return line
}

func TestHandlerAccessLogHit(t *testing.T) {
	bs, recs, carName := setupBS(t)
	var buf bytes.Buffer
	h, err := Handler(bs, &buf)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	r := recs[0]
	resp, err := http.Get(srv.URL + "/ipfs/" + r.cid.String() + "?format=raw")
	if err != nil {
		t.Fatalf("GET raw: %v", err)
	}
	body := new(bytes.Buffer)
	body.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(body.Bytes(), r.data) {
		t.Errorf("body mismatch: got %q want %q", body.Bytes(), r.data)
	}

	line := lastJSONLine(t, &buf)
	if line.Status != http.StatusOK {
		t.Errorf("log status = %d, want 200", line.Status)
	}
	if line.CID != r.cid.String() {
		t.Errorf("log cid = %q, want %q", line.CID, r.cid.String())
	}
	if line.Format != "raw" {
		t.Errorf("log format = %q, want raw", line.Format)
	}
	if line.Bytes != r.len {
		t.Errorf("log bytes = %d, want %d", line.Bytes, r.len)
	}
	if len(line.Cars) != 1 || line.Cars[0] != carName {
		t.Errorf("log cars = %v, want [%s]", line.Cars, carName)
	}
}

func TestHandlerAccessLogMiss(t *testing.T) {
	bs, _, _ := setupBS(t)
	var buf bytes.Buffer
	h, err := Handler(bs, &buf)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	missMH, _ := multihash.Sum([]byte("absent block"), multihash.SHA2_256, -1)
	missCID := cid.NewCidV1(cid.Raw, missMH)
	resp, err := http.Get(srv.URL + "/ipfs/" + missCID.String() + "?format=raw")
	if err != nil {
		t.Fatalf("GET miss: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	line := lastJSONLine(t, &buf)
	if line.Status != http.StatusNotFound {
		t.Errorf("log status = %d, want 404", line.Status)
	}
	if len(line.Cars) != 0 {
		t.Errorf("log cars = %v, want []", line.Cars)
	}
}

func TestHandlerAccessLogIdentity(t *testing.T) {
	bs, _, _ := setupBS(t)
	var buf bytes.Buffer
	h, err := Handler(bs, &buf)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/ipfs/bafkqaaa?format=raw")
	if err != nil {
		t.Fatalf("GET identity: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	line := lastJSONLine(t, &buf)
	if line.Status != http.StatusOK {
		t.Errorf("log status = %d, want 200", line.Status)
	}
	if len(line.Cars) != 0 {
		t.Errorf("identity log cars = %v, want []", line.Cars)
	}
}
