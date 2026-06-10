package carindex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	_ "modernc.org/sqlite"
)

// blockRec records a block's CID and where its data bytes land in the shard.
type blockRec struct {
	cid  cid.Cid
	data []byte
	off  int64
	len  int64
}

// writeCARv1 writes a minimal CARv1 file containing the given data blocks and
// returns the location (offset/length of the data bytes) of each block within
// the file. The header is the canonical dag-cbor {roots:[], version:1}.
func writeCARv1(t *testing.T, path string, datas [][]byte) []blockRec {
	t.Helper()

	var buf bytes.Buffer

	// CARv1 header: dag-cbor map {roots: [], version: 1}.
	// Built by hand so the test has no dependency on a CAR writer.
	//   a2                         map(2)
	//   65 "roots"  80             "roots" -> array(0)
	//   67 "version" 01            "version" -> 1
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

		// section = varint(len(cid)+len(data)) | cid | data
		writeUvarint(&buf, uint64(len(cidBytes)+len(data)))
		buf.Write(cidBytes)
		dataOff := int64(buf.Len()) // offset of the data bytes within the file
		buf.Write(data)

		recs = append(recs, blockRec{
			cid:  c,
			data: data,
			off:  dataOff,
			len:  int64(len(data)),
		})
	}

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("write CAR: %v", err)
	}
	return recs
}

func writeUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

// buildIndex creates a SQLite index (blk + shards) pointing at the shard file's
// block locations.
func buildIndex(t *testing.T, dbPath, shardName string, recs []blockRec) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open index for write: %v", err)
	}
	defer db.Close()

	schema := `
CREATE TABLE blk(mh BLOB PRIMARY KEY, shard INTEGER, off INTEGER, len INTEGER);
CREATE TABLE shards(id INTEGER PRIMARY KEY, name TEXT);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO shards(id, name) VALUES (1, ?)`, shardName); err != nil {
		t.Fatalf("insert shard: %v", err)
	}
	for _, r := range recs {
		mh := []byte(r.cid.Hash())
		if _, err := db.Exec(`INSERT INTO blk(mh, shard, off, len) VALUES (?, 1, ?, ?)`, mh, r.off, r.len); err != nil {
			t.Fatalf("insert blk: %v", err)
		}
	}
}

// setup builds a CAR + index in a temp dir and returns the open blockstore and
// the block records.
func setup(t *testing.T) (*CarIndexBlockstore, []blockRec) {
	t.Helper()

	dir := t.TempDir()
	shardName := "test.shard-0.car"
	shardPath := filepath.Join(dir, shardName)
	dbPath := filepath.Join(dir, "index.db")

	datas := [][]byte{
		[]byte("hello trustless gateway"),
		[]byte("the second block of bytes here"),
		[]byte("third"),
		bytes.Repeat([]byte{0xAB}, 1024),
	}
	recs := writeCARv1(t, shardPath, datas)
	buildIndex(t, dbPath, shardName, recs)

	bs, err := Open(dbPath, dir)
	if err != nil {
		t.Fatalf("Open blockstore: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs, recs
}

func TestGetRoundTrip(t *testing.T) {
	bs, recs := setup(t)
	ctx := context.Background()

	for i, r := range recs {
		blk, err := bs.Get(ctx, r.cid)
		if err != nil {
			t.Fatalf("block %d Get: %v", i, err)
		}
		if !bytes.Equal(blk.RawData(), r.data) {
			t.Errorf("block %d: data mismatch: got %d bytes, want %d", i, len(blk.RawData()), len(r.data))
		}
		// The returned block's CID must match (NewBlockWithCid verifies the hash).
		if !blk.Cid().Equals(r.cid) {
			t.Errorf("block %d: cid mismatch: got %s want %s", i, blk.Cid(), r.cid)
		}
		// And the multihash of the data must match the CID's multihash.
		mh, err := multihash.Sum(blk.RawData(), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("rehash: %v", err)
		}
		if !bytes.Equal(mh, r.cid.Hash()) {
			t.Errorf("block %d: rehashed multihash does not match cid", i)
		}
	}
}

func TestHasAndGetSize(t *testing.T) {
	bs, recs := setup(t)
	ctx := context.Background()

	for i, r := range recs {
		has, err := bs.Has(ctx, r.cid)
		if err != nil || !has {
			t.Errorf("block %d Has: has=%v err=%v", i, has, err)
		}
		sz, err := bs.GetSize(ctx, r.cid)
		if err != nil {
			t.Errorf("block %d GetSize: %v", i, err)
		}
		if int64(sz) != r.len {
			t.Errorf("block %d GetSize: got %d want %d", i, sz, r.len)
		}
	}
}

func TestMissReturnsNotFound(t *testing.T) {
	bs, _ := setup(t)
	ctx := context.Background()

	missMH, _ := multihash.Sum([]byte("this block is not in the index"), multihash.SHA2_256, -1)
	missCID := cid.NewCidV1(cid.Raw, missMH)

	_, err := bs.Get(ctx, missCID)
	if !ipld.IsNotFound(err) {
		t.Errorf("Get miss: expected ErrNotFound, got %v", err)
	}

	_, err = bs.GetSize(ctx, missCID)
	if !ipld.IsNotFound(err) {
		t.Errorf("GetSize miss: expected ErrNotFound, got %v", err)
	}

	has, err := bs.Has(ctx, missCID)
	if err != nil {
		t.Errorf("Has miss: unexpected err %v", err)
	}
	if has {
		t.Errorf("Has miss: expected false")
	}
}

func TestReadOnlyMethods(t *testing.T) {
	bs, _ := setup(t)
	ctx := context.Background()

	if err := bs.DeleteBlock(ctx, cid.Undef); err == nil {
		t.Error("DeleteBlock: expected read-only error")
	}
	if err := bs.Put(ctx, nil); err == nil {
		t.Error("Put: expected read-only error")
	}
	if err := bs.PutMany(ctx, nil); err == nil {
		t.Error("PutMany: expected read-only error")
	}
}

// TestGatewayServesRaw mounts the cargw handler over the blockstore and asserts
// GET /ipfs/<cid>?format=raw returns 200 with the correct bytes.
func TestGatewayServesRaw(t *testing.T) {
	bs, recs := setup(t)

	exch := offline.Exchange(bs)
	bsvc := blockservice.New(bs, exch)
	backend, err := gateway.NewBlocksBackend(bsvc)
	if err != nil {
		t.Fatalf("NewBlocksBackend: %v", err)
	}
	cfg := gateway.Config{DeserializedResponses: false, NoDNSLink: true}
	handler := gateway.NewHandler(cfg, backend)

	mux := http.NewServeMux()
	mux.Handle("/ipfs/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := recs[0]
	resp, err := http.Get(srv.URL + "/ipfs/" + r.cid.String() + "?format=raw")
	if err != nil {
		t.Fatalf("GET raw: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw: status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/vnd.ipld.raw" {
		t.Errorf("raw: content-type %q", ct)
	}
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body.Bytes(), r.data) {
		t.Errorf("raw body mismatch: got %q want %q", body.Bytes(), r.data)
	}

	// A miss should 404.
	missMH, _ := multihash.Sum([]byte("absent"), multihash.SHA2_256, -1)
	missCID := cid.NewCidV1(cid.Raw, missMH)
	resp2, err := http.Get(srv.URL + "/ipfs/" + missCID.String() + "?format=raw")
	if err != nil {
		t.Fatalf("GET raw miss: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("miss: status %d, want 404", resp2.StatusCode)
	}
}

// TestGatewayServesCAR asserts ?format=car&dag-scope=all returns a CARv1.
func TestGatewayServesCAR(t *testing.T) {
	bs, recs := setup(t)

	exch := offline.Exchange(bs)
	bsvc := blockservice.New(bs, exch)
	backend, err := gateway.NewBlocksBackend(bsvc)
	if err != nil {
		t.Fatalf("NewBlocksBackend: %v", err)
	}
	cfg := gateway.Config{DeserializedResponses: false, NoDNSLink: true}
	handler := gateway.NewHandler(cfg, backend)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := recs[0]
	resp, err := http.Get(srv.URL + "/ipfs/" + r.cid.String() + "?format=car&dag-scope=all")
	if err != nil {
		t.Fatalf("GET car: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("car: status %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !bytes.Contains([]byte(ct), []byte("application/vnd.ipld.car")) {
		t.Errorf("car: content-type %q", ct)
	}
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read car body: %v", err)
	}
	// CARv1 begins with a varint length then the dag-cbor header; the block's
	// raw data must appear somewhere in the CAR body.
	if !bytes.Contains(body.Bytes(), r.data) {
		t.Errorf("car body does not contain block data")
	}
}
