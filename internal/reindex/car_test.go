// SPDX-License-Identifier: BSD-3-Clause

package reindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/multiformats/go-multihash"
)

// writeTestCAR writes a minimal CARv1 file containing the given data blocks,
// each addressed by a raw (codec 0x55) CIDv1 over its SHA2-256 multihash. The
// builder is by hand so the test has no dependency on a CAR/CID library.
func writeTestCAR(t *testing.T, path string, datas [][]byte) {
	t.Helper()

	var buf bytes.Buffer

	// CARv1 header: dag-cbor map {roots: [], version: 1}.
	//   a2                         map(2)
	//   65 "roots"  80             "roots" -> array(0)
	//   67 "version" 01            "version" -> 1
	header := []byte{
		0xa2,
		0x65, 'r', 'o', 'o', 't', 's', 0x80,
		0x67, 'v', 'e', 'r', 's', 'i', 'o', 'n', 0x01,
	}
	writeUvarintBuf(&buf, uint64(len(header)))
	buf.Write(header)

	for _, data := range datas {
		mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("multihash.Sum: %v", err)
		}
		// raw CIDv1: <version=0x01><codec=0x55 raw><multihash bytes>
		cidBytes := append([]byte{0x01, 0x55}, []byte(mh)...)

		// section = varint(len(cid)+len(data)) | cid | data
		writeUvarintBuf(&buf, uint64(len(cidBytes)+len(data)))
		buf.Write(cidBytes)
		buf.Write(data)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("write CAR: %v", err)
	}
}

func writeUvarintBuf(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func TestScanCarRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.car")
	datas := [][]byte{[]byte("alpha block here"), []byte("second"), bytes.Repeat([]byte{7}, 500)}
	writeTestCAR(t, p, datas) // helper builds CARv1 with raw CIDv1 blocks
	raw, _ := os.ReadFile(p)
	var got [][]byte
	err := ScanCar(p, func(b Block) error {
		// verify the recorded location and hash
		sum := sha256.Sum256(raw[b.DataOff : b.DataOff+b.DataLen])
		mh, _ := multihash.Encode(sum[:], multihash.SHA2_256)
		if !bytes.Equal(mh, b.MH) {
			t.Errorf("mh mismatch at off %d", b.DataOff)
		}
		got = append(got, raw[b.DataOff:b.DataOff+b.DataLen])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(datas) {
		t.Fatalf("got %d blocks want %d", len(got), len(datas))
	}
}

func TestScanCarTruncated(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "t.car")
	writeTestCAR(t, p, [][]byte{[]byte("aaa"), []byte("bbb")})
	full, _ := os.ReadFile(p)
	os.WriteFile(p, full[:len(full)-2], 0644) // chop last 2 bytes
	n := 0
	err := ScanCar(p, func(Block) error { n++; return nil })
	var trunc *ErrTruncated
	if !errors.As(err, &trunc) {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}
