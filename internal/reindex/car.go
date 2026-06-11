// SPDX-License-Identifier: BSD-3-Clause

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
	MH      multihash.Multihash
	DataOff int64
	DataLen int64
}

// ErrTruncated is returned (with the count of blocks read so far) when a CAR
// ends mid-frame; the blocks read before the break are still valid.
type ErrTruncated struct {
	N   int
	Msg string
}

func (e *ErrTruncated) Error() string {
	return fmt.Sprintf("truncated after %d blocks: %s", e.N, e.Msg)
}

// ScanCar streams a CARv1 file, calling fn for each block. fn must copy MH if it
// retains it. Returns *ErrTruncated for a truncated file (after delivering the
// good blocks), nil on clean EOF.
func ScanCar(path string, fn func(Block) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, _ := f.Stat()
	size := fi.Size()
	r := bufio.NewReaderSize(f, 1<<20)

	// header: uvarint length + CBOR header bytes; we only need to skip it.
	hlen, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("read header len: %w", err)
	}
	off := int64(uvarintLen(hlen)) + int64(hlen)
	if _, err := r.Discard(int(hlen)); err != nil {
		return &ErrTruncated{0, "header past EOF"}
	}

	n := 0
	for {
		blen, err := binary.ReadUvarint(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return &ErrTruncated{n, "section length"}
		}
		secStart := off + int64(uvarintLen(blen))
		if secStart+int64(blen) > size {
			return &ErrTruncated{n, "section past EOF"}
		}
		// parse CID prefix to learn its byte length, then mh.
		mh, cidLen, err := readCID(r)
		if err != nil {
			return &ErrTruncated{n, "cid: " + err.Error()}
		}
		dataOff := secStart + int64(cidLen)
		dataLen := int64(blen) - int64(cidLen)
		if err := fn(Block{MH: mh, DataOff: dataOff, DataLen: dataLen}); err != nil {
			return err
		}
		// advance past the data (we already consumed the cid via readCID)
		if _, err := r.Discard(int(dataLen)); err != nil {
			return &ErrTruncated{n, "data past EOF"}
		}
		off = secStart + int64(blen)
		n++
	}
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// readCID reads a binary CID from r, returning its multihash and total CID byte
// length. Handles CIDv0 (0x12 0x20 + 32) and CIDv1 (<ver><codec><mhcode><mhlen><digest>).
func readCID(r *bufio.Reader) (multihash.Multihash, int, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	if b0 == 0x12 { // CIDv0: 0x12 0x20 + 32-byte digest
		b1, err := r.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		if b1 != 0x20 {
			return nil, 0, fmt.Errorf("bad cidv0")
		}
		dig := make([]byte, 32)
		if _, err := io.ReadFull(r, dig); err != nil {
			return nil, 0, err
		}
		mh, _ := multihash.Encode(dig, multihash.SHA2_256)
		return mh, 34, nil
	}
	// CIDv1: b0 is the version varint's first byte (0x01). codec, then multihash.
	// Read codec varint.
	if b0 != 0x01 {
		return nil, 0, fmt.Errorf("unsupported cid version byte 0x%02x", b0)
	}
	consumed := 1
	codec, cn, err := readUvarintCounted(r)
	if err != nil {
		return nil, 0, err
	}
	_ = codec
	consumed += cn
	mhcode, mn, err := readUvarintCounted(r)
	if err != nil {
		return nil, 0, err
	}
	consumed += mn
	mhlen, ln, err := readUvarintCounted(r)
	if err != nil {
		return nil, 0, err
	}
	consumed += ln
	dig := make([]byte, mhlen)
	if _, err := io.ReadFull(r, dig); err != nil {
		return nil, 0, err
	}
	consumed += int(mhlen)
	mh, err := multihash.Encode(dig, mhcode)
	if err != nil {
		return nil, 0, err
	}
	return mh, consumed, nil
}

func readUvarintCounted(r *bufio.Reader) (uint64, int, error) {
	var x uint64
	var s uint
	n := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		n++
		if b < 0x80 {
			return x | uint64(b)<<s, n, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
		if s > 63 {
			return 0, 0, fmt.Errorf("varint too long")
		}
	}
}
