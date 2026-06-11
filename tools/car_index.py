#!/usr/bin/env python3
# SPDX-License-Identifier: BSD-3-Clause
#
# REFERENCE ONLY. The Go `carboot reindex` subcommand is the runtime indexer;
# this Python builder is kept as the reference origin of that logic. The
# container and production runtime use `carboot reindex`, not this script.
#
# car_index.py builds the SQLite index that carboot serves from: it streams each
# CARv1 file, records every block's multihash and the (file, data offset, data
# length) of its bytes, and writes them into the schema below. Lookups are by
# multihash, so a CID's raw (0x55) vs dag-pb (0x70) codec never matters.
#
#   cars(id INTEGER PRIMARY KEY, path TEXT UNIQUE,
#        mtime INTEGER, size INTEGER, status TEXT, detail TEXT)
#   blk(mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER)
#
# `off`/`len` point at the block's DATA bytes within the CAR file (not the CID).
# A truncated CAR is indexed up to the break and flagged in cars.status.
#
# Usage:
#   car_index.py --index index.db --cars-dir /store/cars
#
# This single-pass builder mirrors the Go reindexer's CARv1 parsing. The Go
# version additionally does mtime/size add/change/prune sync and is what runs in
# the container.

import argparse
import os
import sqlite3
import sys


def read_uvarint(buf, pos):
    """Read an unsigned LEB128 varint from buf starting at pos.

    Returns (value, new_pos). Raises EOFError if the buffer ends mid-varint.
    """
    x = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise EOFError("varint past EOF")
        b = buf[pos]
        pos += 1
        x |= (b & 0x7F) << shift
        if b < 0x80:
            return x, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def uvarint_len(x):
    n = 1
    while x >= 0x80:
        x >>= 7
        n += 1
    return n


def cid_len(buf, pos):
    """Return the byte length of the binary CID starting at pos.

    Handles CIDv0 (0x12 0x20 + 32-byte digest) and CIDv1
    (<version><codec><mhcode><mhlen><digest>). The multihash is the trailing
    <mhcode><mhlen><digest>, which is what we key blocks on.
    """
    start = pos
    if buf[pos] == 0x12:  # CIDv0: 0x12 0x20 + 32-byte sha2-256 digest
        # The whole CIDv0 IS the multihash (sha2-256, 32 bytes): 34 bytes.
        return 34, 34
    # CIDv1: version varint, codec varint, then a multihash.
    _version, pos = read_uvarint(buf, pos)
    _codec, pos = read_uvarint(buf, pos)
    mh_start = pos
    _mhcode, pos = read_uvarint(buf, pos)
    mhlen, pos = read_uvarint(buf, pos)
    pos += mhlen
    total = pos - start
    mh_len = pos - mh_start
    return total, mh_len


def multihash_of(buf, cid_off):
    """Extract the raw multihash bytes for the CID at cid_off."""
    if buf[cid_off] == 0x12:  # CIDv0 == its multihash
        return bytes(buf[cid_off:cid_off + 34])
    pos = cid_off
    _version, pos = read_uvarint(buf, pos)
    _codec, pos = read_uvarint(buf, pos)
    mh_start = pos
    _mhcode, pos = read_uvarint(buf, pos)
    mhlen, pos = read_uvarint(buf, pos)
    pos += mhlen
    return bytes(buf[mh_start:pos])


def index_one(db, car_id, path):
    """Stream one CARv1 file, inserting (mh, car_id, data_off, data_len) rows.

    Returns (status, detail). A truncated file is indexed up to the break.
    """
    with open(path, "rb") as f:
        buf = f.read()
    size = len(buf)
    pos = 0
    n = 0

    try:
        hlen, pos = read_uvarint(buf, 0)  # header length varint
    except EOFError:
        return "error", "empty or unreadable header"
    pos += hlen  # skip the CBOR header bytes
    if pos > size:
        return "error", "header past EOF"

    cur = db.cursor()
    while pos < size:
        sec_start_varint = pos
        try:
            blen, pos = read_uvarint(buf, pos)
        except (EOFError, ValueError):
            return "truncated", "section length after %d blocks" % n
        sec_data_start = pos
        if sec_data_start + blen > size:
            return "truncated", "section past EOF after %d blocks" % n
        try:
            clen, _mhl = cid_len(buf, sec_data_start)
        except (EOFError, ValueError, IndexError):
            return "truncated", "cid after %d blocks" % n
        data_off = sec_data_start + clen
        data_len = blen - clen
        mh = multihash_of(buf, sec_data_start)
        cur.execute(
            "INSERT OR IGNORE INTO blk(mh,car,off,len) VALUES(?,?,?,?)",
            (mh, car_id, data_off, data_len),
        )
        pos = sec_data_start + blen
        n += 1
        _ = sec_start_varint
    return "done", ""


SCHEMA = """
CREATE TABLE IF NOT EXISTS cars(
  id INTEGER PRIMARY KEY, path TEXT UNIQUE,
  mtime INTEGER, size INTEGER, status TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS blk(
  mh BLOB PRIMARY KEY, car INTEGER, off INTEGER, len INTEGER);
"""


def main():
    ap = argparse.ArgumentParser(description="Build the carboot SQLite index from a CAR directory (reference builder).")
    ap.add_argument("--index", required=True, help="path to the SQLite index to write")
    ap.add_argument("--cars-dir", required=True, help="directory of CAR files (scanned recursively)")
    args = ap.parse_args()

    db = sqlite3.connect(args.index)
    db.executescript(SCHEMA)

    for root, _dirs, files in os.walk(args.cars_dir):
        for name in files:
            if not name.endswith(".car"):
                continue
            full = os.path.join(root, name)
            rel = os.path.relpath(full, args.cars_dir)
            st = os.stat(full)
            db.execute(
                "INSERT OR REPLACE INTO cars(path,mtime,size,status) VALUES(?,?,?,'indexing')",
                (rel, int(st.st_mtime), st.st_size),
            )
            car_id = db.execute("SELECT id FROM cars WHERE path=?", (rel,)).fetchone()[0]
            db.execute("DELETE FROM blk WHERE car=?", (car_id,))
            status, detail = index_one(db, car_id, full)
            db.execute(
                "UPDATE cars SET status=?, detail=? WHERE id=?",
                (status, detail, car_id),
            )
            db.commit()
            print("%s\t%s\t%s" % (rel, status, detail), file=sys.stderr)

    db.close()


if __name__ == "__main__":
    main()
