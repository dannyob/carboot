// SPDX-License-Identifier: BSD-3-Clause

// carpni advertises every block in the SQLite index to IPNI (cid.contact), so
// that kubo and the public gateways discover that carboot's gateway can serve
// those multihashes over the ipfs-gateway-http transport.
//
// It streams all multihashes from the index with a cursor-backed iterator; the
// 25M rows are never materialized in memory. The provider address advertised to
// IPNI is the gateway's public URL (--public-addr).
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	_ "modernc.org/sqlite"
)

const (
	// ipniPath is the HTTP path at which the engine's ad-chain handler is
	// served. Because it is a subset of /ipni/v1/ad/, the publisher's ServeHTTP
	// is agnostic and only cares about path.Base().
	ipniPath = "/ipni/"

	// contextID identifies this advertised set. Stable across restarts so the
	// ad chain is continuous.
	contextID = "carboot-all-blocks-v1"
)

func main() {
	indexPath := flag.String("index", "", "path to the SQLite index (required)")
	listen := flag.String("listen", "0.0.0.0:3104", "host:port to bind the IPNI ad-chain server on")
	publicAddr := flag.String("public-addr", "", "the GATEWAY's public URL advertised to IPNI as the content provider, e.g. http://1.2.3.4:3747 (required)")
	publisherAddr := flag.String("publisher-addr", "", "THIS process's public URL where the indexer fetches the ad chain, e.g. http://1.2.3.4:3104 (required; must be publicly reachable)")
	announceURL := flag.String("announce-url", "https://cid.contact/ingest/announce", "indexer announce endpoint")
	identityPath := flag.String("identity", "", "path to the persisted Ed25519 identity key (default ~/.carboot/key)")
	datastorePath := flag.String("datastore", "", "path to the persistent ad-chain datastore (default ~/.carboot/adstore)")
	flag.Parse()

	if *indexPath == "" || *publicAddr == "" || *publisherAddr == "" {
		log.Fatal("carpni: --index, --public-addr and --publisher-addr are required")
	}

	dsPath := *datastorePath
	if dsPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("carpni: home dir: %v", err)
		}
		dsPath = filepath.Join(home, ".carboot", "adstore")
	}

	keyPath := *identityPath
	if keyPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("carpni: home dir: %v", err)
		}
		keyPath = filepath.Join(home, ".carboot", "key")
	}

	privKey, id, err := loadOrCreateKey(keyPath)
	if err != nil {
		log.Fatalf("carpni: identity key: %v", err)
	}
	log.Printf("carpni: provider peer id %s", id)

	// The provider (gateway) public address as a multiaddr.
	gwMaddr, err := publicAddrToMaddr(*publicAddr)
	if err != nil {
		log.Fatalf("carpni: parse --public-addr: %v", err)
	}

	// The ad-chain server binds to --listen (e.g. 0.0.0.0:3104). The address the
	// indexer fetches ads from must be the PUBLIC --publisher-addr, not the bind
	// address — 0.0.0.0 is not routable.
	listenURL, err := url.Parse("http://" + *listen)
	if err != nil {
		log.Fatalf("carpni: parse --listen: %v", err)
	}
	pubMaddr, err := publicAddrToMaddr(*publisherAddr)
	if err != nil {
		log.Fatalf("carpni: parse --publisher-addr: %v", err)
	}
	announceMaddr, err := announceAddrFrom(pubMaddr)
	if err != nil {
		log.Fatalf("carpni: build announce addr: %v", err)
	}

	dsn := "file:" + *indexPath + "?immutable=1&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("carpni: open index: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if err := db.Ping(); err != nil {
		log.Fatalf("carpni: ping index: %v", err)
	}

	// Persistent datastore for the advertisement chain, so a restart resumes the
	// chain (and re-announce is suppressed via ErrAlreadyAdvertised) rather than
	// re-publishing all multihashes from scratch.
	ds, err := leveldb.NewDatastore(dsPath, nil)
	if err != nil {
		log.Fatalf("carpni: open ad-chain datastore: %v", err)
	}
	defer ds.Close()

	// The engine builds and signs the ad chain but does not serve it
	// (WithoutServer); we mount its publisher handler on our own HTTP server
	// below so the indexer can pull /ipni/v1/ad/... from --listen. (Letting the
	// engine self-serve was tried and served 404 for every path.)
	eng, err := engine.New(
		engine.WithPrivateKey(privKey),
		engine.WithDatastore(ds),
		engine.WithProvider(peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{gwMaddr}}),
		engine.WithDirectAnnounce(*announceURL),
		engine.WithPublisherKind(engine.HttpPublisher),
		engine.WithHttpPublisherWithoutServer(),
		// Empty handler path: ipniPath ("/ipni/") is a prefix of /ipni/v1/ad/,
		// so the publisher's ServeHTTP is path-agnostic and keys only off
		// path.Base. Passing "/ipni/" here makes it reject /ipni/v1/ad/head as
		// an invalid path. We still mount the handler at ipniPath on our mux.
		engine.WithHttpPublisherHandlerPath(""),
		engine.WithHttpPublisherListenAddr(listenURL.Host),
		engine.WithHttpPublisherAnnounceAddr(announceMaddr.String()),
	)
	if err != nil {
		log.Fatalf("carpni: engine: %v", err)
	}

	// Fresh cursor per call: *sql.Rows is single-use and the engine may iterate
	// more than once (e.g. on re-sync).
	eng.RegisterMultihashLister(func(ctx context.Context, p peer.ID, ctxID []byte) (provider.MultihashIterator, error) {
		return newSQLiteMHIterator(ctx, db)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("carpni: engine start: %v", err)
	}
	defer func() {
		if err := eng.Shutdown(); err != nil {
			log.Printf("carpni: engine shutdown: %v", err)
		}
	}()

	// Serve the ad chain. mux pattern "/ipni/" is a subtree match, so the
	// indexer's GET /ipni/v1/ad/<cid> routes to the publisher handler, which
	// keys off path.Base. Must be up before we announce.
	handlerFunc, err := eng.GetPublisherHttpFunc()
	if err != nil {
		log.Fatalf("carpni: publisher handler: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(ipniPath, handlerFunc)
	adServer := &http.Server{Addr: listenURL.Host, Handler: mux}
	go func() {
		if err := adServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("carpni: ad-chain server: %v", err)
		}
	}()
	defer adServer.Close()

	md := metadata.Default.New(metadata.IpfsGatewayHttp{})
	adCid, err := eng.NotifyPut(ctx, nil, []byte(contextID), md)
	switch {
	case errors.Is(err, provider.ErrAlreadyAdvertised):
		log.Printf("carpni: already advertised context %q; ad chain continues", contextID)
	case err != nil:
		log.Fatalf("carpni: notify put: %v", err)
	default:
		log.Printf("carpni: published advertisement %s for context %q", adCid, contextID)
	}

	log.Printf("carpni: advertising %s over ipfs-gateway-http; serving ad chain on %s%s", *publicAddr, *listen, ipniPath)
	log.Printf("carpni: announced to %s", *announceURL)

	<-ctx.Done()
	log.Println("carpni: shutting down...")
}

// publicAddrToMaddr converts a public URL (or multiaddr) into a multiaddr.
func publicAddrToMaddr(addr string) (multiaddr.Multiaddr, error) {
	if u, err := url.Parse(addr); err == nil && u.Scheme != "" {
		return maurl.FromURL(u)
	}
	return multiaddr.NewMultiaddr(addr)
}

// announceAddrFrom builds the multiaddr the indexer fetches ads from, given the
// publisher's public base multiaddr, appending the ipni httpath component when
// it is not a subset of /ipni/v1/ad/.
func announceAddrFrom(base multiaddr.Multiaddr) (multiaddr.Multiaddr, error) {
	if strings.HasPrefix("/ipni/v1/ad/", ipniPath) {
		// subset of /ipni/v1/ad/: ServeHTTP only cares about path.Base, no
		// httpath component needed.
		return base, nil
	}
	httpath, err := multiaddr.NewComponent("httpath", url.PathEscape(ipniPath))
	if err != nil {
		return nil, err
	}
	return multiaddr.Join(base, httpath), nil
}

// loadOrCreateKey loads an Ed25519 private key from keyFile, generating and
// persisting one (mode 0600) if absent. The peer ID is derived from the key.
func loadOrCreateKey(keyFile string) (crypto.PrivKey, peer.ID, error) {
	if err := os.MkdirAll(path.Dir(keyFile), 0700); err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(keyFile)
	var privKey crypto.PrivKey
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, "", err
		}
		privKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, "", err
		}
		marshalled, err := crypto.MarshalPrivateKey(privKey)
		if err != nil {
			return nil, "", err
		}
		if err := os.WriteFile(keyFile, marshalled, 0600); err != nil {
			return nil, "", err
		}
	} else {
		privKey, err = crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, "", err
		}
	}
	id, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, "", err
	}
	return privKey, id, nil
}

// sqliteMHIterator streams multihashes from the index with a single cursor.
// The engine pulls Next() lazily, chunking the rows into entry-chunk DAG nodes,
// so the full set never materializes in RAM.
type sqliteMHIterator struct {
	rows *sql.Rows
}

var _ provider.MultihashIterator = (*sqliteMHIterator)(nil)

func newSQLiteMHIterator(ctx context.Context, db *sql.DB) (*sqliteMHIterator, error) {
	// ORDER BY rowid gives a deterministic order: the lister contract requires
	// identical multihashes in identical order across re-iterations.
	rows, err := db.QueryContext(ctx, "SELECT mh FROM blk ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	return &sqliteMHIterator{rows: rows}, nil
}

func (it *sqliteMHIterator) Next() (multihash.Multihash, error) {
	if !it.rows.Next() {
		if err := it.rows.Err(); err != nil {
			return nil, err
		}
		_ = it.rows.Close()
		return nil, io.EOF // contract: zero multihash + io.EOF at end
	}
	var raw []byte
	if err := it.rows.Scan(&raw); err != nil {
		return nil, err
	}
	// blk.mh stores full multihash bytes; Cast validates them.
	return multihash.Cast(raw)
}
