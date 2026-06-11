// SPDX-License-Identifier: BSD-3-Clause

// Package advertise advertises every block in the SQLite index to IPNI
// (cid.contact), so that kubo and the public gateways discover that carboot's
// gateway can serve those multihashes over the ipfs-gateway-http transport.
//
// It streams all multihashes from the index with a cursor-backed iterator; the
// 25M rows are never materialized in memory. The provider address advertised to
// IPNI is the gateway's public URL (Options.PublicAddr).
package advertise

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

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

// Options configures the IPNI advertiser.
type Options struct {
	// IndexPath is the path to the SQLite index (required).
	IndexPath string
	// Listen is the host:port to bind the IPNI ad-chain server on.
	Listen string
	// PublicAddr is the GATEWAY's public URL advertised to IPNI as the content
	// provider, e.g. http://1.2.3.4:3747 (required).
	PublicAddr string
	// PublisherAddr is THIS process's public URL where the indexer fetches the
	// ad chain, e.g. http://1.2.3.4:3104 (required; must be publicly reachable).
	PublisherAddr string
	// AnnounceURL is the indexer announce endpoint.
	AnnounceURL string
	// IdentityPath is the path to the persisted Ed25519 identity key (default
	// ~/.carboot/key).
	IdentityPath string
	// DatastorePath is the path to the persistent ad-chain datastore (default
	// ~/.carboot/adstore).
	DatastorePath string
}

// Run advertises the index's blocks to IPNI and serves the ad chain until ctx
// is done.
func Run(ctx context.Context, opt Options) error {
	if opt.IndexPath == "" || opt.PublicAddr == "" || opt.PublisherAddr == "" {
		return fmt.Errorf("advertise: IndexPath, PublicAddr and PublisherAddr are required")
	}
	if opt.Listen == "" {
		opt.Listen = "0.0.0.0:3104"
	}
	if opt.AnnounceURL == "" {
		opt.AnnounceURL = "https://cid.contact/ingest/announce"
	}

	dsPath := opt.DatastorePath
	if dsPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("advertise: home dir: %w", err)
		}
		dsPath = filepath.Join(home, ".carboot", "adstore")
	}

	keyPath := opt.IdentityPath
	if keyPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("advertise: home dir: %w", err)
		}
		keyPath = filepath.Join(home, ".carboot", "key")
	}

	privKey, id, err := loadOrCreateKey(keyPath)
	if err != nil {
		return fmt.Errorf("advertise: identity key: %w", err)
	}
	log.Printf("advertise: provider peer id %s", id)

	// The provider (gateway) public address as a multiaddr.
	gwMaddr, err := publicAddrToMaddr(opt.PublicAddr)
	if err != nil {
		return fmt.Errorf("advertise: parse public-addr: %w", err)
	}

	// The ad-chain server binds to Listen (e.g. 0.0.0.0:3104). The address the
	// indexer fetches ads from must be the PUBLIC PublisherAddr, not the bind
	// address — 0.0.0.0 is not routable.
	listenURL, err := url.Parse("http://" + opt.Listen)
	if err != nil {
		return fmt.Errorf("advertise: parse listen: %w", err)
	}
	pubMaddr, err := publicAddrToMaddr(opt.PublisherAddr)
	if err != nil {
		return fmt.Errorf("advertise: parse publisher-addr: %w", err)
	}
	announceMaddr, err := announceAddrFrom(pubMaddr)
	if err != nil {
		return fmt.Errorf("advertise: build announce addr: %w", err)
	}

	dsn := "file:" + opt.IndexPath + "?immutable=1&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("advertise: open index: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if err := db.Ping(); err != nil {
		return fmt.Errorf("advertise: ping index: %w", err)
	}

	// Persistent datastore for the advertisement chain, so a restart resumes the
	// chain (and re-announce is suppressed via ErrAlreadyAdvertised) rather than
	// re-publishing all multihashes from scratch.
	ds, err := leveldb.NewDatastore(dsPath, nil)
	if err != nil {
		return fmt.Errorf("advertise: open ad-chain datastore: %w", err)
	}
	defer ds.Close()

	// The engine builds and signs the ad chain but does not serve it
	// (WithoutServer); we mount its publisher handler on our own HTTP server
	// below so the indexer can pull /ipni/v1/ad/... from Listen. (Letting the
	// engine self-serve was tried and served 404 for every path.)
	eng, err := engine.New(
		engine.WithPrivateKey(privKey),
		engine.WithDatastore(ds),
		engine.WithProvider(peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{gwMaddr}}),
		engine.WithDirectAnnounce(opt.AnnounceURL),
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
		return fmt.Errorf("advertise: engine: %w", err)
	}

	// Fresh cursor per call: *sql.Rows is single-use and the engine may iterate
	// more than once (e.g. on re-sync).
	eng.RegisterMultihashLister(func(ctx context.Context, p peer.ID, ctxID []byte) (provider.MultihashIterator, error) {
		return newSQLiteMHIterator(ctx, db)
	})

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("advertise: engine start: %w", err)
	}
	defer func() {
		if err := eng.Shutdown(); err != nil {
			log.Printf("advertise: engine shutdown: %v", err)
		}
	}()

	// Serve the ad chain. mux pattern "/ipni/" is a subtree match, so the
	// indexer's GET /ipni/v1/ad/<cid> routes to the publisher handler, which
	// keys off path.Base. Must be up before we announce.
	handlerFunc, err := eng.GetPublisherHttpFunc()
	if err != nil {
		return fmt.Errorf("advertise: publisher handler: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(ipniPath, handlerFunc)
	adServer := &http.Server{Addr: listenURL.Host, Handler: mux}
	go func() {
		if err := adServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("advertise: ad-chain server: %v", err)
		}
	}()
	defer adServer.Close()

	md := metadata.Default.New(metadata.IpfsGatewayHttp{})
	adCid, err := eng.NotifyPut(ctx, nil, []byte(contextID), md)
	switch {
	case errors.Is(err, provider.ErrAlreadyAdvertised):
		log.Printf("advertise: already advertised context %q; ad chain continues", contextID)
	case err != nil:
		return fmt.Errorf("advertise: notify put: %w", err)
	default:
		log.Printf("advertise: published advertisement %s for context %q", adCid, contextID)
	}

	log.Printf("advertise: advertising %s over ipfs-gateway-http; serving ad chain on %s%s", opt.PublicAddr, opt.Listen, ipniPath)
	log.Printf("advertise: announced to %s", opt.AnnounceURL)

	<-ctx.Done()
	log.Println("advertise: shutting down...")
	return nil
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
