// SPDX-License-Identifier: AGPL-3.0-or-later

// cargw serves a directory of CAR shards as an IPFS trustless gateway, backed
// by a SQLite index. It is read-only and offline (no Bitswap, no network block
// fetching): boxo's gateway resolves /ipfs/{cid} requests against the index.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipfs/boxo/blockservice"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"

	"github.com/dannyob/carboot/internal/carindex"
)

func main() {
	indexPath := flag.String("index", "", "path to the SQLite index (required)")
	carsDir := flag.String("cars-dir", "", "directory containing CAR shard files (required)")
	listen := flag.String("listen", ":3747", "HTTP listen address")
	flag.Parse()

	if *indexPath == "" || *carsDir == "" {
		log.Fatal("cargw: --index and --cars-dir are required")
	}

	bs, err := carindex.Open(*indexPath, *carsDir)
	if err != nil {
		log.Fatalf("cargw: open index: %v", err)
	}
	defer bs.Close()

	exch := offline.Exchange(bs) // offline: never reaches the network for a missing block
	bsvc := blockservice.New(bs, exch)

	backend, err := gateway.NewBlocksBackend(bsvc)
	if err != nil {
		log.Fatalf("cargw: new backend: %v", err)
	}

	cfg := gateway.Config{
		// trustless-only: serves ?format=raw and ?format=car. No UnixFS
		// deserialization (we never reassemble files for humans).
		DeserializedResponses: false,
		NoDNSLink:             true,
	}
	gwHandler := gateway.NewHandler(cfg, backend)

	mux := http.NewServeMux()
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)

	handler := logRequests(mux)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("cargw: serving trustless gateway on %s (index=%s cars-dir=%s)", *listen, *indexPath, *carsDir)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("cargw: serve: %v", err)
		}
	case <-ctx.Done():
		log.Println("cargw: shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("cargw: shutdown error: %v", err)
		}
	}
}

// logRequests logs each request line and the time it took.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s%s -> %d (%s)", r.Method, r.URL.Path, queryString(r), sw.status, time.Since(start))
	})
}

func queryString(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return ""
	}
	return "?" + r.URL.RawQuery
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}
