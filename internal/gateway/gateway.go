// SPDX-License-Identifier: BSD-3-Clause

// Package gateway wires the carindex blockstore into a boxo trustless gateway
// HTTP handler, adds a per-request JSON access log (which CAR files each request
// touched), and serves it with optional reindex-on-start.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"

	"github.com/dannyob/carboot/internal/carindex"
	"github.com/dannyob/carboot/internal/reindex"
)

// Options configures Serve.
type Options struct {
	Index, CarsDir, Listen string
	ReindexOnStart         bool
	LogOut                 io.Writer
}

// accessLine is the one-per-request JSON access-log record.
type accessLine struct {
	TS     string   `json:"ts"`
	Remote string   `json:"remote_addr"`
	Method string   `json:"method"`
	CID    string   `json:"cid"`
	Format string   `json:"format"`
	Status int      `json:"status"`
	Bytes  int64    `json:"bytes"`
	Cars   []string `json:"cars"`
}

// Handler builds the boxo trustless-gateway http.Handler over the index, wrapped
// in the access-log middleware (one JSON line per request to logw).
func Handler(bs blockstore.Blockstore, logw io.Writer) (http.Handler, error) {
	exch := offline.Exchange(bs) // offline: never reaches the network for a missing block
	bsvc := blockservice.New(bs, exch)

	backend, err := gateway.NewBlocksBackend(bsvc)
	if err != nil {
		return nil, err
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

	return accessLog(mux, logw), nil
}

// accessLog wraps next, threading a request-scoped AccessCollector through the
// request context and emitting one JSON line per request to logw.
func accessLog(next http.Handler, logw io.Writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		col := carindex.NewAccessCollector()
		ctx := carindex.WithCollector(r.Context(), col)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))

		line := accessLine{
			TS:     time.Now().UTC().Format(time.RFC3339),
			Remote: r.RemoteAddr,
			Method: r.Method,
			CID:    cidFromPath(r.URL.Path),
			Format: r.URL.Query().Get("format"),
			Status: sw.status,
			Bytes:  col.Bytes(),
			Cars:   col.Cars(),
		}
		if b, err := json.Marshal(line); err == nil {
			logw.Write(append(b, '\n'))
		}
	})
}

// cidFromPath extracts the {cid} from /ipfs/{cid}/... (empty if not present).
func cidFromPath(p string) string {
	const prefix = "/ipfs/"
	if !strings.HasPrefix(p, prefix) {
		return ""
	}
	rest := p[len(prefix):]
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i]
	}
	return rest
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

// Serve opens the index (OpenRO), optionally runs reindex first, and serves
// until ctx is done.
func Serve(ctx context.Context, opt Options) error {
	if opt.ReindexOnStart {
		if err := reindex.Run(opt.Index, opt.CarsDir); err != nil {
			return err
		}
	}

	bs, err := carindex.Open(opt.Index, opt.CarsDir)
	if err != nil {
		return err
	}
	defer bs.Close()

	logw := opt.LogOut
	if logw == nil {
		logw = log.Writer()
	}
	handler, err := Handler(bs, logw)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              opt.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("carboot gateway: serving on %s (index=%s cars-dir=%s)", opt.Listen, opt.Index, opt.CarsDir)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
