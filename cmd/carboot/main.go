// SPDX-License-Identifier: BSD-3-Clause

// carboot is a single static binary with three subcommands:
//
//	carboot gateway    serve a CAR directory as a trustless IPFS gateway
//	carboot reindex    build/update the SQLite index from a CAR directory
//	carboot advertise  advertise every indexed block to IPNI (cid.contact)
//
// The gateway and reindex subcommands accept env fallbacks (CARBOOT_INDEX,
// CARBOOT_CARS_DIR, CARBOOT_REINDEX_ON_START) for container ergonomics.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/dannyob/carboot/internal/advertise"
	"github.com/dannyob/carboot/internal/gateway"
	"github.com/dannyob/carboot/internal/reindex"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gateway":
		runGateway(os.Args[2:])
	case "reindex":
		runReindex(os.Args[2:])
	case "advertise":
		runAdvertise(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "carboot: unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: carboot <subcommand> [flags]

subcommands:
  gateway    serve a CAR directory as a trustless IPFS gateway
  reindex    build/update the SQLite index from a CAR directory
  advertise  advertise every indexed block to IPNI

run "carboot <subcommand> -h" for the flags of each subcommand.
`)
}

func runGateway(args []string) {
	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	index := fs.String("index", envOr("CARBOOT_INDEX", ""), "path to the SQLite index (required; env CARBOOT_INDEX)")
	carsDir := fs.String("cars-dir", envOr("CARBOOT_CARS_DIR", ""), "directory containing CAR files (required; env CARBOOT_CARS_DIR)")
	listen := fs.String("listen", ":3747", "HTTP listen address")
	reindexOnStart := fs.Bool("reindex-on-start", envBool("CARBOOT_REINDEX_ON_START", true), "run a reindex pass before serving (env CARBOOT_REINDEX_ON_START)")
	fs.Parse(args)

	if *index == "" || *carsDir == "" {
		log.Fatal("carboot gateway: --index and --cars-dir are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opt := gateway.Options{
		Index:          *index,
		CarsDir:        *carsDir,
		Listen:         *listen,
		ReindexOnStart: *reindexOnStart,
		LogOut:         os.Stdout,
	}
	if err := gateway.Serve(ctx, opt); err != nil {
		log.Fatalf("carboot gateway: %v", err)
	}
}

func runReindex(args []string) {
	fs := flag.NewFlagSet("reindex", flag.ExitOnError)
	index := fs.String("index", envOr("CARBOOT_INDEX", ""), "path to the SQLite index (required; env CARBOOT_INDEX)")
	carsDir := fs.String("cars-dir", envOr("CARBOOT_CARS_DIR", ""), "directory containing CAR files (required; env CARBOOT_CARS_DIR)")
	fs.Parse(args)

	if *index == "" || *carsDir == "" {
		log.Fatal("carboot reindex: --index and --cars-dir are required")
	}

	if err := reindex.Run(*index, *carsDir); err != nil {
		log.Fatalf("carboot reindex: %v", err)
	}
}

func runAdvertise(args []string) {
	fs := flag.NewFlagSet("advertise", flag.ExitOnError)
	index := fs.String("index", "", "path to the SQLite index (required)")
	listen := fs.String("listen", "0.0.0.0:3104", "host:port to bind the IPNI ad-chain server on")
	publicAddr := fs.String("public-addr", "", "the GATEWAY's public URL advertised to IPNI as the content provider, e.g. http://1.2.3.4:3747 (required)")
	publisherAddr := fs.String("publisher-addr", "", "THIS process's public URL where the indexer fetches the ad chain, e.g. http://1.2.3.4:3104 (required; must be publicly reachable)")
	announceURL := fs.String("announce-url", "https://cid.contact/ingest/announce", "indexer announce endpoint")
	identityPath := fs.String("identity", "", "path to the persisted Ed25519 identity key (default ~/.carboot/key)")
	datastorePath := fs.String("datastore", "", "path to the persistent ad-chain datastore (default ~/.carboot/adstore)")
	fs.Parse(args)

	if *index == "" || *publicAddr == "" || *publisherAddr == "" {
		log.Fatal("carboot advertise: --index, --public-addr and --publisher-addr are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opt := advertise.Options{
		IndexPath:     *index,
		Listen:        *listen,
		PublicAddr:    *publicAddr,
		PublisherAddr: *publisherAddr,
		AnnounceURL:   *announceURL,
		IdentityPath:  *identityPath,
		DatastorePath: *datastorePath,
	}
	if err := advertise.Run(ctx, opt); err != nil {
		log.Fatalf("carboot advertise: %v", err)
	}
}

// envOr returns the value of env var key, or def if unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBool parses a boolean env var (1/0/true/false), or def if unset/unparseable.
func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
