// SPDX-License-Identifier: BSD-3-Clause

package carindex

import (
	"context"
	"sort"
	"sync"
)

type ctxKey struct{}

// AccessCollector accumulates, per request, the distinct car files read and the
// total bytes served. Safe for concurrent block reads within one request.
type AccessCollector struct {
	mu    sync.Mutex
	cars  map[string]struct{}
	bytes int64
}

func NewAccessCollector() *AccessCollector { return &AccessCollector{cars: map[string]struct{}{}} }

// WithCollector returns a context carrying c, for the gateway middleware.
func WithCollector(ctx context.Context, c *AccessCollector) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}
func collectorFrom(ctx context.Context) *AccessCollector {
	c, _ := ctx.Value(ctxKey{}).(*AccessCollector)
	return c
}

func (c *AccessCollector) record(carPath string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cars[carPath] = struct{}{}
	c.bytes += n
}

// Cars returns the sorted distinct car paths read. Bytes returns total bytes.
func (c *AccessCollector) Cars() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.cars))
	for k := range c.cars {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func (c *AccessCollector) Bytes() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.bytes }
