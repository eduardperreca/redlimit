package redlimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// FixedWindow allows up to limit requests per key in each window.
//
// A window opens on the first request for a key and closes when its counter
// expires. Windows are not aligned to the wall clock, so no clock is read.
// A client can get up to 2*limit requests through around the moment one
// window closes and the next opens.
type FixedWindow struct {
	rdb    redis.UniversalClient
	limit  int
	window time.Duration
	opts   options
}

// NewFixedWindow returns a limiter that allows limit requests per window.
// It needs Redis 7.0 or later.
func NewFixedWindow(rdb redis.UniversalClient, limit int, window time.Duration, opts ...Option) *FixedWindow {
	return &FixedWindow{rdb: rdb, limit: limit, window: window, opts: newOptions(opts)}
}

func (l *FixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	k := l.opts.key(key, "fw")

	var count *redis.IntCmd
	var ttl *redis.DurationCmd
	// One round trip, not a transaction. INCR alone keeps the count exact under
	// concurrency. PEXPIRE NX only sets the TTL on a key that has none, so the
	// first request opens the window and later ones do not extend it. If the
	// client dies between INCR and PEXPIRE, the next request sets the TTL.
	_, err := l.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		count = p.Incr(ctx, k)
		p.Do(ctx, "PEXPIRE", k, l.window.Milliseconds(), "NX")
		ttl = p.PTTL(ctx, k)
		return nil
	})
	if err != nil {
		return l.opts.failed(l.limit), err
	}

	n := int(count.Val())
	res := Result{
		Allowed:   n <= l.limit,
		Limit:     l.limit,
		Remaining: max(l.limit-n, 0),
		ResetIn:   ttl.Val(),
	}
	if !res.Allowed {
		res.RetryAfter = res.ResetIn
	}
	return res, nil
}
