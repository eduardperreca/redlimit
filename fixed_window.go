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
// It needs Redis 7.0 or later. It panics if limit < 1 or window < 1ms.
func NewFixedWindow(rdb redis.UniversalClient, limit int, window time.Duration, opts ...Option) *FixedWindow {
	checkWindow(limit, window)
	return &FixedWindow{rdb: rdb, limit: limit, window: window, opts: newOptions(opts)}
}

// Allow implements Limiter.
func (l *FixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN is Allow for a request that counts as n. It returns ErrExceedsLimit
// if n > limit, and panics if n < 1.
func (l *FixedWindow) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if err := checkN(n, l.limit); err != nil {
		return Result{Limit: l.limit}, err
	}
	k := l.opts.key(key, "fw")
	ms := l.window.Milliseconds()

	var count *redis.IntCmd
	var ttl *redis.DurationCmd
	// One round trip, not a transaction. INCRBY alone keeps the count exact
	// under concurrency. PEXPIRE NX only sets the TTL on a key that has none,
	// so the first request opens the window and later ones do not extend it.
	// If the client dies between INCRBY and PEXPIRE, the next request sets it.
	_, err := l.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		count = p.IncrBy(ctx, k, int64(n))
		p.Do(ctx, "PEXPIRE", k, ms, "NX")
		ttl = p.PTTL(ctx, k)
		return nil
	})
	if err != nil {
		return l.opts.failed(l.limit), err
	}

	c := int(count.Val())
	res := Result{
		Allowed:   c <= l.limit,
		Limit:     l.limit,
		Remaining: max(l.limit-c, 0),
		ResetIn:   ttl.Val(),
	}
	if res.Allowed {
		return res, nil
	}
	res.RetryAfter = res.ResetIn
	if before := c - n; before < l.limit {
		// The window had room, just not for n, so give the n back for smaller
		// requests to use. A single request is only rejected once the window
		// is full, so Allow never gets here. If the window closes between the
		// two round trips, DECRBY creates a negative counter and the next
		// window allows up to n more; PEXPIRE NX makes sure it still expires.
		// If this fails, the window stays n short until it closes. The
		// rejection itself is still correct, so the error is not returned.
		_, _ = l.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.DecrBy(ctx, k, int64(n))
			p.Do(ctx, "PEXPIRE", k, ms, "NX")
			return nil
		})
		res.Remaining = l.limit - before
	}
	return res, nil
}

// Reset forgets every request made with key, as after a successful login.
func (l *FixedWindow) Reset(ctx context.Context, key string) error {
	return l.rdb.Del(ctx, l.opts.key(key, "fw")).Err()
}
