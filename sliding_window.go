package redlimit

import (
	"context"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// SlidingWindow allows up to limit requests per key in any window-long span.
//
// Every request is stored in a sorted set, scored by the time the client read
// from its own clock. Application servers whose clocks disagree will count the
// same span differently.
type SlidingWindow struct {
	rdb    redis.UniversalClient
	limit  int
	window time.Duration
	opts   options
}

// NewSlidingWindow returns a limiter that allows limit requests in any
// window-long span. Memory grows with limit: one sorted set entry per request.
// It panics if limit < 1 or window < 1ms.
func NewSlidingWindow(rdb redis.UniversalClient, limit int, window time.Duration, opts ...Option) *SlidingWindow {
	checkWindow(limit, window)
	return &SlidingWindow{rdb: rdb, limit: limit, window: window, opts: newOptions(opts)}
}

// Allow implements Limiter.
func (l *SlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	k := l.opts.key(key, "sw")
	now := time.Now().UnixMicro()
	// Concurrent requests can read the same microsecond, so the member needs a
	// random part. Otherwise ZADD would overwrite instead of add.
	member := strconv.FormatInt(now, 10) + "-" + strconv.FormatUint(rand.Uint64(), 36)

	var count *redis.IntCmd
	var oldest *redis.ZSliceCmd
	// MULTI/EXEC runs the commands as one unit, but cannot branch on their
	// results. The request is added before we know whether it is allowed.
	_, err := l.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRemRangeByScore(ctx, k, "-inf", strconv.FormatInt(now-l.window.Microseconds(), 10))
		p.ZAdd(ctx, k, redis.Z{Score: float64(now), Member: member})
		count = p.ZCard(ctx, k)
		oldest = p.ZRangeWithScores(ctx, k, 0, 0)
		p.PExpire(ctx, k, l.window)
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
		ResetIn:   l.window,
	}
	if res.Allowed {
		return res, nil
	}

	// A rejected request must not use up the limit, so take it back out. Until
	// then it is counted, and a concurrent request can be rejected because of
	// it. That errs on the strict side: it never lets more than limit through.
	// If ZREM fails, the entry ages out with the window like any other; the
	// rejection itself is still correct, so the error is not returned.
	_ = l.rdb.ZRem(ctx, k, member).Err()
	if z := oldest.Val(); len(z) > 0 {
		res.RetryAfter = time.Duration(int64(z[0].Score)+l.window.Microseconds()-now) * time.Microsecond
	}
	return res, nil
}
