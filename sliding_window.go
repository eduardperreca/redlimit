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
// Every allowed request is stored in a sorted set, scored by the time Redis
// read from its own clock. The decision runs in a Lua script, so it is exact
// under concurrency.
type SlidingWindow struct {
	rdb    redis.UniversalClient
	limit  int
	window time.Duration
	opts   options
}

// NewSlidingWindow returns a limiter that allows limit requests in any
// window-long span. Memory grows with limit: one sorted set entry per request.
// It needs Redis 7.0 or later. It panics if limit < 1 or window < 1ms.
func NewSlidingWindow(rdb redis.UniversalClient, limit int, window time.Duration, opts ...Option) *SlidingWindow {
	checkWindow(limit, window)
	return &SlidingWindow{rdb: rdb, limit: limit, window: window, opts: newOptions(opts)}
}

// Times are in microseconds. Members only need to be unique, and ARGV[4] is
// a random string from the client: Redis seeds math.random the same way on
// every call.
var slidingWindowScript = redis.NewScript(`
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
if count + n > limit then
  -- There is room for n once the oldest count + n - limit entries have aged out.
  local i = count + n - limit - 1
  local z = redis.call('ZRANGE', KEYS[1], i, i, 'WITHSCORES')
  return {0, limit - count, z[2] + window - now}
end

for i = 1, n do
  redis.call('ZADD', KEYS[1], now, ARGV[4] .. '-' .. i)
end
redis.call('PEXPIRE', KEYS[1], math.ceil(window / 1000))
return {1, limit - count - n, 0}
`)

// Allow implements Limiter.
func (l *SlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN is Allow for a request that counts as n. It returns ErrExceedsLimit
// if n > limit, and panics if n < 1.
func (l *SlidingWindow) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if err := checkN(n, l.limit); err != nil {
		return Result{Limit: l.limit}, err
	}
	v, err := slidingWindowScript.Run(ctx, l.rdb, []string{l.opts.key(key, "sw")},
		l.window.Microseconds(), l.limit, n, strconv.FormatUint(rand.Uint64(), 36)).Int64Slice()
	if err != nil {
		return l.opts.failed(l.limit), err
	}
	return Result{
		Allowed:    v[0] == 1,
		Limit:      l.limit,
		Remaining:  max(int(v[1]), 0), // negative if the limit was lowered
		RetryAfter: time.Duration(v[2]) * time.Microsecond,
		ResetIn:    l.window,
	}, nil
}
