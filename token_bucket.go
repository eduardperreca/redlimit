package redlimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenBucket gives each key a bucket of burst tokens that refills at rate
// tokens per second. A request takes one token, or n with AllowN.
//
// The whole decision runs in a Lua script, which Redis executes atomically.
// The script reads the time from Redis, so the clocks of application servers
// do not matter.
type TokenBucket struct {
	rdb   redis.UniversalClient
	rate  float64
	burst int
	opts  options
}

// NewTokenBucket returns a limiter that refills rate tokens per second, up to burst.
// It needs Redis 7.0 or later: older versions reject writes after TIME in a script.
// It panics if rate <= 0 or burst < 1.
func NewTokenBucket(rdb redis.UniversalClient, rate float64, burst int, opts ...Option) *TokenBucket {
	if !(rate > 0) || burst < 1 { // !(rate > 0) also catches NaN
		panic("redlimit: token bucket needs rate > 0 and burst >= 1")
	}
	return &TokenBucket{rdb: rdb, rate: rate, burst: burst, opts: newOptions(opts)}
}

// Lua numbers are floats, but Redis truncates them to integers on return, so
// every returned value is already in whole tokens or microseconds.
// A missing key means a full bucket, which is why the key may expire once full.
var tokenBucketScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or burst
local ts = tonumber(b[2]) or now
-- TIME can go backwards: NTP can step the clock, and a failover can move the
-- key to a node whose clock is behind. Treat that as no time passing.
tokens = math.min(burst, tokens + math.max(0, now - ts) / 1000000 * rate)

local allowed = 0
local retry = 0
if tokens >= n then
  tokens = tokens - n
  allowed = 1
else
  retry = math.ceil((n - tokens) / rate * 1000000)
end

local full = math.ceil((burst - tokens) / rate * 1000000)
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.max(1, math.ceil(full / 1000)))
return {allowed, math.floor(tokens), full, retry}
`)

// Allow implements Limiter.
func (l *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN is Allow for a request that takes n tokens at once. It returns
// ErrExceedsLimit if n > burst, and panics if n < 1.
func (l *TokenBucket) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if err := checkN(n, l.burst); err != nil {
		return Result{Limit: l.burst}, err
	}
	// Run sends EVALSHA and falls back to EVAL when Redis has not cached the
	// script yet, so the script body crosses the network only once per node.
	v, err := tokenBucketScript.Run(ctx, l.rdb, []string{l.opts.key(key, "tb")}, l.rate, l.burst, n).Int64Slice()
	if err != nil {
		return l.opts.failed(l.burst), err
	}
	return Result{
		Allowed:    v[0] == 1,
		Limit:      l.burst,
		Remaining:  int(v[1]),
		ResetIn:    time.Duration(v[2]) * time.Microsecond,
		RetryAfter: time.Duration(v[3]) * time.Microsecond,
	}, nil
}

// Reset forgets every request made with key, as after a successful login.
func (l *TokenBucket) Reset(ctx context.Context, key string) error {
	return l.rdb.Del(ctx, l.opts.key(key, "tb")).Err()
}
