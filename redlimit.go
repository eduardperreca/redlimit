// Package redlimit implements rate limiters backed by Redis.
package redlimit

import (
	"context"
	"errors"
	"time"
)

// Limiter decides whether a request identified by key may proceed.
//
// On a Redis error, Allow returns the error together with a Result whose
// Allowed field follows the fail mode: true by default, false with FailClosed.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

// ErrExceedsLimit is returned by AllowN when n is larger than the limit, or
// than the burst of a token bucket. Such a request can never be allowed, so it
// is rejected whatever the fail mode, without asking Redis.
var ErrExceedsLimit = errors.New("redlimit: n exceeds the limit")

// Result describes the outcome of a single Allow call.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration // zero when Allowed
	ResetIn    time.Duration // time until the limit fully resets
}

// Option configures a limiter.
type Option func(*options)

// FailClosed rejects requests when Redis cannot be reached.
// The default is to let them through.
func FailClosed() Option {
	return func(o *options) { o.failClosed = true }
}

// Prefix sets the prefix of every Redis key the limiter writes. The default is "rl".
func Prefix(p string) Option {
	return func(o *options) { o.prefix = p }
}

type options struct {
	prefix     string
	failClosed bool
}

func newOptions(opts []Option) options {
	o := options{prefix: "rl"}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// key wraps the caller's key in a hash tag so that every Redis key belonging
// to it lands in the same Redis Cluster slot.
func (o options) key(key, algo string) string {
	return o.prefix + ":{" + key + "}:" + algo
}

// checkWindow rejects settings that would silently disable limiting:
// PEXPIRE takes milliseconds, and a TTL of 0 deletes the key.
func checkWindow(limit int, window time.Duration) {
	if limit < 1 || window < time.Millisecond {
		panic("redlimit: window limiters need limit >= 1 and window >= 1ms")
	}
}

// checkN validates the n passed to AllowN. n < 1 is a programming error;
// n > limit can come from user input, so it is an error instead of a panic.
func checkN(n, limit int) error {
	if n < 1 {
		panic("redlimit: AllowN needs n >= 1")
	}
	if n > limit {
		return ErrExceedsLimit
	}
	return nil
}

func (o options) failed(limit int) Result {
	return Result{Allowed: !o.failClosed, Limit: limit}
}
