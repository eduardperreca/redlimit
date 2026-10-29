package redlimit

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Fires 100 concurrent requests at a limit of 10. Exactly 10 must pass;
// any other number means the limiter is not atomic.
func TestConcurrentRequests(t *testing.T) {
	rdb := redisClient(t)

	limiters := map[string]Limiter{
		"fixed window":   NewFixedWindow(rdb, 10, time.Minute),
		"sliding window": NewSlidingWindow(rdb, 10, time.Minute),
		"token bucket":   NewTokenBucket(rdb, 0.01, 10),
	}
	for name, lim := range limiters {
		t.Run(name, func(t *testing.T) {
			key := t.Name() + strconv.FormatInt(time.Now().UnixNano(), 10)
			var allowed atomic.Int32
			var wg sync.WaitGroup
			for range 100 {
				wg.Go(func() {
					res, err := lim.Allow(context.Background(), key)
					if err != nil {
						t.Error(err)
					}
					if res.Allowed {
						allowed.Add(1)
					}
				})
			}
			wg.Wait()
			if got := allowed.Load(); got != 10 {
				t.Fatalf("allowed %d requests, want 10", got)
			}
		})
	}
}

// Requests of 4, 4, 4 and 2 against a limit of 10: the third does not fit,
// and must not use up the 2 that the fourth needs.
func TestAllowN(t *testing.T) {
	rdb := redisClient(t)
	limiters := map[string]interface {
		AllowN(context.Context, string, int) (Result, error)
	}{
		"fixed window":   NewFixedWindow(rdb, 10, time.Minute),
		"sliding window": NewSlidingWindow(rdb, 10, time.Minute),
		"token bucket":   NewTokenBucket(rdb, 0.01, 10),
	}
	for name, lim := range limiters {
		t.Run(name, func(t *testing.T) {
			key := t.Name() + strconv.FormatInt(time.Now().UnixNano(), 10)
			for i, want := range []struct {
				n         int
				allowed   bool
				remaining int
			}{{4, true, 6}, {4, true, 2}, {4, false, 2}, {2, true, 0}} {
				res, err := lim.AllowN(context.Background(), key, want.n)
				if err != nil || res.Allowed != want.allowed || res.Remaining != want.remaining {
					t.Fatalf("request %d: got allowed=%v remaining=%d err=%v, want allowed=%v remaining=%d",
						i, res.Allowed, res.Remaining, err, want.allowed, want.remaining)
				}
				if !res.Allowed && res.RetryAfter <= 0 {
					t.Fatalf("request %d: rejected with RetryAfter %v", i, res.RetryAfter)
				}
			}
			if _, err := lim.AllowN(context.Background(), key, 11); err != ErrExceedsLimit {
				t.Fatalf("n > limit: got err %v, want ErrExceedsLimit", err)
			}
		})
	}
}

// A failover can hand the bucket to a node whose clock is behind the one that
// last wrote it. Time going backwards must not take tokens away.
func TestTokenBucketClockBackwards(t *testing.T) {
	rdb := redisClient(t)
	ctx := context.Background()
	lim := NewTokenBucket(rdb, 1, 10)
	key := t.Name() + strconv.FormatInt(time.Now().UnixNano(), 10)

	ahead := time.Now().Add(time.Hour).UnixMicro()
	if err := rdb.HSet(ctx, lim.opts.key(key, "tb"), "tokens", 5, "ts", ahead).Err(); err != nil {
		t.Fatal(err)
	}
	res, err := lim.Allow(ctx, key)
	if err != nil || !res.Allowed || res.Remaining != 4 {
		t.Fatalf("got allowed=%v remaining=%d err=%v, want allowed with 4 remaining", res.Allowed, res.Remaining, err)
	}
}

func TestFailMode(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:1", MaxRetries: -1})
	defer rdb.Close()

	if res, err := NewFixedWindow(rdb, 10, time.Minute).Allow(context.Background(), "k"); err == nil || !res.Allowed {
		t.Errorf("fail open: got allowed=%v err=%v, want allowed with an error", res.Allowed, err)
	}
	if res, err := NewFixedWindow(rdb, 10, time.Minute, FailClosed()).Allow(context.Background(), "k"); err == nil || res.Allowed {
		t.Errorf("fail closed: got allowed=%v err=%v, want rejected with an error", res.Allowed, err)
	}
}

func redisClient(t *testing.T) *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}
