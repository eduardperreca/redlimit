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
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

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
