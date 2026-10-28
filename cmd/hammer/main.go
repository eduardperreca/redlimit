// Command hammer sends concurrent requests through a redlimit limiter and
// prints, every second, how many were allowed, rejected and failed.
//
// One address connects to a single Redis, several to a Redis Cluster.
// With -master, the addresses are Sentinels.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eduardperreca/redlimit"
	"github.com/redis/go-redis/v9"
)

func main() {
	addrs := flag.String("addrs", "localhost:6379", "comma-separated Redis addresses")
	master := flag.String("master", "", "Sentinel master name; -addrs are then Sentinel addresses")
	algo := flag.String("algo", "tb", "fw (fixed window), sw (sliding window) or tb (token bucket)")
	limit := flag.Int("limit", 10, "fw, sw: requests per window; tb: burst")
	window := flag.Duration("window", time.Second, "fw, sw: window length")
	rate := flag.Float64("rate", 10, "tb: tokens per second")
	keys := flag.Int("keys", 10, "number of distinct keys")
	workers := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 30*time.Second, "how long to run")
	timeout := flag.Duration("timeout", 0, "per-request context timeout; 0 means none")
	clientTimeout := flag.Duration("client-timeout", 0, "go-redis dial, read and write timeout; 0 keeps the go-redis defaults")
	reload := flag.Duration("reload", 0, "Cluster only: how often to reload the slot map; 0 keeps the go-redis default of 60s")
	run := flag.String("run", strconv.FormatInt(time.Now().Unix(), 36), "key namespace; pass an earlier run's value to keep counting on its keys")
	flag.Parse()
	if *reload > 0 && *master != "" {
		log.Fatal("-reload is for Redis Cluster and cannot be combined with -master")
	}

	opt := &redis.UniversalOptions{
		Addrs:        strings.Split(*addrs, ","),
		MasterName:   *master,
		DialTimeout:  *clientTimeout,
		ReadTimeout:  *clientTimeout,
		WriteTimeout: *clientTimeout,
	}
	var rdb redis.UniversalClient
	if *reload > 0 {
		// UniversalOptions has no field for the reload interval.
		copt := opt.Cluster()
		copt.ClusterStateReloadInterval = *reload
		rdb = redis.NewClusterClient(copt)
	} else {
		rdb = redis.NewUniversalClient(opt)
	}
	defer rdb.Close()

	var lim redlimit.Limiter
	var perSec float64
	switch *algo {
	case "fw":
		lim = redlimit.NewFixedWindow(rdb, *limit, *window)
		perSec = float64(*limit) / window.Seconds()
	case "sw":
		lim = redlimit.NewSlidingWindow(rdb, *limit, *window)
		perSec = float64(*limit) / window.Seconds()
	case "tb":
		lim = redlimit.NewTokenBucket(rdb, *rate, *limit)
		perSec = *rate
	default:
		log.Fatalf("unknown -algo %q", *algo)
	}
	fmt.Printf("run %s: %s, %d keys, %d workers: expect about %.0f allowed/s\n\n", *run, *algo, *keys, *workers, perSec*float64(*keys))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	var allowed, rejected, failed atomic.Int64
	var lastErr atomic.Value

	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				res, err := allow(ctx, lim, "hammer:"+*run+":"+strconv.Itoa(rand.IntN(*keys)), *timeout)
				switch {
				case ctx.Err() != nil:
				case err != nil:
					lastErr.Store(err.Error()) // before Add: the ticker reads it once the count moves
					failed.Add(1)
					time.Sleep(10 * time.Millisecond) // don't spin while Redis is down
				case res.Allowed:
					allowed.Add(1)
				default:
					rejected.Add(1)
				}
			}
		})
	}

	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var a0, r0, f0 int64
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			a, r, f := allowed.Load(), rejected.Load(), failed.Load()
			line := fmt.Sprintf("%4.0fs  allowed %6d  rejected %7d  errors %6d", time.Since(start).Seconds(), a-a0, r-r0, f-f0)
			if f > f0 {
				line += "  last error: " + lastErr.Load().(string)
			}
			fmt.Println(line)
			a0, r0, f0 = a, r, f
		}
	}
	wg.Wait()

	secs := time.Since(start).Seconds()
	fmt.Printf("\ntotal  allowed %d (%.0f/s)  rejected %d  errors %d\n", allowed.Load(), float64(allowed.Load())/secs, rejected.Load(), failed.Load())
}

func allow(ctx context.Context, lim redlimit.Limiter, key string, timeout time.Duration) (redlimit.Result, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return lim.Allow(ctx, key)
}
