# redlimit

[![ci](https://github.com/eduardperreca/redlimit/actions/workflows/ci.yml/badge.svg)](https://github.com/eduardperreca/redlimit/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eduardperreca/redlimit.svg)](https://pkg.go.dev/github.com/eduardperreca/redlimit)

Rate limiting for Go, backed by Redis.

```sh
go get github.com/eduardperreca/redlimit
```

Requires Go 1.25 and Redis 7.0 or later. Tested with Redis 7.0, Redis 8 and
Valkey 8.

## Usage

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

// 100 requests per minute per API key.
lim := redlimit.NewFixedWindow(rdb, 100, time.Minute)

mux := http.NewServeMux()
mux.HandleFunc("/", handler)

limit := redlimit.Middleware(lim, func(r *http.Request) string {
	return r.Header.Get("X-API-Key")
})
http.ListenAndServe(":8080", limit(mux))
```

Without the middleware:

```go
res, err := lim.Allow(ctx, "user:42")
if !res.Allowed {
	// reject; retry in res.RetryAfter
}
```

Limiters take a `redis.UniversalClient`, so `*redis.Client`, `*redis.ClusterClient`
and the client returned by `redis.NewFailoverClient` all work.

## Choosing the key

The middleware has no default key. You have to say what identifies a client.

Limiting by IP is common, but `r.RemoteAddr` is the address of whatever opened
the TCP connection. Behind a load balancer or reverse proxy, that is the proxy,
so every client shares one limit. Take the client address from the header your
proxy sets, and only trust that header if the proxy overwrites it:

```go
func(r *http.Request) string { return r.Header.Get("X-Real-IP") }
```

## When Redis is unavailable

By default requests are let through when Redis cannot be reached (fail open).
Rate limiting stops, but your service keeps working.

```go
lim := redlimit.NewFixedWindow(rdb, 5, time.Minute, redlimit.FailClosed())
```

`FailClosed` rejects requests instead. Use it when letting traffic through
unchecked is worse than rejecting it, as with login attempts.

`Allow` always returns the error, so you can log or count failures. The
middleware logs them with `log/slog`, at most once per second with a count of
the ones it skipped, and responds `503 Service Unavailable` in fail-closed mode.

## Timeouts

Fail-open only helps if `Allow` returns. With go-redis defaults, a dead
primary makes every call hang for seconds while the client retries. Give the
client short timeouts:

```go
rdb := redis.NewClient(&redis.Options{
	Addr:         "localhost:6379",
	DialTimeout:  100 * time.Millisecond,
	ReadTimeout:  100 * time.Millisecond,
	WriteTimeout: 100 * time.Millisecond,
})
```

With Redis Cluster, also set `ClusterStateReloadInterval` to about a second.
Otherwise the client keeps sending requests to a dead primary for up to
60 seconds after a replica has replaced it.

[docs/experiments.md](docs/experiments.md) has the measurements behind this, and
what eviction policies and persistence settings do to the counters.

## HTTP headers

Every response the limiter decided on carries the headers below. When the
limiter fails, they are left out.

| Header                | Value                                      |
| --------------------- | ------------------------------------------ |
| `RateLimit-Limit`     | requests per window, or burst for the token bucket |
| `RateLimit-Remaining` | requests that can be made right now                |
| `RateLimit-Reset`     | seconds until the full limit is available again    |
| `Retry-After`         | seconds to wait, on `429` responses only           |

The `RateLimit-*` names follow earlier drafts of
[draft-ietf-httpapi-ratelimit-headers](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/).
Seconds are rounded up.

## Algorithms

|                | Burst at most  | Redis memory per key   | Atomicity      | Clock          |
| -------------- | -------------- | ---------------------- | -------------- | -------------- |
| Fixed window   | `2 * limit`    | one counter            | single command | none (TTL)     |
| Sliding window | `limit`        | one entry per request  | Lua script     | Redis `TIME`   |
| Token bucket   | `burst`        | one hash, two fields   | Lua script     | Redis `TIME`   |

Fixed window is the cheapest. Sliding window is exact, but memory grows with the
limit, so keep it for small limits. Token bucket allows short bursts while
holding an average rate, and is the usual choice for APIs.

### Fixed window

`NewFixedWindow(rdb, limit, window)`

Each key gets a counter with a TTL. The first request creates the counter and
starts the window; the window ends when the counter expires.

Per request, one round trip:

```
INCR    rl:{key}:fw
PEXPIRE rl:{key}:fw <window> NX
PTTL    rl:{key}:fw
```

`INCR` is atomic, so concurrent requests never over-count. `NX` sets the TTL
only on a key that has none. Without it, every request would push the expiry
back and the window would never close for a client that keeps sending.

The cost of the simplicity: a client can send `limit` requests at the end of one
window and `limit` more at the start of the next, so up to `2 * limit` in a
short burst.

### Sliding window

`NewSlidingWindow(rdb, limit, window)`

Each allowed request is an entry in a sorted set, scored by its timestamp. A
request is allowed if fewer than `limit` entries are younger than `window`.
The check runs as a Lua script, one round trip:

```
ZREMRANGEBYSCORE rl:{key}:sw -inf <now - window>
ZCARD            rl:{key}:sw
ZADD             rl:{key}:sw <now> <random>      only if allowed
PEXPIRE          rl:{key}:sw <window>            only if allowed
```

An earlier version used `MULTI`/`EXEC` instead. `MULTI` queues commands and
runs them with nothing else in between, but it cannot use one command's result
in the next. So the request had to be added before it was known to be allowed,
and removed with a separate `ZREM` if it was rejected. That cost a second round
trip for every rejected request, and in the moment between the two, concurrent
requests saw one entry too many. A script can check first and add after.

`now` is read with Redis `TIME`, as in the token bucket, so the clocks of the
application servers do not matter.

### Token bucket

`NewTokenBucket(rdb, rate, burst)`

Each key has a bucket holding up to `burst` tokens, refilled at `rate` tokens
per second. A request takes one token, or is rejected if there is none.

The state is a hash with the token count and the time of the last update. The
refill depends on the previous state, which `MULTI` cannot express, so the
logic runs as a Lua script. Redis runs a script atomically. The client calls
the script by its SHA1 with `EVALSHA`, and sends the full body with `EVAL` only
when Redis answers that it does not have it cached, as after a restart or a
failover.

The script reads the time with Redis `TIME`, so all application servers share
one clock. If that clock goes backwards, which NTP or a failover to a node with
a slower clock can cause, the script counts it as no time passing.

The key expires when the bucket would be full again. A missing key reads as a
full bucket.

## Keys

Keys look like `rl:{<your key>}:<algorithm>`. Change `rl` with
`redlimit.Prefix("myapp")`. The braces are a Redis Cluster hash tag: every key
for one client lands in the same slot.

## Development

```sh
docker compose up -d
REDIS_ADDR=localhost:6379 go test -race ./...
```

### Load testing with hammer

`cmd/hammer` runs concurrent requests through a limiter and prints what
happened every second:

```sh
go run ./cmd/hammer -algo tb -rate 10 -limit 10 -keys 10 -c 50 -d 30s
```

```
run tnmoo5: tb, 10 keys, 50 workers: expect about 100 allowed/s

   1s  allowed    170  rejected   24626  errors      0
   2s  allowed    100  rejected   35797  errors      0
```

The first second is higher because every bucket starts full. Run
`go run ./cmd/hammer -h` for all flags.

### Sentinel and Cluster

`compose.yaml` has two more profiles:

```sh
docker compose --profile sentinel up -d   # primary, 2 replicas, 3 Sentinels
docker compose --profile cluster up -d    # 3 primaries, 3 replicas
```

Both hand out container IP addresses to clients, and the host cannot always
reach those. Run hammer inside the compose network instead:

```sh
docker compose run --rm hammer -addrs sentinel1:26379,sentinel2:26379 -master mymaster
docker compose run --rm hammer -addrs node1:6379,node2:6379,node3:6379
```

The cluster needs a few seconds after `up` to assign slots. Until
`docker compose exec node1 redis-cli cluster info` shows `cluster_state:ok`,
requests fail with `CLUSTERDOWN`.

## License

MIT
