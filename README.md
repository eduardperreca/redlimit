# redlimit

Rate limiting for Go, backed by Redis.

```sh
go get github.com/eduardperreca/redlimit
```

Requires Redis 7.0 or later. Valkey works too, since only standard commands are used.

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
middleware logs them with `log/slog` and responds `503 Service Unavailable` in
fail-closed mode.

## HTTP headers

Every response that went through the limiter carries:

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
| Sliding window | `limit`        | one entry per request  | `MULTI`/`EXEC` | client         |
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

Each request is an entry in a sorted set, scored by its timestamp. A request is
allowed if fewer than `limit` entries are younger than `window`.

```
MULTI
ZREMRANGEBYSCORE rl:{key}:sw -inf <now - window>
ZADD             rl:{key}:sw <now> <now>-<random>
ZCARD            rl:{key}:sw
ZRANGE           rl:{key}:sw 0 0 WITHSCORES
PEXPIRE          rl:{key}:sw <window>
EXEC
```

`MULTI` queues the commands and runs them with nothing else in between, but it
cannot use one command's result in the next. So the request is added before it
is known to be allowed, and removed with a separate `ZREM` if it was rejected.
In the moment between the two, a concurrent request sees one entry too many and
can be rejected too. The limiter can be stricter than `limit` under contention,
never looser.

`now` comes from the application server's clock. If two servers disagree by a
second, one of them sees entries a second older than they are. Keep the clocks
in sync with NTP, or use the token bucket.

### Token bucket

`NewTokenBucket(rdb, rate, burst)`

Each key has a bucket holding up to `burst` tokens, refilled at `rate` tokens
per second. A request takes one token, or is rejected if there is none.

The state is a hash with the token count and the time of the last update. The
refill depends on the previous state, which `MULTI` cannot express, so the
logic runs as a Lua script. Redis runs a script atomically. The script is sent
once with `EVAL`. After that, the client calls it by SHA1 with `EVALSHA`.

The script reads the time with Redis `TIME`, so all application servers share
one clock.

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

## License

MIT
