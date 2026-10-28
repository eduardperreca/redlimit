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
| `RateLimit-Limit`     | requests allowed per window                |
| `RateLimit-Remaining` | requests left in the current window        |
| `RateLimit-Reset`     | seconds until the window resets            |
| `Retry-After`         | seconds to wait, on `429` responses only   |

The `RateLimit-*` names follow earlier drafts of
[draft-ietf-httpapi-ratelimit-headers](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/).
Seconds are rounded up.

## Algorithms

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
