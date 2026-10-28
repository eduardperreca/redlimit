# Experiments

What happens to the limiter when Redis fails. Each experiment was run with the
setups in `compose.yaml` and `cmd/hammer`, on a laptop, with Redis 8.

The numbers are from single runs. Timings depend on the machine and on the
timeouts configured; what matters is the shape of each failure.

## Sentinel failover

Setup: one primary, two replicas, three Sentinels with
`down-after-milliseconds 5000`. Fixed window, 100 requests per key, 10 keys,
a window long enough that every key is exhausted in the first second. Any
`allowed` after the first second means a counter was lost.

```sh
docker compose --profile sentinel up -d
docker compose run --rm hammer -addrs sentinel1:26379,sentinel2:26379,sentinel3:26379 \
  -master mymaster -algo fw -limit 100 -window 10m -keys 10 -d 75s -timeout 100ms
docker compose kill primary   # in another terminal, while hammer runs
```

### Counters survive

Across three runs and four failovers, the total was always exactly 1000
allowed requests. Replication is asynchronous, so a write the primary
acknowledged but had not yet sent to a replica is lost when the primary dies.
For a rate limiter that means a few increments can disappear, and if that
takes a counter back under its limit, extra requests get through. It did not
show up here.

### Without a timeout, requests hang

With go-redis defaults, killing the primary at 9s gave this:

```
   8s  allowed      0  rejected  242964  errors      0
   9s  allowed      0  rejected  119153  errors      0
  10s  allowed      0  rejected       0  errors      0
  ...
  18s  allowed      0  rejected       0  errors      0
  19s  allowed      0  rejected   42480  errors      0
```

Ten seconds with no requests served and no errors: go-redis kept retrying
while every caller waited. go-redis logged the new primary 7 seconds after
the kill: 5 for Sentinel to declare the old one down, 2 to elect a leader and
promote a replica. Requests started flowing 4 seconds after that.

Fail-open does nothing here: it only applies once `Allow` returns an error,
and `Allow` did not return.

### With a timeout, requests fail fast

The same run with a 100ms context timeout per request:

```
  11s  allowed      0  rejected  215002  errors      0
  12s  allowed      0  rejected  121978  errors    150  last error: context deadline exceeded
  13s  allowed      0  rejected       0  errors    400  last error: context deadline exceeded
  ...
  22s  allowed      0  rejected    2986  errors    448  last error: context deadline exceeded
  23s  allowed      0  rejected  213380  errors      0
```

The outage is the same length, but every request gets an answer within
100ms. With the default fail-open mode those 4,500 requests would have been
let through unchecked; with `FailClosed` they would have been rejected.

Give the limiter a deadline. With `Allow`, pass a context with a timeout.
With the middleware, set `DialTimeout`, `ReadTimeout` and `WriteTimeout` on
the go-redis client.

### Sentinel and container hostnames

The first version of the setup told Sentinel to monitor the primary by its
hostname (`sentinel resolve-hostnames yes`). The first failover worked. The
second left the cluster broken: every write failed with
`READONLY You can't write against a read only replica`.

The Sentinel logs show why. While the old primary's container was stopped,
Docker removed its name from DNS, and Sentinel recorded the node with an empty
address:

```
+sdown slave :6379  6379 @ mymaster 192.168.107.4 6379
```

After the container came back and rejoined as a replica, Sentinel still had
that entry. In the second failover it promoted this node, then sent the
reconfiguration meant for the remaining replicas to the empty-address entry,
which was the same node. The new primary became a replica of itself:

```
role:slave
master_host:192.168.107.5   # its own address
master_link_status:down
```

Sentinel kept reporting it as the primary, so clients kept writing to it, and
it kept refusing.

The setup now gives every Sentinel node a fixed IP address and monitors the
primary by IP. With that, two failovers in a row work, and the old primary
rejoins as a replica of the new one each time.

If Sentinel runs where node names can stop resolving (containers,
Kubernetes pods), use stable addresses or make sure the names always resolve.

## Cluster failover

Setup: six nodes, three primaries with one replica each,
`cluster-node-timeout 5000`. Fixed window, 100 requests per key, 30 keys
spread over all three primaries. One primary is killed about 10 seconds in.

```sh
docker compose --profile cluster up -d
docker compose run --rm hammer -addrs node1:6379,node2:6379,node3:6379 \
  -algo fw -limit 100 -window 10m -keys 30 -d 40s -client-timeout 100ms
docker compose kill node2   # pick a primary: redis-cli -h node1 cluster nodes
```

In every run a replica took over the dead primary's slots, and the total
stayed at exactly 3000 allowed: no counter was lost. The client was the
problem.

### The client keeps using the dead node

| Client settings                          | Back to normal after the kill |
| ---------------------------------------- | ----------------------------- |
| go-redis defaults                        | not within 25 s               |
| 100ms `DialTimeout`/`ReadTimeout`        | not within 25 s               |
| 100ms context timeout per request        | 48 s                          |
| 100ms client timeouts, slot map reloaded every second | 12 s             |

With the defaults, nothing was served for 20 seconds, then requests for the
dead primary's slots failed with `dial tcp ...: i/o timeout` until the run
ended. With timeouts, those requests failed fast, but kept failing.

A Cluster client keeps a map of which node owns which slot. go-redis rebuilds
it when a node answers `MOVED`, and otherwise every
`ClusterStateReloadInterval`, which defaults to 60 seconds. A dead node never
answers `MOVED`, so the client keeps sending that slot's requests to it. The
48-second run is the one where the 60-second reload happened to land inside
the run: the client had loaded the map at startup, 12 seconds before the kill.

Lowering the interval fixes it:

```go
rdb := redis.NewClusterClient(&redis.ClusterOptions{
	Addrs:                      addrs,
	DialTimeout:                100 * time.Millisecond,
	ReadTimeout:                100 * time.Millisecond,
	WriteTimeout:               100 * time.Millisecond,
	ClusterStateReloadInterval: time.Second,
})
```

`redis.UniversalOptions` has no field for it, so build a `ClusterClient`
directly. A reload is one `CLUSTER SLOTS` call, cheap to run every second.

### Requests for other slots slow down too

Only a third of the keys lived on the dead primary, yet throughput fell from
about 140,000 to about 1,000 checks per second. Each request for a dead slot
held a worker for the full timeout, and with random keys every worker soon
hit one. In a server, the equivalent is request goroutines piling up on the
limiter. Short timeouts keep that bounded.

## Eviction

Setup: a single Redis with `maxmemory 3mb`, filled up front with about 2,000
keys of 200 bytes and no TTL, written by `redis-benchmark`. Then a fixed window
of 5 requests per key over 2,000 keys for 10 seconds, so exactly 10,000
requests should be allowed.

```sh
redis-cli config set maxmemory 3mb
redis-cli config set maxmemory-policy volatile-lru
redis-benchmark -t set -n 100000 -r 100000 -d 200 -q
go run ./cmd/hammer -algo fw -limit 5 -window 10m -keys 2000 -d 10s
```

| `maxmemory-policy` | Allowed | Errors | Limiter keys left |
| ------------------ | ------- | ------ | ----------------- |
| `allkeys-lru`      | 10,000  | 0      | 2,000             |
| `noeviction`       | 10,000  | 2,418  | 2,000             |
| `volatile-lru`     | 12,085  | 3,861  | 2,000             |

The errors were all
`OOM command not allowed when used memory > 'maxmemory'`.

`allkeys-lru` evicted the filler, which nobody was reading, and the limiter
was exact.

`noeviction` refused writes once memory was full, so creating a new counter
failed. With fail-open, those 2,418 requests would have gone through unchecked.

`volatile-lru` only evicts keys that have a TTL. The filler had none and every
limiter key had one, so Redis evicted the limiter's own counters. Each evicted
counter started again from zero, which let 2,085 extra requests through. When
it ran out of keys with a TTL, writes failed with OOM like `noeviction`.

`volatile-*` policies are a common choice when Redis holds a cache with TTLs
next to data that must not be evicted. A rate limiter's keys always have a
TTL, so they look like cache to Redis. Give the limiter its own Redis, or use
an `allkeys-*` policy and enough memory.

## Persistence

Setup: a single Redis. A fixed window of 100 requests per key over 10 keys
uses up every key, then Redis is killed with `SIGKILL` and started again, and
the same keys are hit again.

```sh
REDIS_ARGS="--appendonly yes" docker compose up -d --force-recreate redis
go run ./cmd/hammer -algo fw -limit 100 -window 10m -keys 10 -d 3s -run p1
docker compose kill redis && docker compose start redis
go run ./cmd/hammer -algo fw -limit 100 -window 10m -keys 10 -d 3s -run p1
```

| Configuration                    | Keys after restart | Allowed after restart |
| -------------------------------- | ------------------ | --------------------- |
| default (`save 3600 1 300 100 60 10000`, no AOF) | 0  | 1,000 |
| `appendonly yes` (`appendfsync everysec`)        | 10 | 0     |

With the default configuration, Redis writes an RDB snapshot at most once a
minute, and on a clean shutdown, which `SIGKILL` skips. The counters were
seconds old when Redis died, so none had been saved and every client got a
fresh window.

With the append-only file, every write is logged and flushed to disk once a
second. The counters survived and the clients stayed limited.

Losing counters on a crash lets each client through one extra window. For
most APIs that is acceptable, and without AOF Redis does not have to append
every request to a file. If a reset must not happen, as with login attempts, turn AOF on.
