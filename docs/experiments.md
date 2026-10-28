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
For a rate limiter that means a few increments can disappear and the same
number of extra requests get through. It did not show up here.

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
