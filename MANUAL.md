# Kafka with Go and Confluent Cloud — Manual

Reference for the three programs in this package, the configuration surface they
expose, and the operational reasoning behind each setting. Work through
[LAB.md](LAB.md) for the guided exercises; come here to look things up.

---

## 1. What is in this package

| Path | What it is |
|---|---|
| `cmd/topicadmin/` | **Phase 1.** Create, describe, alter and inspect topics. |
| `cmd/producer/` | **Phase 2.** Producer with four delivery modes. |
| `cmd/consumer/` | **Phase 3.** Consumer with four commit strategies. |
| `config/cluster.properties` | Connection and authentication. Loaded first by all three. |
| `config/topic.properties` | Topic shape and durability. Phase 1 only. |
| `config/producer.properties` | Every producer knob, annotated. |
| `config/consumer.properties` | Every consumer knob, annotated. |
| `config/local.properties` | Overlay pointing at the offline Docker cluster. |
| `internal/config/` | The `.properties` loader. Layering, `${ENV}`, redaction. |
| `internal/kclient/` | Bridge from properties to `kafka.ConfigMap`. |
| `internal/model/` | The `Order` message type. |
| `docker/docker-compose.yml` | Three-broker offline fallback cluster. |
| `setup.sh` | Preflight: toolchain, build, credentials, connectivity. |
| `verify-all.sh` | 44 automated checks, static and live. |

The configuration files are the teaching material. The Go programs exist to make
each setting observable — read the `.properties` files as documents, not as
inputs.

---

## 2. Prerequisites

| Requirement | Why |
|---|---|
| Go 1.25+ | The module targets 1.25. |
| **cgo enabled** | `confluent-kafka-go` wraps the C library `librdkafka`. There is no pure-Go fallback: `CGO_ENABLED=0` fails to compile. |
| A C compiler | macOS: `xcode-select --install`. Debian/Ubuntu: `apt install build-essential`. Windows: use WSL2. |
| A Confluent Cloud cluster | Or Docker, for the offline fallback. |

`librdkafka` itself ships prebuilt inside the Go module for macOS and Linux on
both amd64 and arm64, so there is nothing to `brew install` or `apt install`.

Run `./setup.sh` — it checks all of the above and then tries a real connection.

---

## 3. Connecting to Confluent Cloud

1. In the Confluent Cloud console, create a cluster (Basic is enough).
2. **Cluster settings → Endpoints** gives the bootstrap server.
3. **API keys → Add key → My account**, scoped to this cluster. Copy both halves;
   the secret is shown exactly once.
4. Export them. Credentials are never written into a config file — the loader
   expands `${...}` from the environment and refuses to start if one is unset.

```bash
export KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
export KAFKA_API_KEY="XXXXXXXXXXXXXXXX"
export KAFKA_API_SECRET="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
./setup.sh
```

To keep them out of your shell history, put them in an untracked `.env`
(`.gitignore` already covers it) and `source .env`.

### What Confluent Cloud fixes for you

| Setting | On Confluent Cloud |
|---|---|
| `replication.factor` | Always 3. Not changeable. |
| `min.insync.replicas` | Settable, but only to `1` or `2`. |
| `unclean.leader.election.enable` | Fixed at `false`. |
| Partition count | Increasable on every cluster type. Never decreasable. |
| `security.protocol` | `SASL_SSL` only. |

Do not trust that table over your own cluster — it changes. Ask the cluster:

```bash
./bin/topicadmin -cmd describe -all
```

The `EDITABLE HERE?` column comes from the broker's own `DescribeConfigs`
response, so it is correct for whatever cluster type you are actually on.

> **Two kinds of API key.** Confluent Cloud issues **Cloud/Global** keys (for the
> `api.confluent.cloud` control plane) and **Kafka cluster** keys (for the broker's
> `:9092` SASL endpoint). Only the second works here. Create it from the
> **cluster's** API Keys tab — a key made under "Cloud resource management" will
> connect over TLS, negotiate SASL, and then be rejected. Prefer **My account**
> over a service account, or you must also grant that account a role on the
> cluster.

### Offline fallback

No cluster or no internet? A three-broker Docker cluster shaped like Confluent
Cloud — RF=3, `min.insync.replicas=2` satisfiable, transactions working:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Then add the overlay to every command. Only the connection differs; every topic,
producer and consumer setting behaves identically:

```bash
./bin/topicadmin -cluster config/cluster.properties,config/local.properties -cmd list
```

The one real difference is security: the local listener is `PLAINTEXT` with no
authentication, because running a private CA in a classroom teaches nothing about
Kafka.

---

## 4. How configuration works

### The one rule

> Every key that does **not** start with `app.` is a real Kafka or librdkafka
> property name, spelled exactly as Confluent spells it, and is passed to the
> client verbatim. Keys starting with `app.` are read by these training programs
> and never reach Kafka.

That is why the files are `.properties` rather than YAML: what you learn here
transfers unchanged to `kafka-console-producer`, the Java client, Kafka Connect
and the Confluent Cloud UI, with no mental translation layer.

### Layering

Files are applied in order, then `-set` overrides on top. Later wins.

```bash
./bin/producer \
  -cluster config/cluster.properties,config/local.properties \
  -config  config/producer.properties \
  -set     acks=1 \
  -set     linger.ms=0
```

`-set` is what the lab uses to change one property at a time without editing a
file, so you can always get back to a known state.

### Seeing the effective configuration

Every program prints its full resolved configuration at startup, sorted, with
the source of each value and credentials redacted:

```
=== producer configuration (librdkafka 2.15.0) ===
  acks                                   = all              (config/producer.properties:57)
  compression.type                       = lz4              (config/producer.properties:169)
  enable.idempotence                     = true             (config/producer.properties:76)
  linger.ms                              = 0                (-set)
  sasl.password                          = xxxx********     (config/cluster.properties:83)
```

When something behaves unexpectedly, this block answers "what was it actually
configured with, and which line did that come from" before you start guessing.

### Environment expansion

`${NAME}` is replaced from the environment. An unset variable is **not** an error
at parse time — a later overlay may replace the whole value, which is how
`local.properties` blanks out the Cloud credentials. It is reported once, by
name, after all layers are applied:

```
these environment variables are not set:
  KAFKA_API_KEY (needed by "sasl.username" at config/cluster.properties:82)
```

---

## 5. Command reference

### `topicadmin` — Phase 1

```bash
./bin/topicadmin -cmd <command> [flags]
```

| Command | What it does |
|---|---|
| `create` | Create the topic from `config/topic.properties`, then describe it. Re-runnable: an existing topic is skipped, not an error. |
| `describe` | Partitions, leaders, replicas, ISR, and every config entry with its source and whether this cluster lets you change it. |
| `alter` | Apply changed settings to an existing topic, via `IncrementalAlterConfigs`. |
| `partitions` | Increase the partition count. One way only. |
| `offsets` | Earliest and latest offset per partition, and how many messages are retained. |
| `list` | Topics on the cluster, with partition counts. |
| `delete` | Delete the topic. |

| Flag | Default | Purpose |
|---|---|---|
| `-cluster` | `config/cluster.properties` | Connection files, comma-separated to layer. |
| `-topic-config` | `config/topic.properties` | Topic settings file. |
| `-topic` | `app.topic` | Override the topic name. |
| `-set k=v` | — | Override one setting. Repeatable. |
| `-to N` | — | New partition count, for `-cmd partitions`. |
| `-dry-run` | off | Ask the broker to validate and change nothing. |
| `-all` | off | `describe`: include entries at their cluster default. `list`: include internal topics. |
| `-timeout` | `30s` | Admin request timeout. |

### `producer` — Phase 2

```bash
./bin/producer [-set key=value ...]
```

| `app.mode` | Behaviour |
|---|---|
| `async` | Queue and return; delivery reports handled on another goroutine; `Flush` at exit. **The production pattern.** |
| `sync` | Block on each message's own delivery report. Correct, ~50× slower. |
| `transactional` | Group messages into transactions committed atomically. Requires `transactional.id`. |
| `fireforget` | Deliberately wrong: delivery reports discarded, so success and loss are indistinguishable. |

### `consumer` — Phase 3

```bash
./bin/consumer [-set key=value ...]
```

| `app.mode` | Behaviour | Guarantee |
|---|---|---|
| `manual` | Process, then `StoreOffsets`. **The pattern to copy.** | at-least-once |
| `autocommit` | Timer commits regardless of processing outcome. | at-most-once in disguise |
| `atmostonce` | Commit before processing. Deliberately wrong. | at-most-once |
| `eos` | Consume-transform-produce in one transaction. | exactly-once |

Each mode requires commit settings that agree with it. The program **refuses to
start** on a contradiction rather than silently overriding your file:

```
consumer: app.mode=manual needs enable.auto.offset.store=false, but it is "true (the default)"
  With it true, the offset is stored the moment a message is handed to your
  code - before processing - which is exactly the data loss manual mode avoids.
  Fix: -set enable.auto.offset.store=false
```

---

## 6. Delivery guarantees

Kafka gives you three points on a spectrum. Which one you get is decided almost
entirely by **when you commit the offset relative to doing the work**.

| Guarantee | Producer | Consumer | Cost |
|---|---|---|---|
| **At-most-once** | `acks=0` or `1` | commit *before* processing | Fastest. Messages are lost on any failure. |
| **At-least-once** | `acks=all`, `enable.idempotence=true` | commit *after* processing | One extra round trip. Duplicates on retry — processing must be idempotent. |
| **Exactly-once** | `transactional.id` set, transactions | `isolation.level=read_committed`, offsets sent via `SendOffsetsToTransaction` | Commit round trip per transaction; consumers wait for commits. |

### The two-part durability contract

`acks=all` alone guarantees nothing. It means "wait for all *in-sync* replicas",
and if only one replica is in sync, that is one replica.

```
producer acks=all  +  topic min.insync.replicas=2  +  replication.factor=3
```

All three, or the guarantee is not there:

| Configuration | What actually happens |
|---|---|
| `acks=all`, `min.insync.replicas=1` | Silently behaves like `acks=1`. One broker loss loses acknowledged data. |
| `acks=all`, `min.insync.replicas=2`, RF=3 | Correct. Survives one broker loss with no data loss and no downtime. |
| `acks=all`, `min.insync.replicas=3`, RF=3 | Durable, but any single broker going down makes the partition **unwritable**. You traded availability for nothing. |

### Idempotence is nearly free

`enable.idempotence=true` costs essentially nothing and removes two whole classes
of bug: duplicates from retries, and reordering when a retry overtakes an
in-flight batch. It defaults to **`false`** in librdkafka — unlike the Java
client, where it has defaulted to `true` since 3.0.

It protects one producer session writing to one partition. It does **not** make
consume-then-produce atomic; that needs transactions.

---

## 7. Property quick reference

Full annotations — alternatives, trade-offs, and when to change each — live in
the `.properties` files. This is the lookup table.

### Topic (`config/topic.properties`)

| Property | Default here | Notes |
|---|---|---|
| `app.num.partitions` | 6 | Parallelism ceiling and ordering unit. Increase only. |
| `app.replication.factor` | 3 | Fixed at 3 on Confluent Cloud. |
| `min.insync.replicas` | 2 | Half the durability contract. Cloud allows 1 or 2. |
| `cleanup.policy` | `delete` | `compact` keeps the newest value per key. |
| `retention.ms` | 604800000 (7d) | Your replay window. `-1` for infinite. |
| `retention.bytes` | -1 | Per **partition**, not per topic. |
| `segment.ms` | 3600000 (1h) | Retention only deletes *closed* segments. |
| `segment.bytes` | 104857600 | 1 GiB is the real-world value. |
| `max.message.bytes` | 1048588 | Must be ≥ the producer's `message.max.bytes`. |
| `compression.type` | `producer` | Keep the producer's codec; no broker CPU cost. |
| `message.timestamp.type` | `CreateTime` | Event time, not arrival time. |

### Producer (`config/producer.properties`)

| Property | librdkafka default | Set here | Why |
|---|---|---|---|
| `acks` | `-1` (all) | `all` | Only setting where an ack means something. |
| `enable.idempotence` | **`false`** | `true` | No duplicates, no reordering. Java defaults to `true`. |
| `max.in.flight.requests.per.connection` | 1000000 | 5 | Capped at 5 by idempotence; keeps pipelining. |
| `delivery.timeout.ms` | 300000 | 120000 | The retry budget that actually matters. |
| `retries` | 2147483647 | unchanged | Let `delivery.timeout.ms` be the limit. |
| `request.timeout.ms` | 30000 | 30000 | Must be well under `delivery.timeout.ms`. |
| `linger.ms` | **5** | 10 | Java defaults to 0. Waiting *raises* throughput. |
| `batch.size` | 1000000 | 131072 | Batch sent at this size or at `linger.ms`. |
| `batch.num.messages` | 10000 | 10000 | Often the limit that actually binds. |
| `compression.type` | `none` | `lz4` | Compresses per batch; saves network, disk and replication. |
| `queue.buffering.max.messages` | 100000 | 100000 | Full queue → `ErrQueueFull`, which means *slow down*, not *fail*. |
| `partitioner` | `consistent_random` | unchanged | Use `murmur2_random` to match Java producers. |
| `transactional.id` | (empty) | commented | Enables transactions; fences zombie instances. |
| `transaction.timeout.ms` | 60000 | 120000 | Must be ≥ `delivery.timeout.ms`. |

### Consumer (`config/consumer.properties`)

| Property | librdkafka default | Set here | Why |
|---|---|---|---|
| `group.id` | (none) | `orders-consumer` | Unit of work-sharing *and* of offset storage. |
| `group.instance.id` | (empty) | commented | Static membership: a restart causes no rebalance. |
| `auto.offset.reset` | `largest` | `earliest` | Only applies when there is **no** valid committed offset. |
| `isolation.level` | **`read_committed`** | `read_committed` | Java defaults to `read_uncommitted`. |
| `enable.auto.commit` | `true` | `true` | Safe *only* with the next line. |
| `enable.auto.offset.store` | **`true`** | `false` | The key setting. Offsets become eligible only after you process. |
| `auto.commit.interval.ms` | 5000 | 5000 | Worst-case reprocessing window. |
| `session.timeout.ms` | 45000 | 45000 | Detects a **dead process** via heartbeats. |
| `heartbeat.interval.ms` | 3000 | 3000 | Roughly a third of the session timeout. |
| `max.poll.interval.ms` | 300000 | 300000 | Detects a **stuck loop**. A different failure. |
| `partition.assignment.strategy` | `range,roundrobin` | `cooperative-sticky` | Removes the stop-the-world rebalance pause. |
| `fetch.min.bytes` | 1 | 1 | With `fetch.wait.max.ms`, the consumer's `linger.ms`. |
| `max.partition.fetch.bytes` | 1048576 | 1048576 | Must be ≥ topic `max.message.bytes` or you stall. |
| `enable.partition.eof` | `false` | `false` | On for batch jobs that must know they are done. |

### librdkafka defaults that differ from the Java client

These four catch people out constantly. A blog post about "Kafka producer
defaults" is almost certainly describing the Java client.

| Property | librdkafka | Java |
|---|---|---|
| `enable.idempotence` | `false` | `true` (since 3.0) |
| `linger.ms` | `5` | `0` |
| `isolation.level` | `read_committed` | `read_uncommitted` |
| `auto.offset.reset` | `largest` | `latest` (same meaning) |

---

## 8. Two timeouts that are not the same thing

Routinely confused, and the confusion causes real outages.

| | `session.timeout.ms` | `max.poll.interval.ms` |
|---|---|---|
| Watches | A background heartbeat thread | Your `Poll()` loop |
| Fires when | The process is **gone** — crashed, killed, partitioned | The process is **alive but stuck** — slow query, lock, GC pause |
| Default | 45000 | 300000 |

A consumer that stops calling `Poll` but keeps heartbeating is the classic
"group rebalances forever" incident. Only `max.poll.interval.ms` catches it.

If you cannot bound processing time, move the work off the poll thread and use
`Pause`/`Resume` for backpressure rather than raising the timeout indefinitely.

---

## 9. Rebalancing

| Strategy | Behaviour | Cost |
|---|---|---|
| `range` | Contiguous ranges per topic. **Default.** | Skews: 12 partitions over 5 consumers gives 3,3,2,2,2, and the same members are overloaded on every topic. |
| `roundrobin` | Deal partitions out across all topics. | Even, but every rebalance reassigns everything. |
| `cooperative-sticky` | Keep existing assignments; move only what must move. | **Use this.** Rebalances become incremental — consumers that keep their partitions never stop consuming. |

`range` and `roundrobin` are *eager*: every consumer revokes every partition and
the whole group stops until a new assignment is agreed. That is a stop-the-world
pause on every scale event, deploy and restart.

> **Migration warning.** You cannot mix eager and cooperative members in one
> group. Moving an existing group across needs a documented two-step rolling
> upgrade, or a full stop and restart.

### Taking control of rebalances in Go

`go.application.rebalance.enable=true` forwards rebalance events to the
**`Events()` channel** — the channel-based consumer. A `Poll()`-based consumer,
which is what `cmd/consumer` uses and what you should use, **never sees them that
way**. Pass a callback to `Subscribe` instead:

```go
c.Subscribe(topic, myRebalanceCallback)
```

Pass `nil` and librdkafka assigns partitions silently, costing you the only
moment where you can commit offsets for a partition you are about to lose.

The callback **must** call one of the assign/unassign methods for every event, or
the consumer hangs waiting for acknowledgement. For cooperative rebalancing use
the incremental forms:

```go
if c.GetRebalanceProtocol() == "COOPERATIVE" {
    c.IncrementalAssign(e.Partitions)   // ADD to the current assignment
} else {
    c.Assign(e.Partitions)              // REPLACE the whole assignment
}
```

---

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `CGO_ENABLED=0` build errors: `undefined: kafka.NewProducer` | cgo is off. `go env -w CGO_ENABLED=1` and install a C compiler. |
| `these environment variables are not set: KAFKA_API_KEY ...` | Export the three variables, or add `,config/local.properties` for the offline cluster. |
| `Local: Broker transport failure`, and `debug=broker,security` shows `Authentication failed. If you are using a Global API key...` | You have a **Cloud/Global** API key, not a **Kafka cluster** key. Only a cluster-scoped key authenticates to `:9092`. Create one from the **cluster's own** API Keys tab, not from "Cloud resource management". |
| Connects and lists topics, but `Authorization failed` on create/produce | The key authenticates but its owner has no permissions. A service-account key needs `CloudClusterAdmin` (or matching ACLs) on that cluster; a key created under **My account** inherits your rights and needs none. |
| `Local: Authentication failure` | Wrong API key/secret, or the key is scoped to a different cluster. |
| `broker certificate could not be verified` | A TLS-inspecting corporate proxy. Point `ssl.ca.location` at your CA bundle. Never disable verification. |
| ``message.timeout.ms` must be set <= `transaction.timeout.ms`` | `delivery.timeout.ms` exceeds `transaction.timeout.ms`. Raise the latter. |
| Producer fails with `Local: Message timed out` | The delivery report shows the **final verdict, not the cause**. Retriable broker errors are retried until `delivery.timeout.ms` expires. Re-run with `-set app.verbose=true -set debug=broker,msg` to see the real reason. |
| Producer hangs at `Flush` for the full timeout | Nothing is draining `Events()`. Delivery reports pile up and the queue never empties. Drain it in a goroutine. |
| `ErrQueueFull` | Backpressure, **not** failure. Wait and retry. `cmd/producer` shows the pattern. |
| Consumer reprocesses everything on restart | `group.id` changed, or offsets expired. `auto.offset.reset` then decides. |
| Consumer receives nothing | The group is already at the end. Use a new `group.id`, or `-set auto.offset.reset=earliest` with a fresh group. |
| Consumer stalls on one offset forever | A message larger than `max.partition.fetch.bytes`. Raise it above the topic's `max.message.bytes`. |
| `topic does not exist` right after creating it | Metadata propagation lag. `topicadmin` waits for it; your own code should tolerate a brief `UNKNOWN_TOPIC_OR_PART`. |
| Group "rebalances forever" | Processing exceeds `max.poll.interval.ms`. Move work off the poll thread. |
| Offset span exceeds message count | Transaction control records advance offsets without being deliverable. Not data loss. |

---

## 11. Production checklist

**Topic**
- [ ] Partition count sized from throughput, with headroom. It only goes up.
- [ ] `min.insync.replicas=2` with `replication.factor=3`.
- [ ] `retention.ms` set from how long you would take to notice and fix a bug.
- [ ] `segment.ms` well below `retention.ms`, or nothing ever expires.

**Producer**
- [ ] `acks=all` **and** `min.insync.replicas=2` on the topic. Both, or neither counts.
- [ ] `enable.idempotence=true`.
- [ ] `delivery.timeout.ms` set from a business requirement, not left at the default.
- [ ] Delivery reports actually inspected — `Produce()` returning nil means *queued*.
- [ ] `ErrQueueFull` handled as backpressure, not as a fatal error.
- [ ] `Flush()` before exit, with a timeout ≥ `delivery.timeout.ms`.
- [ ] `linger.ms` ≥ 5 and compression on, unless you have measured otherwise.

**Consumer**
- [ ] `enable.auto.offset.store=false`, offsets stored only after processing.
- [ ] `partition.assignment.strategy=cooperative-sticky`.
- [ ] A rebalance callback that commits before partitions are revoked.
- [ ] `max.poll.interval.ms` above your worst realistic processing time.
- [ ] `group.instance.id` set for a rolling-restart deployment.
- [ ] A dead-letter path — a poison message must never block a partition.
- [ ] Processing is idempotent, because at-least-once means duplicates.
- [ ] `Close()` on shutdown, so the group does not wait `session.timeout.ms`.

**Operations**
- [ ] `statistics.interval.ms` on, feeding your metrics system.
- [ ] `client.id` set to something an on-call engineer can act on at 3am.
- [ ] Consumer lag alerted on.
- [ ] One API key per application, scoped to only the ACLs it needs.
