# Kafka with Go and Confluent Cloud — Hands-on Lab

Thirteen exercises. Each one changes **one** setting, re-runs, and shows you the
difference in real output. Work through them in order — later exercises rely on
topics earlier ones create.

Look settings up in [MANUAL.md](MANUAL.md). The `.properties` files carry the
full reasoning for every knob, including the alternatives you are not using.

**Numbers in this document are real captured output from a three-broker local
cluster.** Yours will differ, and against Confluent Cloud they will differ a
lot — a real network between you and the brokers changes every latency-sensitive
result. Where that matters, the exercise says so. Record your own numbers; the
*direction* of each change is the lesson, not the absolute figure.

---

## Exercise 0 — Setup

**Goal.** A working toolchain and a reachable cluster.

```bash
cd kafka
./setup.sh
```

It checks Go, that **cgo is enabled** (mandatory — `confluent-kafka-go` wraps the
C library `librdkafka`, and there is no pure-Go fallback), that a C compiler
exists, that everything builds and the unit tests pass, and then that your
credentials actually connect.

For Confluent Cloud, export the three variables first:

```bash
export KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"
```

No cluster, or no internet? Start the offline fallback — three brokers shaped
like Confluent Cloud, so every setting behaves the same:

```bash
docker compose -f docker/docker-compose.yml up -d
```

and add `,config/local.properties` to the `-cluster` flag on every command below.

Build the three binaries:

```bash
mkdir -p bin
go build -o bin/topicadmin ./cmd/topicadmin
go build -o bin/producer   ./cmd/producer
go build -o bin/consumer   ./cmd/consumer
```

> **Shorthand.** Every command below is written for Confluent Cloud. On the local
> cluster add `-cluster config/cluster.properties,config/local.properties`.

### Sharing a cluster with the rest of the room? Read this first

If everyone in the class points at **one** Confluent Cloud cluster, the defaults
collide — every student would use the topic `orders` and the group
`orders-consumer`. Three things go wrong, and only the first is obvious:

| Shared | What happens |
|---|---|
| Topic name | The second person to run `-cmd create` gets `SKIP already exists` and then reads everyone else's messages mixed with their own. |
| `group.id` | You become **one group**, so partitions are split between you and you each see only a third of the data. Your offsets move when someone else consumes. |
| `transactional.id` | Worse: `InitTransactions` **fences** the previous holder. Two students on `orders-producer-1` will knock each other offline in a loop, and neither makes progress. |

The fix is one variable. Pick something unique — your name, or your seat number:

```bash
export ME=alice
```

Then add `-$ME` to **every topic name, every `group.id`, and every
`transactional.id`** in the exercises below. So Exercise 1 becomes:

```bash
./bin/topicadmin -cmd create -topic orders-$ME
```

Exercise 6 becomes:

```bash
./bin/producer -topic orders-$ME -set app.mode=async -set app.message.count=2000
```

Exercise 9 becomes:

```bash
./bin/consumer -topic orders-$ME -set group.id=lab-manual-$ME -set app.mode=manual
```

and Exercise 11, which uses the most names of any exercise:

```bash
./bin/producer -topic orders-txn-$ME -set app.mode=transactional   -set transactional.id=lab-txn-$ME -set app.message.count=1000   -set app.transaction.size=100 -set app.transaction.abort.every=2
```

Delete your own topics at the end (see [Wrapping up](#wrapping-up)) so the next
class starts clean.

> **Not sharing?** If you are on the local Docker cluster, or you have a
> Confluent Cloud cluster to yourself, ignore all of this and use the commands
> exactly as written. Nothing else in the lab depends on `$ME`.

---

## Exercise 1 — Create a topic, and find out what your cluster will not let you change

**Goal.** Create the topic deliberately, then discover the managed-cluster limits
from the cluster itself rather than from a document.

**Run.**

```bash
./bin/topicadmin -cmd create
```

**Observe.** Six partitions, replication factor 3, and nine config entries
applied. Then the layout:

```
=== topic "orders" ===
  partitions: 6

  PART   LEADER   REPLICAS           IN-SYNC (ISR)
  0      1        [1,2,3]            [1,2,3]
  1      2        [2,3,1]            [2,3,1]
  2      3        [3,1,2]            [3,1,2]
```

Leadership is spread across brokers — that is the load balancing. `REPLICAS` is
where the data lives; `IN-SYNC` is which of those copies are currently caught up.
**When ISR is shorter than REPLICAS, a replica is lagging.** If it drops below
`min.insync.replicas`, every `acks=all` produce to that partition starts failing.

Now ask the cluster what it will let you change:

```bash
./bin/topicadmin -cmd describe -all
```

**Observe.** The `EDITABLE HERE?` column. On Confluent Cloud several entries come
back `NO - fixed by the cluster`. This comes from the broker's own
`DescribeConfigs` response, so it is right for your cluster type — Basic,
Standard, Enterprise or Dedicated — without trusting a table that goes stale.

**Why.** A topic is not a cheap decision to revisit. Partition count only goes
up, retention is your replay window, and `min.insync.replicas` is half of a
durability contract whose other half lives in the producer. Creating topics
deliberately from a reviewed file — rather than letting a producer auto-create
one with defaults nobody chose — is why `allow.auto.create.topics=false` is set
in `config/cluster.properties`.

**Try this next.** Ask the broker to validate a change without applying it:

```bash
./bin/topicadmin -cmd alter -dry-run -set retention.ms=3600000
./bin/topicadmin -cmd describe
```

The retention is unchanged. `-dry-run` sets `validate_only` on the request, which
is how you find out a setting will be rejected *before* half-applying it.

---

## Exercise 2 — Partitions, keys and ordering

**Goal.** See that partitioning *is* the ordering guarantee.

**Run.**

```bash
./bin/producer -set app.message.count=2000
```

**Observe.**

```
  distribution across 5 partition(s):
    partition 0      516  ##########
    partition 1       94  #
    partition 3      412  ########
    partition 4      508  ##########
    partition 5      470  #########
```

Six partitions exist, but only **five** received anything, and the counts are
badly uneven — 94 against 516. That is not a bug. There are 20 distinct customer
keys (`app.key.cardinality`), each hashed to a partition. With 20 keys over 6
partitions, some partitions get several keys and one gets none.

**Now remove the keys:**

```bash
./bin/producer -set app.message.count=2000 -set app.null.keys=true
```

Distribution flattens across all six — and per-key ordering is gone entirely.

**Why.** Kafka orders messages **within a partition**, and the default
partitioner sends equal keys to the same partition. So:

- Key by customer → that customer's events are strictly ordered, and different
  customers spread across the cluster for parallelism.
- No key → even spread, no ordering.
- **One hot key → one hot partition,** and no amount of extra partitions helps.

Even distribution is a *consequence* of having many distinct keys. It is never a
guarantee.

**Try this next.** Increase the partition count and read the warning carefully:

```bash
./bin/topicadmin -cmd partitions -to 12
```

Partition count can only go **up**. Worse, the default partitioner is
`hash(key) % partition_count`, so changing the count sends existing keys
somewhere new — that key now exists in two partitions with no ordering between
them, and the old messages do not move. Safe only when your keys are null or
per-key ordering genuinely does not matter.

---

## Exercise 3 — The durability contract

**Goal.** Prove that `acks=all` on its own guarantees nothing.

**Run.** Try to weaken durability while idempotence is on:

```bash
./bin/producer -set app.message.count=10 -set acks=0
```

**Observe.** It refuses to start:

```
producer: creating producer: `acks` must be set to `all` when `enable.idempotence` is true
```

The client enforces the invariant at construction rather than letting you
discover it in production. To explore the ladder you must turn idempotence off
deliberately:

```bash
./bin/producer -set app.message.count=20000 -set enable.idempotence=false \
  -set max.in.flight.requests.per.connection=1000000 -set acks=0
./bin/producer -set app.message.count=20000 -set enable.idempotence=false \
  -set max.in.flight.requests.per.connection=1000000 -set acks=1
./bin/producer -set app.message.count=20000 -set enable.idempotence=false \
  -set max.in.flight.requests.per.connection=1000000 -set acks=all
```

**Observe.** On a three-broker local cluster:

| `acks` | Throughput | What an acknowledgement means |
|---|---|---|
| `0` | 134404 msg/s | Nothing. The producer never waits and never learns about failure. |
| `1` | 80010 msg/s | The leader has it. If the leader dies before replicating, it is gone — and you were told it succeeded. |
| `all` | 81011 msg/s | The leader plus all in-sync replicas have it. |

`acks=0` is roughly **1.7× faster** than either alternative, and `acks=1` and
`acks=all` are nearly identical — replication between three containers on one
laptop is almost free.

**Now the same three runs against Confluent Cloud (`lkc-xqrgrr1`, us-east-2, 24
brokers):**

| `acks` | Local (3 containers) | Confluent Cloud |
|---|---|---|
| `0` | 134404 msg/s | 4304 msg/s |
| `1` | 80010 msg/s | 3898 msg/s |
| `all` | 81011 msg/s | 3765 msg/s |

Two things here are worth more than the durability lecture.

First, **absolute throughput collapses by roughly 30×.** Nothing about the code
changed; a wide-area network appeared. Any Kafka benchmark run on a laptop tells
you nothing about your production numbers.

Second — and this genuinely surprises people — **`acks=all` costs only about 12%
against `acks=0` on Cloud, versus 1.7× locally.** The intuition that "durability
is slow" is largely wrong here, for two reasons: an *async* producer pipelines,
so it is not sitting idle waiting for acknowledgements; and replication between
Confluent's brokers happens on their fast internal network, not across your WAN
link. What you actually pay for on Cloud is the round trip between *you* and the
cluster, and you pay that at every `acks` setting.

**So the argument for `acks=all` is correctness, and it is close to free.** If
you were planning to trade durability for throughput, measure first — on a
managed cluster there is far less to win than you think.

**Now the contract.** `acks=all` means "wait for all *in-sync* replicas". If only
one replica is in sync, that is one replica:

| Configuration | What actually happens |
|---|---|
| `acks=all`, `min.insync.replicas=1` | Silently behaves like `acks=1`. |
| `acks=all`, `min.insync.replicas=2`, RF=3 | Correct. Survives one broker loss. |
| `acks=all`, `min.insync.replicas=3`, RF=3 | Any single broker down makes the partition **unwritable**. |

**Try this next — a trap worth seeing.** Set an impossible value:

```bash
./bin/topicadmin -cmd alter -set min.insync.replicas=9
./bin/topicadmin -cmd describe
```

The broker **accepts it**. Kafka does not validate `min.insync.replicas` against
replication factor at config time — only at produce time, where every `acks=all`
write then fails with `NOT_ENOUGH_REPLICAS`. A configuration that is accepted is
not a configuration that works. Put it back:

```bash
./bin/topicadmin -cmd alter -set min.insync.replicas=2
```

---

## Exercise 4 — Idempotence

**Goal.** Understand the single highest-value line in `producer.properties`.

**Run.**

```bash
./bin/producer -set app.message.count=5000 -set enable.idempotence=true
```

**Observe.** Look at the configuration block the producer prints at startup.
`enable.idempotence=true` forces `acks=all`, caps
`max.in.flight.requests.per.connection` at 5, and requires `retries>0`. Contradict
any of them and the producer refuses to start, as Exercise 3 showed.

**Why.** Without idempotence:

- A retry after a **timed-out-but-actually-succeeded** write produces a
  **duplicate**.
- A retry that **overtakes** an in-flight batch **reorders** your messages.

With it on, the broker tracks a producer id and a per-partition sequence number,
so a retry of an already-written message is discarded server-side. You keep both
ordering and pipelining.

It costs essentially nothing — and **librdkafka defaults it to `false`**, unlike
the Java client, which has defaulted to `true` since 3.0. Never assume a blog post
about "Kafka producer defaults" describes this client.

**The limit.** Idempotence protects **one producer session writing to one
partition**. It does not make consume-then-produce atomic. That is Exercise 11.

---

## Exercise 5 — Batching and compression

**Goal.** See that waiting can make you faster.

**Run.**

```bash
./bin/producer -set app.message.count=20000 -set linger.ms=0
./bin/producer -set app.message.count=20000 -set linger.ms=10
./bin/producer -set app.message.count=20000 -set linger.ms=50
./bin/producer -set app.message.count=20000 -set compression.type=none
./bin/producer -set app.message.count=20000 -set compression.type=lz4
./bin/producer -set app.message.count=20000 -set compression.type=zstd
```

**Observe.** On a local cluster the differences are small — around 25000–27500
msg/s across all six runs. Batching amortises *per-request network overhead*, and
on a laptop talking to containers there is barely any to amortise.

**Here are the same runs against Confluent Cloud, N=20000:**

| Setting | Cloud throughput | vs baseline |
|---|---|---|
| `linger.ms=0` | 1904 msg/s | — |
| `linger.ms=10` | 2148 msg/s | **+13%** |
| `linger.ms=50` | 2088 msg/s | +10% |
| `linger.ms=100` | 1982 msg/s | +4% |
| `compression.type=none` | 1642 msg/s | — |
| `compression.type=lz4` | 2055 msg/s | **+25%** |
| `compression.type=zstd` | 2135 msg/s | **+30%** |
| `compression.type=gzip` | 2866 msg/s | **+75%** |

**Two findings, and one of them overturns the usual advice.**

`linger.ms` does help on Cloud where it barely moved locally — but only by about
13%, and pushing it to 50 or 100 makes things *worse*, not better. More waiting is
not more throughput; past the point where batches are already full, you are just
adding latency.

**Compression is the big lever on Cloud, and `gzip` won by a distance** — +75%,
comfortably ahead of `lz4` and `zstd`. That is the opposite of the usual "lz4 is
the sensible default" advice, and the reason is that the constraint changed. On a
laptop, CPU is the bottleneck and lz4's speed wins. Over a WAN link, **bandwidth**
is the bottleneck, so the codec with the best *ratio* wins even though it burns
more CPU.

The transferable lesson is not "use gzip". It is that **the right compression
codec depends on which resource you are short of**, and you cannot know that
without measuring on the network you will actually run on.

**Why.** `linger.ms=0` does not mean "fast". It means every message may travel as
its own request, so you pay per-request overhead on all of them and compression
has a single message to work with — which is no compression at all. A few
milliseconds of waiting typically multiplies throughput and *reduces* p99 latency
under load, because the broker is no longer drowning in tiny requests.

| `linger.ms` | Use for |
|---|---|
| 0–5 | Latency-critical request/response paths |
| 5–20 | General purpose. Start here. |
| 50–100 | Bulk ingest, log shipping, backfills |

Compression works **per batch**, so it cooperates with `linger.ms` rather than
competing: bigger batches compress far better. JSON like this payload typically
compresses 5–10×, which saves network, disk **and** replication traffic, because
`compression.type=producer` on the topic means the broker stores the batch as-is
without recompressing.

**Try this next.** `batch.num.messages` defaults to 10000 and with small messages
is reached long before `batch.size`. If tuning `batch.size` seems to do nothing,
this is usually why:

```bash
./bin/producer -set app.message.count=20000 -set batch.num.messages=100
```

---

## Exercise 6 — Delivery reports

**Goal.** Internalise that `Produce()` returning `nil` means *queued*, never
*delivered*.

**Run.**

```bash
./bin/producer -set app.mode=async -set app.message.count=2000
./bin/producer -set app.mode=sync  -set app.message.count=300
```

**Observe.**

```
  mode                : async          mode                : sync
  produced (queued)   : 2000           produced (queued)   : 300
  delivered (confirmed): 2000          delivered (confirmed): 300
  throughput          : 1023 msg/s     throughput          : 64 msg/s
                                       latency p50/p99     : 13.275ms / 21.733ms
```

Sync is roughly **16× slower here** — it pays a full round trip per message, with
no batching and no pipelining. The p50 of 13.3 ms is `linger.ms=10` plus the round
trip, visible in the number.

**On Confluent Cloud the same comparison is brutal: 282 msg/s async against
3 msg/s sync — a 94× gap.** Every message now waits for a wide-area round trip
that a laptop cluster did not charge you for. If you take one number from this
lab into a design review, make it this one: synchronous per-message production
against a remote cluster is not a slower option, it is a different order of
magnitude.

**Now the wrong way:**

```bash
./bin/producer -set app.mode=fireforget -set app.message.count=500
```

**Observe.**

```
  produced (queued)   : 500
  delivered (confirmed): 0

  *** 500 message(s) were queued but never reached a verdict. ***
```

Note also that it **hangs for the full 30-second flush timeout**. Nothing is
draining the events channel, so delivery reports pile up in it and `Flush` can
never see the queue empty. A producer that forgets to drain `Events()` behaves
exactly like this in production, and it is invariably misdiagnosed as a slow
broker.

**Why.** The delivery report is the **only** place a producer learns the truth. A
program that ignores them cannot distinguish success from data loss. Use `async`
with a goroutine draining `Events()`, and always `Flush()` before exit.

**Try this next.** Watch backpressure work. Shrink the queue so it fills:

```bash
./bin/producer -set app.message.count=50000 -set queue.buffering.max.messages=1000
```

Look for the `queue-full waits` line. `ErrQueueFull` is **not** a failure — it is
the producer saying it cannot accept work as fast as you are offering it. The
correct response is to wait and retry, which is what `cmd/producer` does. Treating
it as fatal turns a survivable 30-second leader election into an outage.

---

## Exercise 7 — Consumer groups and parallelism

**Goal.** See partitions distributed across group members, and find the ceiling.

**Run.** In terminal 1:

```bash
./bin/consumer -set group.id=lab-group -set app.print.messages=false
```

**Observe** the assignment:

```
  >> REBALANCE (COOPERATIVE): assigned 6 partition(s): [0,1,2,3,4,5]
```

Now start a second consumer in terminal 2 with the **same** group:

```bash
./bin/consumer -set group.id=lab-group
```

**Observe.** Both terminals log a rebalance and each ends up with roughly half the
partitions. Add a third and a fourth. Then add a **seventh** — with six
partitions, it gets nothing and sits idle.

**Why.** A partition is owned by exactly one consumer in a group at a time, so
**the partition count is the hard ceiling on group parallelism.** This is the real
reason partition count matters, and why it is worth over-provisioning slightly at
creation: you cannot decrease it later, and increasing it breaks per-key ordering.

**Try this next.** Open a third terminal with a *different* `group.id`:

```bash
./bin/consumer -set group.id=lab-audit
```

It receives **every** message, independently of `lab-group`, with its own
offsets. Two applications that must each see everything need two group ids — not
two members of one group.

---

## Exercise 8 — Rebalancing

**Goal.** Feel the difference between eager and cooperative rebalancing.

**Run.** Start two consumers using the **eager** default strategy:

```bash
./bin/consumer -set group.id=lab-eager -set partition.assignment.strategy=range
```

Start a second in another terminal, then stop it with Ctrl+C.

**Observe.** Every consumer revokes **every** partition and the whole group stops
until a new assignment is agreed. That is a stop-the-world pause on every scale
event, deploy and restart.

Now the cooperative version:

```bash
./bin/consumer -set group.id=lab-coop -set partition.assignment.strategy=cooperative-sticky
```

**Observe.** `>> REBALANCE (COOPERATIVE)`. Only the partitions that must move are
revoked; consumers that keep theirs never stop consuming.

**Why.** `range` also **skews**: 12 partitions across 5 consumers gives 3,3,2,2,2,
and across several topics the same members are overloaded every time.
`cooperative-sticky` fixes both problems and is what you should use.

> **Migration warning.** You cannot mix eager and cooperative members in one
> group. Moving an existing group across needs a two-step rolling upgrade, or a
> full stop and restart.

**Try this next — make the group evict you.** `max.poll.interval.ms` watches
*your loop*, not your process:

```bash
./bin/consumer -set group.id=lab-slow -set app.process.time=12s \
  -set max.poll.interval.ms=10000 -set session.timeout.ms=6000 -set heartbeat.interval.ms=2000
```

**Two details make this work, and both are worth understanding.**

First, the client enforces `max.poll.interval.ms >= session.timeout.ms`. Lower
only the poll interval and it refuses to start:

```
consumer: creating consumer: `max.poll.interval.ms`must be >= `session.timeout.ms`
```

So `session.timeout.ms` comes down to 6000 too — the lowest most brokers accept,
since `group.min.session.timeout.ms` defaults to 6000.

Second, `app.process.time` must exceed `max.poll.interval.ms` for a **single**
message. This consumer polls once per message, so 2 s of processing means a poll
every 2 s and the 10 s limit is never reached. 12 s of processing on one message
does breach it.

**Observe.**

```
  >> REBALANCE (COOPERATIVE): assigned 6 partition(s): [0,1,2,3,4,5]
  WARN client error err="Application maximum poll interval (10000ms) exceeded by 434ms"
  >> REBALANCE (COOPERATIVE): revoked 6 partition(s): [0,1,2,3,4,5]
     assignment was LOST (we were evicted); offsets cannot be committed
  >> REBALANCE (COOPERATIVE): assigned 6 partition(s): [0,1,2,3,4,5]
  WARN client error err="Application maximum poll interval (10000ms) exceeded by 66ms"
     assignment was LOST (we were evicted); offsets cannot be committed
```

It evicts, rejoins, processes one slow message, and is evicted again — **a
rebalance storm**, live. Note `assignment was LOST`: the partitions were taken
away rather than handed back, so the offsets for the work just done **cannot be
committed** and another consumer will redo it.

This is the classic "group rebalances forever" incident. The process is alive and
heartbeating perfectly the whole time, so `session.timeout.ms` never fires. Only
`max.poll.interval.ms` catches it. Press Ctrl+C to stop.

---

## Exercise 9 — Commit strategies

**Goal.** See exactly where at-least-once comes from.

**Run.** The correct pattern, with 10% of messages failing:

```bash
./bin/consumer -set group.id=lab-manual -set app.mode=manual -set app.fail.rate=0.10
```

Now the unsafe one:

```bash
./bin/consumer -set group.id=lab-auto -set app.mode=autocommit \
  -set enable.auto.commit=true -set enable.auto.offset.store=true -set app.fail.rate=0.10
```

**Observe.** Try running `manual` mode with the wrong commit setting:

```bash
./bin/consumer -set group.id=lab-x -set app.mode=manual -set enable.auto.offset.store=true
```

It refuses, and tells you exactly which property is wrong and why:

```
consumer: app.mode=manual needs enable.auto.offset.store=false, but it is "true (the default)"
  With it true, the offset is stored the moment a message is handed to your
  code - before processing - which is exactly the data loss manual mode avoids.
  Fix: -set enable.auto.offset.store=false
```

**Why.** Committing an offset is a **bookmark**, not an acknowledgement. It says
"if I restart, resume here."

| Order | Result |
|---|---|
| Commit, then process | **At-most-once.** A crash loses the message; there is nothing to redeliver. |
| Process, then commit | **At-least-once.** A crash reprocesses it. |

There is no third option without transactions — which is why the whole industry
defaults to at-least-once and makes processing **idempotent**.

The recommended pattern is the one `manual` mode uses, and it gets both
correctness and efficiency:

```
enable.auto.commit=true        # librdkafka batches the network calls
enable.auto.offset.store=false # but YOU decide what is eligible, after processing
```

`enable.auto.offset.store` is librdkafka-specific and is the key to doing this
properly. Leave it `true` — the default — and the offset is stored before you
have processed anything. That is precisely where auto-commit data loss comes from.

**Try this next.** Prove offsets persist. Run the same group twice:

```bash
./bin/consumer -set group.id=lab-persist -set enable.partition.eof=true -set app.exit.on.eof=true
./bin/consumer -set group.id=lab-persist -set enable.partition.eof=true -set app.exit.on.eof=true
```

The second run receives **0** messages. Change `group.id` and it replays
everything — because a new group has no committed offset, which is the *only*
situation where `auto.offset.reset` applies. That is the usual explanation for
"my consumer suddenly reprocessed everything".

---

## Exercise 10 — Poison messages

**Goal.** Stop one bad message from blocking a partition.

**Run.**

```bash
./bin/topicadmin -cmd create -topic orders-dlq
./bin/consumer -set group.id=lab-dlq -set app.fail.rate=0.10 \
  -set enable.partition.eof=true -set app.exit.on.eof=true
./bin/topicadmin -cmd offsets -topic orders-dlq
```

**Observe.** The consumer reports `dead-lettered: N`, and the dead-letter topic
holds exactly that many messages. Crucially, **the consumer did not stall** — it
processed every message on the topic despite a 10% failure rate.

**Why.** A message that can never succeed must be moved aside. Retrying it forever
blocks its partition *and everything queued behind it* — the classic "consumer
stuck at one offset" incident, where one malformed record halts a pipeline.

Look at what is written alongside the payload:

```
dlq-reason, dlq-source-topic, dlq-source-partition, dlq-source-offset, dlq-timestamp
```

Without those headers a dead-letter topic is just a pile of payloads nobody can
act on. With them you can find the original, understand the failure, and replay
after a fix.

---

## Exercise 11 — Exactly-once

**Goal.** Make the exactly-once guarantee visible as a number.

**Run.** Produce 1000 messages in transactions, deliberately aborting every
second one:

```bash
./bin/topicadmin -cmd create -topic orders-txn
./bin/producer -topic orders-txn -set app.mode=transactional \
  -set transactional.id=lab-txn-1 -set app.message.count=1000 \
  -set app.transaction.size=100 -set app.transaction.abort.every=2
```

**Observe.**

```
  in committed txns   : 500
  in ABORTED txns     : 500   (a read_committed consumer will never see these)
```

Now read the same topic twice, changing only the isolation level:

```bash
./bin/consumer -topic orders-txn -set group.id=lab-rc \
  -set isolation.level=read_committed   -set enable.partition.eof=true -set app.exit.on.eof=true
./bin/consumer -topic orders-txn -set group.id=lab-ru \
  -set isolation.level=read_uncommitted -set enable.partition.eof=true -set app.exit.on.eof=true
```

**Observe.**

| `isolation.level` | Messages visible |
|---|---|
| `read_committed` | **500** |
| `read_uncommitted` | **1000** |

That difference **is** the exactly-once guarantee. The aborted messages were
physically written to the log and then marked aborted; `read_committed` skips
them.

> **A subtlety worth knowing.** `AbortTransaction` also purges the producer's
> *local* queue. Anything still buffered locally is dropped and never reaches the
> broker at all — so it is not an "aborted record", it simply never existed.
> `cmd/producer` calls `Flush` before a deliberate abort precisely so the messages
> do reach the broker first and this demonstration shows real aborted records
> rather than a local no-op.

**Now end-to-end.** Consume-transform-produce inside a transaction:

```bash
./bin/topicadmin -cmd create -topic orders-processed
./bin/consumer -set group.id=lab-eos -set app.mode=eos -set enable.auto.commit=false \
  -set app.output.topic=orders-processed -set app.output.transactional.id=lab-eos-1 \
  -set enable.partition.eof=true -set app.exit.on.eof=true
```

**Why.** `SendOffsetsToTransaction` puts the **input offset commit** and the
**output message** into one atomic transaction. Either the output exists and the
input is marked done, or neither happened. Without it, a crash between "produced
the output" and "committed the input offset" reprocesses the input and emits the
output twice.

**Try this next — a genuinely confusing observation.** Compare the offsets with
the message count:

```bash
./bin/topicadmin -cmd offsets -topic orders-processed
./bin/consumer -topic orders-processed -set group.id=lab-count \
  -set enable.partition.eof=true -set app.exit.on.eof=true
```

The offset span is **larger** than the number of messages a consumer receives.
The difference is **transaction control records** — commit and abort markers
occupy offsets but are never delivered to your application. This is not data
loss, and it is why "high watermark minus low watermark" is not a message count
on a transactional topic.

**The cost.** Transactions are materially slower: every commit is a round trip to
the transaction coordinator, and `read_committed` consumers cannot advance past
an *open* transaction. A long-running producer transaction directly becomes
consumer latency. Keep transactions short, and use exactly-once where correctness
is worth the latency — not everywhere.

---

## Exercise 12 — Broker failure

*Local cluster only. You cannot stop a Confluent Cloud broker — which is rather
the point of a managed service.*

**Goal.** Watch replication absorb a failure, then watch the durability contract
refuse to lose data — and learn why the error you see is not the error you expect.

### Part 1 — losing one broker of three

```bash
docker stop kafka2
./bin/topicadmin -cluster config/cluster.properties,config/local.properties -cmd describe
```

**Observe.** The ISR has shrunk and leadership has moved off broker 2:

```
  PART   LEADER   REPLICAS           IN-SYNC (ISR)
  0      1        [1,2,3]            [1,3]
  1      3        [2,3,1]            [3,1]
  2      3        [3,1,2]            [3,1]

  WARNING: 6 partition(s) are under-replicated - a replica has fallen out of the ISR
```

Now produce:

```bash
./bin/producer -cluster config/cluster.properties,config/local.properties -set app.message.count=200
```

**Observe.** `delivered: 200`, `failed: 0`. **Nothing broke.** RF=3 with
`min.insync.replicas=2` means two surviving replicas still satisfy the contract.
This is exactly what you paid for.

### Part 2 — breaking the contract

Raise the requirement above what the surviving brokers can meet:

```bash
./bin/topicadmin -cluster config/cluster.properties,config/local.properties \
  -cmd alter -set min.insync.replicas=3
./bin/producer -cluster config/cluster.properties,config/local.properties \
  -set app.message.count=50 -set delivery.timeout.ms=15000
```

**Observe.** Every message fails — but look closely at *how*:

```
  failures by kind:
    Local: Message timed out                             50
```

**Not** `NOT_ENOUGH_REPLICAS`. This surprises almost everyone, and it is the most
useful thing in this exercise.

**Why.** `NOT_ENOUGH_REPLICAS` is a **retriable** error. The broker returns it,
librdkafka retries, the broker returns it again, and this continues until
`delivery.timeout.ms` expires. The delivery report then tells you the **final
outcome** — the message timed out — not the underlying cause that was retried a
hundred times along the way.

This is the general rule, and it is why "message timed out" is such a
frustratingly uninformative production error: **a delivery report reports the
verdict, not the reason.**

### Part 3 — finding the actual cause

Turn on the client's internal logging:

```bash
./bin/producer -cluster config/cluster.properties,config/local.properties \
  -set app.message.count=20 -set delivery.timeout.ms=12000 \
  -set app.verbose=true -set debug=broker,msg
```

**Observe.** The real cause, repeated once per retry:

```
Not enough in-sync replicas
```

In one captured run that line appeared **150 times** for 20 messages — the retry
loop made visible. `debug=broker,msg` is the first thing to reach for when a
producer "just times out"; other useful values are `topic`, `metadata`, `cgrp`
(consumer groups), `protocol` and `all`.

### Restore

```bash
docker start kafka2
./bin/topicadmin -cluster config/cluster.properties,config/local.properties \
  -cmd alter -set min.insync.replicas=2
```

Give the ISR twenty seconds or so to refill, then `-cmd describe` to confirm the
warning is gone.

**Why it matters.** Kafka chose to make the partition **unwritable** rather than
accept a write it could not replicate. With `min.insync.replicas=1` those writes
would have been accepted and then lost, with no error anywhere. Unavailability
you can see beats data loss you cannot.

> **A note on this topology.** Each container here runs as both broker *and* KRaft
> controller. Stopping **two** of the three therefore also destroys the controller
> quorum, and you get local timeouts for that reason instead — a different failure
> wearing the same error message. That is why Part 2 raises
> `min.insync.replicas` rather than stopping a second broker: it isolates the
> durability contract from the control plane. Production clusters usually run
> controllers separately for exactly this reason.

## Exercise 13 — Retention and segments

**Goal.** Understand why "retention is broken" tickets are almost always about
segments.

**Run.**

```bash
./bin/topicadmin -cmd create -topic orders-short \
  -set retention.ms=60000 -set segment.ms=60000
./bin/producer -topic orders-short -set app.message.count=1000
./bin/topicadmin -cmd offsets -topic orders-short
```

Wait a few minutes, then check again:

```bash
./bin/topicadmin -cmd offsets -topic orders-short
```

**Observe.** `EARLIEST` climbs above 0 as old data is deleted, while `LATEST`
stays put. The retained count falls.

**Why.** Two things people get wrong:

1. **Retention only ever deletes *closed* segments.** With the default
   `segment.ms` of 7 days and low traffic, a 1-hour `retention.ms` appears to do
   nothing at all — nothing has rolled yet. Set `segment.ms` well below
   `retention.ms` or expiry looks broken.

2. **`EARLIEST` is not always 0.** Retention deletes from the head, so a topic
   with expired data starts partway in. `LATEST` is the offset the *next* message
   will get, not the last one written.

`retention.bytes` is a **per-partition** cap, and whichever of time or size trips
first wins — so setting bytes can silently shorten your replay window well below
the `retention.ms` you thought you had.

---

## Wrapping up

Run the full automated check:

```bash
./verify-all.sh
```

44 assertions covering all three phases. Clean up your topics when finished:

```bash
./bin/topicadmin -cmd delete -topic orders
./bin/topicadmin -cmd delete -topic orders-dlq
./bin/topicadmin -cmd delete -topic orders-txn
./bin/topicadmin -cmd delete -topic orders-processed
./bin/topicadmin -cmd delete -topic orders-short
```

On a shared cluster, delete the namespaced ones instead — and please do, so the
next class starts clean:

```bash
for t in orders orders-dlq orders-txn orders-processed orders-short; do
  ./bin/topicadmin -cmd delete -topic "$t-$ME"
done
```

And on the local cluster:

```bash
docker compose -f docker/docker-compose.yml down -v
```

### The ten things worth remembering

1. `Produce()` returning `nil` means **queued**, not delivered. The delivery
   report is the only truth.
2. `acks=all` guarantees nothing without `min.insync.replicas=2` on the topic.
3. `enable.idempotence=true` is nearly free and defaults to **false** in
   librdkafka.
4. `linger.ms=0` is not "fast" — batching usually *raises* throughput and *lowers*
   p99.
5. Committing an offset is a bookmark, not an acknowledgement. Its position
   relative to processing decides your delivery guarantee.
6. `enable.auto.offset.store=false` is what makes auto-commit safe.
7. Partition count is the ceiling on consumer-group parallelism, only goes up,
   and increasing it breaks per-key ordering.
8. `session.timeout.ms` catches a dead process; `max.poll.interval.ms` catches a
   stuck loop. Different failures.
9. `cooperative-sticky` removes the stop-the-world rebalance pause.
10. A poison message must go to a dead-letter topic, or it blocks its partition
    and everything behind it.
