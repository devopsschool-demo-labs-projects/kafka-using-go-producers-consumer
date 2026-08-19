# kafka — Kafka with Go and Confluent Cloud

Training material teaching Kafka's configuration surface through three runnable
Go programs. The deliverable is **annotated configuration plus working code**,
not slides.

Read `README.md` for the orientation. This file is the rules.

## Hard rules

1. **The configuration files are the teaching material.** `config/*.properties`
   carry the lesson: every property gets `Values:` / `Default:` / `Why:` in a
   comment above it, naming the alternatives and the trade-off. The Go programs
   exist to make each setting observable. When adding a feature, the question is
   "which property does this let a student see?"

2. **Property names are never renamed or wrapped.** Any key not starting with
   `app.` is a real Kafka/librdkafka name passed to the client verbatim. This is
   the whole reason the files are `.properties` and not YAML — what students
   learn must transfer to `kafka-console-producer`, the Java client and the
   Confluent Cloud UI. Do not introduce a friendly-name mapping layer.

3. **`app.` is the only namespace for program settings.** One rule, one prefix.

4. **Defaults quoted in comments must be librdkafka's actual defaults**, verified
   against the `CONFIGURATION.md` for the pinned version — not the Java client's,
   and not from memory. Four differ in ways that matter and are called out
   explicitly: `enable.idempotence`, `linger.ms`, `isolation.level`,
   `auto.offset.reset`.

5. **Confluent Cloud is the target; Docker is the fallback.** `config/local.properties`
   is a thin overlay changing only the connection. Never fork a setting between
   the two paths — if a lesson only works locally (Exercise 12), say so in the
   exercise rather than diverging the configs.

6. **Do not hard-code what a managed cluster allows.** Confluent Cloud's editable
   topic settings change. `topicadmin describe` reads `IsReadOnly` from the
   broker's own `DescribeConfigs` response. Keep it that way.

7. **Deliberately wrong modes stay wrong.** `app.mode=fireforget` (producer) and
   `autocommit` / `atmostonce` (consumer) exist to be compared against the
   correct ones. Do not "fix" them.

8. **Contradictions are refused, not silently corrected.** If `app.mode` and the
   commit properties disagree, the program exits naming the property and the fix.
   Silently overriding a student's config would undermine rule 1.

9. **Every output block in a document is real captured output.** If behaviour
   changes, re-run and re-capture. `verify-all.sh` asserts the key numbers.

10. **Documented commands are copy-pasted into zsh.** zsh aborts a command whose
    glob matches nothing, so quote any pattern meant for another tool.
    `verify-all.sh` fails the build on an unquoted glob in a documented command.

11. **`./verify-all.sh` must exit 0** before a change is done. 44 checks; the live
    portion needs a cluster and takes a few minutes.

## Layout

| Path | What it is |
|---|---|
| `LAB.md` | 13 exercises, each changing one setting and showing the difference. |
| `MANUAL.md` | Reference: setup, commands, property tables, troubleshooting, production checklist. |
| `cmd/topicadmin/` | Phase 1. create/describe/alter/partitions/offsets/list/delete. |
| `cmd/producer/` | Phase 2. async, sync, transactional, fireforget. |
| `cmd/consumer/` | Phase 3. manual, autocommit, atmostonce, eos. |
| `internal/config/` | `.properties` loader: layering, `${ENV}`, `-set`, redaction, source tracking. Unit-tested, no Kafka import. |
| `internal/kclient/` | Bridge to `kafka.ConfigMap`. Owns the empty-value and `go.*` coercion rules. |
| `internal/model/` | The `Order` message. `Run` identifies the producer process. |
| `docker/` | Three-broker KRaft fallback cluster. |
| `setup.sh` | Preflight. |
| `verify-all.sh` | 44 checks. `--static` skips the cluster. |

## Things that were got wrong once and should not be again

- **`go.*` properties are type-checked by the Go binding**, unlike librdkafka's
  own, which accept strings. `kclient.coerce` converts them. `go.logs.channel.enable`
  as the string `"true"` is a startup error.
- **An empty value means "leave at the library default" and is skipped.** Sending
  `sasl.mechanisms=""` is rejected as an invalid mechanism, which is what
  `local.properties` needs in order to blank out the Cloud credentials.
- **`${ENV}` resolution is deferred** until every layer is applied, so an overlay
  can replace a value whose variable is unset. `Resolve()` reports what remains.
- **`kafka.TopicPartition` holds `Topic` as a `*string`.** Using it as a map key
  compares pointer identity, so looking up a library-produced result map with a
  locally constructed key silently misses every time. Index by partition number.
- **Metadata lags topic mutations.** After `CreateTopics` or `CreatePartitions`
  the client's cache is stale; `topicadmin` polls until it catches up. Removing
  that makes `create` report "topic does not exist".
- **`go.application.rebalance.enable` only feeds the `Events()` channel.** A
  `Poll()`-based consumer must pass a `RebalanceCb` to `Subscribe` or it never
  sees rebalances and cannot commit before revocation.
- **`AbortTransaction` purges the local queue.** Messages still buffered never
  reach the broker, so a read_uncommitted consumer sees nothing extra and the
  demo appears to do nothing. `cmd/producer` flushes before a deliberate abort.
- **`delivery.timeout.ms` must be ≤ `transaction.timeout.ms`**, or the producer
  refuses to start with a message about `message.timeout.ms` (librdkafka's older
  name for the same setting).
- **Ordering is per partition *per producer*.** Each producer run numbers from 1,
  so two runs into one partition interleave two sequences. `model.Order.Run`
  exists so the check tests Kafka's guarantee rather than detecting a restart.
- **A delivery report gives the final verdict, not the retriable cause.**
  `NOT_ENOUGH_REPLICAS` is retried until `delivery.timeout.ms` and surfaces as
  `Local: Message timed out`. Use `debug=broker,msg` to see the cause.
- **Null-keyed messages do NOT round-robin.** `sticky.partitioning.linger.ms=10`
  makes the producer hold one partition per window, and partition choice happens
  at `Produce()` (queue time), not at send time - so a burst of 2000 async
  produces all lands in ONE partition on local AND Cloud. LAB.md Exercise 2 said
  "distribution flattens across all six", which was never measured and was wrong.
  It now shows the real behaviour and has students set
  `sticky.partitioning.linger.ms=0`, or slow the producer, to see the spread.
- **`max.poll.interval.ms` must be >= `session.timeout.ms`**, enforced at
  construction. LAB.md Exercise 8 therefore lowers both. And because the consumer
  polls once per message, triggering an eviction needs `app.process.time` to
  exceed `max.poll.interval.ms` for a SINGLE message - 2s of work against a 10s
  limit never fires.
- **In the Docker topology each node is broker *and* controller**, so stopping
  two of three destroys the KRaft quorum — a different failure with the same
  error text. Exercise 12 raises `min.insync.replicas` instead, to isolate the
  durability contract from the control plane.

## Current state

- Three commands, four config files, two documents, two scripts.
- `confluent-kafka-go/v2 v2.15.0`, bundled librdkafka 2.15.0.
- `./verify-all.sh` exits 0: **44 passed, 0 failed on BOTH targets** — the local
  three-broker Docker cluster and a live Confluent Cloud cluster
  (`lkc-xqrgrr1`, us-east-2, 24 brokers), using the same files with only
  `config/local.properties` layered on for the offline run.
- The SASL_SSL path, `min.insync.replicas=2`, partition increase, cooperative
  rebalancing, dead-lettering and full transactional exactly-once all confirmed
  working on Confluent Cloud.

## Confluent Cloud API keys — the trap every student will hit

Confluent Cloud issues two kinds of API key, and only one of them works with a
Kafka client:

| | Global / Cloud key | Kafka cluster key |
|---|---|---|
| `spec.resource.kind` | `Global` | `Cluster` (an `lkc-` id) |
| Authenticates to | `api.confluent.cloud` control plane | the broker `:9092` SASL endpoint |
| Made in the console via | "Cloud resource management" | the **cluster's own** API Keys tab |

A Global key fails at SASL with a message naming the problem:

    Authentication failed. If you are using a Global API key for authentication,
    please check whether this cluster type is supported

A cluster key that authenticates can still be **unauthorized**: a key owned by a
service account with no role bindings connects and reads metadata fine, then
fails topic creation with a bare `Authorization failed`. The service account
needs `CloudClusterAdmin` (or equivalent ACLs) on that cluster. A key created
under **My account** inherits the user's rights and sidesteps this entirely,
which is what a student should use.

Both failures are in MANUAL.md's troubleshooting table. Diagnose them with
`-set app.verbose=true -set debug=broker,security` — the broker's own text is far
more informative than the `Local: Broker transport failure` the delivery path
reports.

Note `topicadmin`'s `-set` applies to the TOPIC config only, since that is what
the tool tunes; to override a client property there, layer another file onto
`-cluster` instead. `producer` and `consumer` merge everything, so their `-set`
overrides any property.
