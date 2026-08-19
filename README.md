# Kafka with Go and Confluent Cloud

Three runnable Go programs and four heavily annotated configuration files that
teach Kafka's producer, consumer and topic settings by making each one
observable. Built for a Go microservices course; usable on its own.

The **configuration files are the teaching material.** The programs exist so that
changing one property and re-running shows you what that property does.

## Start here

| Document | For |
|---|---|
| **[LAB.md](LAB.md)** | 13 hands-on exercises. Start here. |
| **[MANUAL.md](MANUAL.md)** | Reference: commands, every property, troubleshooting. |

```bash
cd kafka
./setup.sh          # checks toolchain, builds, tests, verifies connectivity
```

## The four phases

| Phase | Deliverable | Where |
|---|---|---|
| 1 | Create and configure topics and partitions | `cmd/topicadmin/`, `config/topic.properties` |
| 2 | Producer, every option annotated | `cmd/producer/`, `config/producer.properties` |
| 3 | Consumer, every option annotated | `cmd/consumer/`, `config/consumer.properties` |
| 4 | Step-by-step lab and manual | `LAB.md`, `MANUAL.md` |

## Requirements

- **Go 1.25+ with cgo enabled.** `confluent-kafka-go` wraps the C library
  `librdkafka`; `CGO_ENABLED=0` does not compile. `librdkafka` itself ships
  prebuilt inside the module for macOS and Linux on amd64 and arm64 — nothing to
  install. Windows: use WSL2.
- **A Confluent Cloud cluster**, or Docker for the offline fallback.

## Running against Confluent Cloud

Credentials are never stored in a config file. The loader expands `${...}` from
the environment and refuses to start if one is unset.

```bash
export KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"

mkdir -p bin
go build -o bin/topicadmin ./cmd/topicadmin
go build -o bin/producer   ./cmd/producer
go build -o bin/consumer   ./cmd/consumer

./bin/topicadmin -cmd create
./bin/producer   -set app.message.count=2000
./bin/consumer   -set group.id=demo
```

## Running offline

A three-broker Docker cluster shaped like Confluent Cloud — RF=3,
`min.insync.replicas=2` satisfiable, transactions working — so the same
`.properties` files behave the same way:

```bash
docker compose -f docker/docker-compose.yml up -d
./bin/topicadmin -cluster config/cluster.properties,config/local.properties -cmd create
```

Add `,config/local.properties` to `-cluster` on every command. Only the
connection differs.

## The one rule for configuration

> Every key that does **not** start with `app.` is a real Kafka or librdkafka
> property name, spelled exactly as Confluent spells it, and is passed to the
> client verbatim. Keys starting with `app.` are read by these programs only.

That is why the files are `.properties` and not YAML: what you learn transfers
unchanged to `kafka-console-producer`, the Java client, Kafka Connect and the
Confluent Cloud UI.

Layer files and override single settings without editing anything:

```bash
./bin/producer -set acks=1 -set linger.ms=0 -set compression.type=none
```

Every program prints its full resolved configuration at startup, with the source
file and line of each value and credentials redacted.

## Verifying

```bash
./verify-all.sh            # 44 checks: static, unit, and live end-to-end
./verify-all.sh --static   # no cluster needed
```

The live run creates only `verify-`prefixed topics and deletes them afterwards,
so it never touches the topics you are working in.

## What it demonstrates

Each of these is a numbered exercise in `LAB.md` with real captured output:

- Partitioning **is** the ordering guarantee, and keyed messages spread unevenly
- `acks=all` guarantees nothing without `min.insync.replicas=2`
- `min.insync.replicas=9` on an RF=3 topic is **accepted**, and fails only at produce time
- `enable.idempotence` defaults to `false` in librdkafka but `true` in Java
- `linger.ms=0` is not "fast"
- `Produce()` returning `nil` means *queued*, not delivered
- Commit position relative to processing is what decides your delivery guarantee
- `session.timeout.ms` catches a dead process; `max.poll.interval.ms` catches a stuck loop
- Cooperative rebalancing removes the stop-the-world pause
- `read_committed` vs `read_uncommitted` across aborted transactions: 500 vs 1000
- Transaction control records make offset spans exceed message counts
- A delivery report gives you the **verdict**, not the retriable cause
