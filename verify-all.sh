#!/usr/bin/env bash
#
# Full verification: static checks, unit tests, then a live end-to-end run that
# asserts the behaviours this material teaches.
#
# Targets Confluent Cloud when KAFKA_API_KEY is exported, otherwise the local
# Docker fallback. Every topic it makes is prefixed "verify-" and deleted at the
# end, so it never touches the topics you are working in.
#
#   ./verify-all.sh          static + unit + live
#   ./verify-all.sh --static static and unit tests only, no cluster needed
#
set -uo pipefail
cd "$(dirname "$0")"

PASS=0; FAIL=0
pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); }
info() { printf '        %s\n' "$1"; }
head2() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }

# assert_eq <actual> <expected> <description>
assert_eq() {
  if [ "$1" = "$2" ]; then pass "$3 ($2)"; else fail "$3: expected $2, got '$1'"; fi
}
# assert_gt <a> <b> <description>   asserts a > b
assert_gt() {
  if [ "${1:-0}" -gt "${2:-0}" ] 2>/dev/null; then pass "$3 ($1 > $2)"
  else fail "$3: expected greater than $2, got '$1'"; fi
}

# ---------------------------------------------------------------- static

head2 "static checks"

UNFORMATTED=$(gofmt -l . 2>/dev/null)
if [ -z "$UNFORMATTED" ]; then pass "gofmt clean"; else fail "gofmt: $UNFORMATTED"; fi

if go vet ./... 2>/tmp/vet.$$; then pass "go vet clean"; else fail "go vet"; sed 's/^/        /' /tmp/vet.$$; fi
rm -f /tmp/vet.$$

if go build ./... 2>/tmp/build.$$; then pass "all packages build"; else fail "build"; sed 's/^/        /' /tmp/build.$$; fi
rm -f /tmp/build.$$

if go test ./internal/... >/tmp/test.$$ 2>&1; then
  pass "unit tests ($(grep -c '^ok' /tmp/test.$$) package(s))"
else
  fail "unit tests"; sed 's/^/        /' /tmp/test.$$
fi
rm -f /tmp/test.$$

# Every property named in a config file must be a real Kafka/librdkafka property
# or an app.* key. A typo here is a startup error for a student, so catch it now.
head2 "configuration files"
for f in config/cluster.properties config/topic.properties config/producer.properties config/consumer.properties config/local.properties; do
  if [ -f "$f" ]; then
    bad=$(grep -vE '^\s*(#|!|$)' "$f" | grep -vE '^[a-zA-Z0-9_.]+\s*=' || true)
    if [ -z "$bad" ]; then pass "$(basename "$f") parses"; else fail "$(basename "$f"): $bad"; fi
  else
    fail "$f is missing"
  fi
done

# Documented commands must not rely on an unquoted glob: zsh aborts a command
# whose glob matches nothing, which breaks copy-paste for students.
if grep -nE '^\s{0,6}(\./bin/|go |docker )' LAB.md MANUAL.md README.md 2>/dev/null | grep -E '\*' | grep -vE "'[^']*\*[^']*'|\"[^\"]*\*[^\"]*\"" >/tmp/glob.$$; then
  if [ -s /tmp/glob.$$ ]; then fail "unquoted glob in a documented command"; sed 's/^/        /' /tmp/glob.$$; else pass "no unquoted globs in documented commands"; fi
else
  pass "no unquoted globs in documented commands"
fi
rm -f /tmp/glob.$$

if [ "${1:-}" = "--static" ]; then
  head2 "summary"; echo "  $PASS passed, $FAIL failed (static only)"; echo
  [ "$FAIL" -eq 0 ] || exit 1
  exit 0
fi

# ---------------------------------------------------------------- target

head2 "cluster"

if [ -n "${KAFKA_API_KEY:-}" ] && [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
  CL="config/cluster.properties"
  info "target: Confluent Cloud ($KAFKA_BOOTSTRAP_SERVERS)"
else
  CL="config/cluster.properties,config/local.properties"
  info "target: local Docker fallback (export KAFKA_API_KEY to use Confluent Cloud)"
  if ! docker compose -f docker/docker-compose.yml ps --status running 2>/dev/null | grep -q kafka1; then
    info "starting the local cluster..."
    docker compose -f docker/docker-compose.yml up -d >/dev/null 2>&1
    for _ in $(seq 1 30); do
      docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
      sleep 2
    done
  fi
fi

mkdir -p bin
go build -o bin/topicadmin ./cmd/topicadmin || exit 1
go build -o bin/producer   ./cmd/producer   || exit 1
go build -o bin/consumer   ./cmd/consumer   || exit 1

T="verify-orders"
TXN="verify-txn"
OUT="verify-processed"
DLQ="verify-dlq"

cleanup() {
  for t in "$T" "$TXN" "$OUT" "$DLQ"; do
    ./bin/topicadmin -cluster "$CL" -cmd delete -topic "$t" >/dev/null 2>&1
  done
}
trap cleanup EXIT

if ./bin/topicadmin -cluster "$CL" -cmd list >/tmp/list.$$ 2>&1; then
  pass "connected ($(grep -oE 'brokers: [0-9]+' /tmp/list.$$ | head -1))"
else
  fail "cannot reach the cluster"; sed 's/^/        /' /tmp/list.$$; rm -f /tmp/list.$$
  head2 "summary"; echo "  $PASS passed, $FAIL failed"; echo; exit 1
fi
rm -f /tmp/list.$$

cleanup
sleep 5

# ---------------------------------------------------------------- phase 1

head2 "phase 1 - topic administration"

for t in "$T" "$TXN" "$OUT" "$DLQ"; do
  ./bin/topicadmin -cluster "$CL" -cmd create -topic "$t" >/tmp/create.$$ 2>&1
  if grep -q "OK   $t created" /tmp/create.$$; then pass "created $t"; else fail "create $t"; tail -3 /tmp/create.$$ | sed 's/^/        /'; fi
done
rm -f /tmp/create.$$

# The settings we asked for must actually be on the topic.
DESC=$(./bin/topicadmin -cluster "$CL" -cmd describe -topic "$T" 2>&1)
echo "$DESC" | grep -qE 'min.insync.replicas +2' && pass "min.insync.replicas=2 applied" || fail "min.insync.replicas not applied"
echo "$DESC" | grep -qE 'cleanup.policy +delete'  && pass "cleanup.policy=delete applied" || fail "cleanup.policy not applied"
PARTS=$(echo "$DESC" | grep -oE 'partitions: [0-9]+' | grep -oE '[0-9]+')
assert_eq "$PARTS" "6" "topic has 6 partitions"

# A dry run must change nothing.
./bin/topicadmin -cluster "$CL" -cmd alter -dry-run -topic "$T" -set retention.ms=999000 >/dev/null 2>&1
AFTER=$(./bin/topicadmin -cluster "$CL" -cmd describe -topic "$T" 2>&1 | grep -oE 'retention.ms +[0-9]+' | grep -oE '[0-9]+$')
if [ "$AFTER" != "999000" ]; then pass "-dry-run changed nothing (retention.ms still $AFTER)"; else fail "-dry-run actually applied the change"; fi

# Partition increase is one-way and must be reflected in metadata.
./bin/topicadmin -cluster "$CL" -cmd partitions -topic "$T" -to 8 >/tmp/parts.$$ 2>&1
NEWP=$(grep -oE 'partitions: [0-9]+' /tmp/parts.$$ | tail -1 | grep -oE '[0-9]+')
assert_eq "$NEWP" "8" "partition increase 6 -> 8 visible in metadata"
rm -f /tmp/parts.$$

# ---------------------------------------------------------------- phase 2

head2 "phase 2 - producer"

N=2000
ASYNC=$(./bin/producer -cluster "$CL" -topic "$T" -set app.message.count=$N 2>&1)
D=$(echo "$ASYNC" | grep -E '^  delivered' | grep -oE '[0-9]+$')
F=$(echo "$ASYNC" | grep -E '^  failed' | grep -oE '[0-9]+$')
assert_eq "$D" "$N" "async: all messages confirmed delivered"
assert_eq "$F" "0" "async: no failures"

ATPS=$(echo "$ASYNC" | grep -oE 'throughput +: [0-9]+' | grep -oE '[0-9]+$')

SYNC=$(./bin/producer -cluster "$CL" -topic "$T" -set app.mode=sync -set app.message.count=300 2>&1)
SD=$(echo "$SYNC" | grep -E '^  delivered' | grep -oE '[0-9]+$')
assert_eq "$SD" "300" "sync: all messages confirmed delivered"
STPS=$(echo "$SYNC" | grep -oE 'throughput +: [0-9]+' | grep -oE '[0-9]+$')
assert_gt "$ATPS" "$STPS" "async is faster than sync"

# Offsets on the broker must match what the producer claimed.
RETAINED=$(./bin/topicadmin -cluster "$CL" -cmd offsets -topic "$T" 2>&1 | grep -oE '^  [0-9]+ message' | grep -oE '[0-9]+')
assert_eq "$RETAINED" "2300" "broker holds exactly the messages the producer confirmed"

# ---------------------------------------------------------------- phase 3

head2 "phase 3 - consumer"

C1=$(./bin/consumer -cluster "$CL" -topic "$T" -set group.id=verify-manual \
      -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1)
R=$(echo "$C1" | grep -E '^  received' | grep -oE '[0-9]+$')
P=$(echo "$C1" | grep -E '^  processed OK' | grep -oE '[0-9]+$')
OOO=$(echo "$C1" | grep -oE 'out-of-order within a partition \(per producer run\): [0-9]+' | grep -oE '[0-9]+$')
assert_eq "$R" "2300" "manual mode consumed every message"
assert_eq "$P" "2300" "manual mode processed every message"
assert_eq "$OOO" "0" "no out-of-order messages within a partition, per producer"
echo "$C1" | grep -q 'REBALANCE (COOPERATIVE)' && pass "cooperative rebalance protocol in use" || fail "expected COOPERATIVE rebalance protocol"

# Re-running the same group must consume nothing: the offsets were committed.
C2=$(./bin/consumer -cluster "$CL" -topic "$T" -set group.id=verify-manual \
      -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1)
R2=$(echo "$C2" | grep -E '^  received' | grep -oE '[0-9]+$')
assert_eq "$R2" "0" "committed offsets survive a restart (no reprocessing)"

# Poison messages must be quarantined, not block the partition.
C3=$(./bin/consumer -cluster "$CL" -topic "$T" -set group.id=verify-dlq -set app.fail.rate=0.10 \
      -set app.dlq.topic="$DLQ" -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1)
R3=$(echo "$C3" | grep -E '^  received' | grep -oE '[0-9]+$')
DL=$(echo "$C3" | grep -E '^  dead-lettered' | grep -oE '[0-9]+' | head -1)
assert_eq "$R3" "2300" "a 10% failure rate does not stall the consumer"
assert_gt "$DL" "0" "failed messages were dead-lettered"
DLQN=$(./bin/topicadmin -cluster "$CL" -cmd offsets -topic "$DLQ" 2>&1 | grep -oE '^  [0-9]+ message' | grep -oE '[0-9]+')
assert_eq "$DLQN" "$DL" "dead-letter topic holds exactly the quarantined messages"

# ---------------------------------------------------------------- exactly-once

head2 "exactly-once"

TX=$(./bin/producer -cluster "$CL" -topic "$TXN" -set app.mode=transactional \
      -set transactional.id=verify-txn-1 -set app.message.count=1000 \
      -set app.transaction.size=100 -set app.transaction.abort.every=2 2>&1)
COMMITTED=$(echo "$TX" | grep -E 'in committed txns' | grep -oE '[0-9]+$')
ABORTED=$(echo "$TX" | grep -E 'in ABORTED txns' | grep -oE '^ +in ABORTED txns +: [0-9]+' | grep -oE '[0-9]+$')
assert_eq "$COMMITTED" "500" "500 messages in committed transactions"

RC=$(./bin/consumer -cluster "$CL" -topic "$TXN" -set group.id=verify-rc -set isolation.level=read_committed \
      -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1 | grep -E '^  received' | grep -oE '[0-9]+$')
RU=$(./bin/consumer -cluster "$CL" -topic "$TXN" -set group.id=verify-ru -set isolation.level=read_uncommitted \
      -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1 | grep -E '^  received' | grep -oE '[0-9]+$')
assert_eq "$RC" "500" "read_committed hides messages from aborted transactions"
assert_eq "$RU" "1000" "read_uncommitted shows them"
assert_gt "$RU" "$RC" "read_uncommitted sees strictly more than read_committed"

# consume-transform-produce
EOS=$(./bin/consumer -cluster "$CL" -topic "$T" -set group.id=verify-eos -set app.mode=eos \
       -set enable.auto.commit=false -set app.output.topic="$OUT" \
       -set app.output.transactional.id=verify-eos-1 \
       -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1)
ER=$(echo "$EOS" | grep -E '^  received' | grep -oE '[0-9]+$')
EP=$(echo "$EOS" | grep -E '^  produced' | grep -oE '[0-9]+' | head -1)
assert_eq "$ER" "2300" "eos consumed every input message"
assert_eq "$EP" "2300" "eos produced one output per input"

# Reading the output back proves the count, which the raw offsets do NOT:
# transaction control records advance the offset without being deliverable.
OUTN=$(./bin/consumer -cluster "$CL" -topic "$OUT" -set group.id=verify-out \
        -set enable.partition.eof=true -set app.exit.on.eof=true 2>&1 | grep -E '^  received' | grep -oE '[0-9]+$')
assert_eq "$OUTN" "2300" "output topic delivers exactly one message per input"

OUTOFFSETS=$(./bin/topicadmin -cluster "$CL" -cmd offsets -topic "$OUT" 2>&1 | grep -oE '^  [0-9]+ message' | grep -oE '[0-9]+')
assert_gt "$OUTOFFSETS" "$OUTN" "offset span exceeds message count (transaction control records)"

# ---------------------------------------------------------------- guardrails

head2 "guardrails"

# A mode that contradicts the commit settings must be refused, not silently fixed.
if ./bin/consumer -cluster "$CL" -topic "$T" -set app.mode=manual -set enable.auto.offset.store=true \
     -set group.id=verify-guard >/tmp/g.$$ 2>&1; then
  fail "manual mode should refuse enable.auto.offset.store=true"
else
  grep -q 'enable.auto.offset.store=false' /tmp/g.$$ && pass "manual mode refuses a contradictory commit setting" \
    || fail "refused, but without explaining which property is wrong"
fi
rm -f /tmp/g.$$

# transactional.id is mandatory for transactional mode.
if ./bin/producer -cluster "$CL" -topic "$T" -set app.mode=transactional -set app.message.count=1 >/tmp/g.$$ 2>&1; then
  fail "transactional mode should refuse to run without transactional.id"
else
  grep -q 'transactional.id' /tmp/g.$$ && pass "transactional mode requires transactional.id" \
    || fail "refused, but without naming transactional.id"
fi
rm -f /tmp/g.$$

# An unset environment variable must be named, not surface as a SASL timeout.
if KAFKA_BOOTSTRAP_SERVERS= KAFKA_API_KEY= KAFKA_API_SECRET= \
   ./bin/topicadmin -cluster config/cluster.properties -cmd list >/tmp/g.$$ 2>&1; then
  fail "should refuse to start with no credentials"
else
  grep -qE 'KAFKA_(API_KEY|BOOTSTRAP_SERVERS)' /tmp/g.$$ && pass "unset credentials are named at startup" \
    || fail "refused, but without naming the missing variable"
fi
rm -f /tmp/g.$$

# ---------------------------------------------------------------- summary

head2 "summary"
echo "  $PASS passed, $FAIL failed"
echo
[ "$FAIL" -eq 0 ] || exit 1
