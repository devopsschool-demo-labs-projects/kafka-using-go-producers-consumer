#!/usr/bin/env bash
# Preflight check. Run this first; it verifies the toolchain and, if you have
# Confluent Cloud credentials exported, that they actually work.
set -uo pipefail
cd "$(dirname "$0")"

fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
warn() { printf '  \033[33mNOTE\033[0m  %s\n' "$1"; }

echo
echo "=== toolchain ==="

if command -v go >/dev/null 2>&1; then
  ok "go $(go version | awk '{print $3}')"
else
  bad "go is not installed - https://go.dev/dl/"
fi

# confluent-kafka-go wraps the C library librdkafka, so cgo is mandatory.
# There is no pure-Go fallback: CGO_ENABLED=0 fails to compile.
if [ "$(go env CGO_ENABLED 2>/dev/null)" = "1" ]; then
  ok "cgo is enabled (required - confluent-kafka-go wraps the C library librdkafka)"
else
  bad "CGO_ENABLED is 0. Run: go env -w CGO_ENABLED=1"
fi

if command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1; then
  ok "a C compiler is available"
else
  bad "no C compiler found (macOS: xcode-select --install; Debian/Ubuntu: apt install build-essential)"
fi

echo
echo "=== build ==="
if go build ./... 2>/tmp/kt-build.$$; then
  ok "all packages build"
else
  bad "build failed:"; sed 's/^/        /' /tmp/kt-build.$$
fi
rm -f /tmp/kt-build.$$

if go test ./internal/... >/dev/null 2>&1; then
  ok "unit tests pass"
else
  bad "unit tests failed - run: go test ./internal/..."
fi

# Build the three binaries the lab actually invokes. Doing it here means a
# student who runs setup.sh and then jumps into LAB.md never meets a
# "./bin/topicadmin: no such file or directory".
mkdir -p bin
built=0
for c in topicadmin producer consumer; do
  if go build -o "bin/$c" "./cmd/$c" 2>/dev/null; then built=$((built+1)); else bad "could not build $c"; fi
done
[ "$built" = "3" ] && ok "built bin/topicadmin, bin/producer, bin/consumer"

echo
echo "=== Confluent Cloud credentials ==="
missing=0
for v in KAFKA_BOOTSTRAP_SERVERS KAFKA_API_KEY KAFKA_API_SECRET; do
  if [ -z "${!v:-}" ]; then
    warn "$v is not set"
    missing=1
  else
    ok "$v is set"
  fi
done

if [ "$missing" = "1" ]; then
  cat <<'MSG'

  Set them from your Confluent Cloud cluster, then re-run this script:

      export KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
      export KAFKA_API_KEY="..."
      export KAFKA_API_SECRET="..."

  No cluster or no internet? Use the local fallback instead - identical
  .properties files, only the connection differs:

      docker compose -f docker/docker-compose.yml up -d
      ./bin/topicadmin -cluster config/cluster.properties,config/local.properties -cmd list
MSG
else
  echo
  echo "=== connecting to Confluent Cloud ==="
  if ./bin/topicadmin -cmd list >/tmp/kt-conn.$$ 2>&1; then
    ok "connected; $(grep -c '^  [a-z]' /tmp/kt-conn.$$ 2>/dev/null || echo 0) topic line(s) returned"
  else
    bad "could not reach the cluster:"; tail -5 /tmp/kt-conn.$$ | sed 's/^/        /'
  fi
  rm -f /tmp/kt-conn.$$
fi

echo
if [ "$fail" = "0" ]; then
  echo "  Ready. Next: read LAB.md and start at Exercise 1."
else
  echo "  Fix the FAIL lines above before starting the lab."
fi
echo
exit $fail
