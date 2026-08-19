// Package kclient turns a loaded .properties Config into the ConfigMap that
// confluent-kafka-go expects, and holds the small amount of plumbing that all
// three programs share.
//
// The conversion is deliberately dumb: every non-"app." key is copied across
// under its exact name. There is no allow-list and no renaming. If you put a
// property in the file, the client gets it; if librdkafka does not recognise it,
// librdkafka says so by name and you have learned something.
package kclient

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"example.com/kafkatraining/internal/config"
)

// ConfigMap copies every Kafka-scoped property into a kafka.ConfigMap.
//
// Two rules, both of which exist because of how the client validates input:
//
//  1. An EMPTY value means "leave this at the library default" and is skipped.
//     That is what lets config/local.properties blank out the Confluent Cloud
//     credentials for a PLAINTEXT broker; sending sasl.mechanisms="" instead
//     would be rejected as an invalid mechanism.
//
//  2. The go.* properties are handled by the Go binding rather than by
//     librdkafka, and it type-checks them: go.logs.channel.enable must be a Go
//     bool, not the string "true". Every librdkafka property is happy as a
//     string, so only go.* needs converting.
func ConfigMap(c *config.Config) kafka.ConfigMap {
	m := kafka.ConfigMap{}
	for k, v := range c.Kafka() {
		if v == "" {
			continue
		}
		if strings.HasPrefix(k, "go.") {
			m[k] = coerce(v)
			continue
		}
		m[k] = v
	}
	return m
}

// coerce turns a properties-file string into the concrete Go type the binding
// wants: bool, then int, then plain string.
func coerce(v string) kafka.ConfigValue {
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return v
}

// Logger builds the structured logger the programs use. Text by default because
// a classroom reads it off a projector; set app.log.format=json to see what a
// production collector would ingest.
func Logger(c *config.Config) *slog.Logger {
	level := slog.LevelInfo
	if v, _ := c.AppBool("verbose", false); v {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(c.App("log.format", "text"), "json") {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}

// LibraryVersion reports the librdkafka build actually linked into this binary.
// Worth printing: the Go module version and the C library version are two
// different things, and config properties are the C library's vocabulary.
func LibraryVersion() string {
	_, ver := kafka.LibraryVersion()
	return ver
}

// SignalContext returns a context cancelled on the first SIGINT or SIGTERM. A
// second signal aborts immediately, so a wedged client can still be killed with
// a second Ctrl+C rather than kill -9.
func SignalContext(log *slog.Logger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Warn("shutdown signal received; finishing cleanly (press Ctrl+C again to abort)")
		cancel()
		<-ch
		log.Error("second signal; aborting without a clean shutdown")
		os.Exit(130)
	}()
	return ctx, cancel
}

// DrainLogs forwards the client's internal log channel to slog. Enabled by
// setting go.logs.channel.enable=true, which is how you see librdkafka's own
// view of brokers, rebalances and retries.
func DrainLogs(logs chan kafka.LogEvent, log *slog.Logger) {
	for e := range logs {
		log.Debug("librdkafka", "name", e.Name, "tag", e.Tag, "message", e.Message)
	}
}

// RequireCloudCredentials gives a precise error when a Confluent Cloud config is
// missing its API key, instead of letting it fail later as an opaque
// authentication timeout.
func RequireCloudCredentials(c *config.Config) error {
	proto := strings.ToUpper(c.Get("security.protocol"))
	if !strings.HasPrefix(proto, "SASL") {
		return nil // a local PLAINTEXT broker needs no credentials
	}
	var missing []string
	for _, k := range []string{"sasl.username", "sasl.password"} {
		if strings.TrimSpace(c.Get(k)) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"security.protocol=%s but %s is empty:\n"+
				"  export KAFKA_API_KEY=<your Confluent Cloud API key>\n"+
				"  export KAFKA_API_SECRET=<your Confluent Cloud API secret>",
			proto, strings.Join(missing, " and "))
	}
	if strings.TrimSpace(c.Get("bootstrap.servers")) == "" {
		return fmt.Errorf("bootstrap.servers is empty; set it to your cluster endpoint")
	}
	return nil
}
