// Command consumer is PHASE 3: a consumer whose every behaviour comes from
// config/consumer.properties.
//
// The idea to carry away: an offset commit is a BOOKMARK, not an
// acknowledgement. It says "if I restart, resume here". Commit before you have
// processed and a crash loses the message; commit after and a crash reprocesses
// it. Those are the only two choices, which is why at-least-once plus idempotent
// processing is the industry default, and why exactly-once needs the offset
// commit and the output write to be one atomic transaction.
//
// Modes (app.mode):
//
//	manual      process, then store the offset. At-least-once, done right.
//	autocommit  let the timer commit whenever. Shows how messages vanish.
//	atmostonce  commit before processing. Deliberately wrong.
//	eos         consume-transform-produce in a transaction. Exactly-once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"example.com/kafkatraining/internal/config"
	"example.com/kafkatraining/internal/kclient"
	"example.com/kafkatraining/internal/model"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nconsumer: %v\n", err)
		os.Exit(1)
	}
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

type counters struct {
	received  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
	dlq       atomic.Int64
	produced  atomic.Int64 // eos mode: transformed messages written
	commits   atomic.Int64
	rebalance atomic.Int64
	perPart   map[int32]int64
	firstSeq  map[int32]int64
	lastSeq   map[int32]int64
	// runSeq is keyed by partition AND producer run. Each producer process
	// numbers its messages from 1, so two runs writing to one partition produce
	// two interleaved sequences. Comparing them as one would report an ordering
	// violation where Kafka has broken no promise.
	runSeq   map[string]int64
	outOfOrd int64
	eofSeen  map[int32]bool
}

func newCounters() *counters {
	return &counters{
		perPart:  map[int32]int64{},
		firstSeq: map[int32]int64{},
		lastSeq:  map[int32]int64{},
		eofSeen:  map[int32]bool{},
		runSeq:   map[string]int64{},
	}
}

type app struct {
	cfg   *config.Config
	log   *slog.Logger
	c     *kafka.Consumer
	dlq   *kafka.Producer
	out   *kafka.Producer
	ctr   *counters
	rnd   *rand.Rand
	topic string
	ctx   context.Context

	mode        string
	maxMessages int
	exitOnEOF   bool
	processTime time.Duration
	failRate    float64
	dlqTopic    string
	outTopic    string
	printMsgs   bool
	progressN   int
}

func run() error {
	var (
		clusterFile = flag.String("cluster", "config/cluster.properties", "connection properties (comma-separated to layer overlays)")
		consFile    = flag.String("config", "config/consumer.properties", "consumer properties (comma-separated)")
		topicFlag   = flag.String("topic", "", "topic (defaults to app.topic)")
		sets        multiFlag
	)
	flag.Var(&sets, "set", "override one property, repeatable: -set auto.offset.reset=latest")
	flag.Parse()

	cfg, err := config.LoadList(*clusterFile)
	if err != nil {
		return err
	}
	for _, p := range strings.Split(*consFile, ",") {
		if p = strings.TrimSpace(p); p != "" {
			if err := cfg.LoadFile(p); err != nil {
				return err
			}
		}
	}
	if err := cfg.Apply(sets); err != nil {
		return err
	}
	if err := cfg.Resolve(); err != nil {
		return err
	}
	if err := kclient.RequireCloudCredentials(cfg); err != nil {
		return err
	}

	a := &app{cfg: cfg, log: kclient.Logger(cfg), ctr: newCounters(), rnd: rand.New(rand.NewSource(7))}
	a.topic = *topicFlag
	if a.topic == "" {
		a.topic = cfg.App("topic", "orders")
	}
	a.mode = strings.ToLower(cfg.App("mode", "manual"))
	if a.maxMessages, err = cfg.AppInt("max.messages", 0); err != nil {
		return err
	}
	if a.exitOnEOF, err = cfg.AppBool("exit.on.eof", false); err != nil {
		return err
	}
	if a.processTime, err = cfg.AppDuration("process.time", 0); err != nil {
		return err
	}
	if a.failRate, err = parseFloat(cfg.App("fail.rate", "0")); err != nil {
		return err
	}
	a.dlqTopic = cfg.App("dlq.topic", "orders-dlq")
	a.outTopic = cfg.App("output.topic", "orders-processed")
	if a.printMsgs, err = cfg.AppBool("print.messages", false); err != nil {
		return err
	}
	if a.progressN, err = cfg.AppInt("progress.every", 1000); err != nil {
		return err
	}

	if err := a.validateModeAgainstConfig(); err != nil {
		return err
	}

	fmt.Print(config.Dump(cfg, fmt.Sprintf(
		"\n=== consumer configuration (librdkafka %s) ===", kclient.LibraryVersion())))
	fmt.Printf("\n=== mode: %s -> topic %q, group %q ===\n\n", a.mode, a.topic, cfg.Get("group.id"))
	a.explainMode()

	cm := kclient.ConfigMap(cfg)
	cm["client.id"] = fmt.Sprintf("%v-consumer", firstNonEmpty(cfg.Get("client.id"), "kafka-training"))

	a.c, err = kafka.NewConsumer(&cm)
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}
	defer a.c.Close()

	if logs := a.c.Logs(); logs != nil {
		go kclient.DrainLogs(logs, a.log)
	}

	if err := a.startHelperProducers(cfg); err != nil {
		return err
	}
	if a.dlq != nil {
		defer a.dlq.Close()
	}
	if a.out != nil {
		defer a.out.Close()
	}

	ctx, cancel := kclient.SignalContext(a.log)
	defer cancel()
	a.ctx = ctx

	// The rebalance callback MUST be passed here. go.application.rebalance.enable
	// only forwards rebalance events to the Events() channel, which the
	// channel-based consumer reads; a Poll()-based consumer like this one never
	// sees them that way. Pass nil here and librdkafka assigns partitions
	// silently, giving you no chance to commit offsets for a partition you are
	// about to lose.
	if err := a.c.Subscribe(a.topic, a.onRebalance); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	start := time.Now()
	loopErr := a.loop(ctx)

	// Shutting down cleanly is not optional. Committing what we have processed
	// stops the next run from redoing it, and leaving the group promptly stops
	// the rest of the group waiting session.timeout.ms to notice we are gone.
	a.shutdown()
	a.report(time.Since(start))
	if loopErr != nil && !errors.Is(loopErr, context.Canceled) {
		return loopErr
	}
	return nil
}

// validateModeAgainstConfig refuses to run when the commit properties contradict
// the requested mode, rather than silently overriding them. The properties file
// is the source of truth; this just makes a mismatch loud.
func (a *app) validateModeAgainstConfig() error {
	autoCommit := a.cfg.Get("enable.auto.commit")
	autoStore := a.cfg.Get("enable.auto.offset.store")

	switch a.mode {
	case "manual":
		if autoStore != "false" {
			return fmt.Errorf("app.mode=manual needs enable.auto.offset.store=false, but it is %q\n"+
				"  With it true, the offset is stored the moment a message is handed to your\n"+
				"  code - before processing - which is exactly the data loss manual mode avoids.\n"+
				"  Fix: -set enable.auto.offset.store=false", orDefault(autoStore, "true (the default)"))
		}
	case "autocommit":
		if autoCommit == "false" || autoStore == "false" {
			return fmt.Errorf("app.mode=autocommit needs enable.auto.commit=true and enable.auto.offset.store=true\n" +
				"  Fix: -set enable.auto.commit=true -set enable.auto.offset.store=true")
		}
	case "atmostonce", "eos":
		if autoCommit != "false" {
			return fmt.Errorf("app.mode=%s needs enable.auto.commit=false, but it is %q\n"+
				"  This mode commits offsets itself; a background committer would race it.\n"+
				"  Fix: -set enable.auto.commit=false", a.mode, orDefault(autoCommit, "true (the default)"))
		}
	default:
		return fmt.Errorf("unknown app.mode %q (manual, autocommit, atmostonce, eos)", a.mode)
	}
	if a.mode == "eos" && a.cfg.Get("app.output.transactional.id") == "" && a.cfg.App("output.transactional.id", "") == "" {
		return errors.New("app.mode=eos needs app.output.transactional.id")
	}
	return nil
}

func (a *app) explainMode() {
	switch a.mode {
	case "manual":
		fmt.Println("  Process the message, THEN store its offset. The background committer")
		fmt.Println("  flushes stored offsets efficiently, and a crash re-delivers anything not")
		fmt.Println("  yet stored. At-least-once. This is the pattern to copy.")
	case "autocommit":
		fmt.Println("  Offsets are stored on delivery and committed on a timer, with no regard")
		fmt.Println("  for whether processing succeeded. A crash mid-batch skips messages that")
		fmt.Println("  were committed but never processed. Watch the 'received' and 'processed'")
		fmt.Println("  counts diverge when app.fail.rate is above zero.")
	case "atmostonce":
		fmt.Println("  DELIBERATELY WRONG. The offset is committed BEFORE processing, so any")
		fmt.Println("  failure loses the message permanently - there is nothing to redeliver.")
	case "eos":
		fmt.Println("  Consume, transform, produce and commit the input offset ALL inside one")
		fmt.Println("  transaction via SendOffsetsToTransaction. Either the output exists and")
		fmt.Println("  the input is marked done, or neither happened.")
	}
	fmt.Println()
}

// startHelperProducers builds the producers this mode needs: a dead-letter
// producer when a failure rate is configured, and a transactional producer for
// exactly-once.
func (a *app) startHelperProducers(cfg *config.Config) error {
	base := kclient.ConfigMap(cfg)
	// Strip consumer-only properties - a producer rejects them outright.
	for _, k := range []string{
		"group.id", "group.instance.id", "group.protocol", "auto.offset.reset",
		"enable.auto.commit", "auto.commit.interval.ms", "enable.auto.offset.store",
		"isolation.level", "partition.assignment.strategy", "session.timeout.ms",
		"heartbeat.interval.ms", "max.poll.interval.ms", "fetch.min.bytes",
		"fetch.wait.max.ms", "fetch.max.bytes", "max.partition.fetch.bytes",
		"enable.partition.eof", "go.application.rebalance.enable",
	} {
		delete(base, k)
	}

	if a.failRate > 0 {
		cm := cloneConfig(base)
		cm["client.id"] = "kafka-training-dlq"
		cm["acks"] = "all"
		cm["enable.idempotence"] = "true"
		p, err := kafka.NewProducer(&cm)
		if err != nil {
			return fmt.Errorf("creating dead-letter producer: %w", err)
		}
		a.dlq = p
		go func() {
			for e := range p.Events() {
				if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
					a.log.Error("dead-letter write failed", "err", m.TopicPartition.Error)
				}
			}
		}()
	}

	if a.mode == "eos" {
		cm := cloneConfig(base)
		cm["client.id"] = "kafka-training-eos"
		cm["acks"] = "all"
		cm["enable.idempotence"] = "true"
		cm["transactional.id"] = a.cfg.App("output.transactional.id", "orders-eos-1")
		p, err := kafka.NewProducer(&cm)
		if err != nil {
			return fmt.Errorf("creating transactional producer: %w", err)
		}
		a.out = p
		go func() {
			for e := range p.Events() {
				if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
					if !isPurge(m.TopicPartition.Error) {
						a.log.Error("output write failed", "err", m.TopicPartition.Error)
					}
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		fmt.Println("  InitTransactions for the output producer...")
		if err := p.InitTransactions(ctx); err != nil {
			return fmt.Errorf("InitTransactions: %w", err)
		}
		if err := p.BeginTransaction(); err != nil {
			return fmt.Errorf("BeginTransaction: %w", err)
		}
		fmt.Println("  transaction open.")
	}
	return nil
}

// ---------------------------------------------------------------- main loop

func (a *app) loop(ctx context.Context) error {
	txnCount := 0
	const txnEvery = 100

	for {
		if ctx.Err() != nil {
			return nil
		}
		if a.maxMessages > 0 && a.ctr.received.Load() >= int64(a.maxMessages) {
			fmt.Printf("\n  reached app.max.messages=%d\n", a.maxMessages)
			return nil
		}

		// Poll drives EVERYTHING: fetching, heartbeats for the classic protocol,
		// rebalance callbacks and offset commits. Stop calling it and the group
		// evicts you after max.poll.interval.ms. This is why slow processing
		// belongs off this thread.
		ev := a.c.Poll(500)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			if err := a.handleMessage(ctx, e); err != nil {
				return err
			}
			if a.mode == "eos" {
				txnCount++
				if txnCount >= txnEvery {
					if err := a.commitTransaction(ctx); err != nil {
						return err
					}
					txnCount = 0
				}
			}

		// Rebalance events are NOT delivered here - they arrive on the
		// rebalance callback passed to Subscribe. See onRebalance.

		case kafka.PartitionEOF:
			a.ctr.eofSeen[e.Partition] = true
			a.log.Debug("reached end of partition", "partition", e.Partition, "offset", e.Offset)
			if a.exitOnEOF && a.allPartitionsAtEOF() {
				fmt.Println("\n  every assigned partition has reached its end (app.exit.on.eof)")
				return nil
			}

		case kafka.OffsetsCommitted:
			a.ctr.commits.Add(1)
			if e.Error != nil {
				// A commit failing after a rebalance is normal - the partitions
				// are no longer ours to commit. Anything else deserves attention.
				a.log.Warn("offset commit failed", "err", e.Error)
			}

		case kafka.Error:
			if e.IsFatal() {
				return fmt.Errorf("fatal consumer error: %w", e)
			}
			// All-brokers-down is usually transient; librdkafka reconnects.
			a.log.Warn("client error", "err", e, "code", e.Code(), "retriable", e.IsRetriable())
		}
	}
}

func (a *app) handleMessage(ctx context.Context, m *kafka.Message) error {
	a.ctr.received.Add(1)
	p := m.TopicPartition.Partition
	a.ctr.perPart[p]++

	// at-most-once commits FIRST. If processing then fails, the message is gone.
	if a.mode == "atmostonce" {
		if _, err := a.c.CommitMessage(m); err != nil {
			a.log.Warn("commit failed", "err", err)
		}
	}

	order, decodeErr := model.Decode(m.Value)
	if decodeErr == nil {
		// Per-partition ordering check. Kafka guarantees order WITHIN a partition,
		// so sequence numbers must only ever increase here. They will interleave
		// across partitions, which is not a violation.
		key := fmt.Sprintf("%d/%s", p, order.Run)
		if last, seen := a.ctr.runSeq[key]; seen && int64(order.Sequence) < last {
			a.ctr.outOfOrd++
		}
		a.ctr.runSeq[key] = int64(order.Sequence)
		if _, seen := a.ctr.lastSeq[p]; !seen {
			a.ctr.firstSeq[p] = int64(order.Sequence)
		}
		a.ctr.lastSeq[p] = int64(order.Sequence)
	}

	if a.printMsgs {
		fmt.Printf("  p%-2d o%-8d key=%-10s %s\n",
			p, int64(m.TopicPartition.Offset), string(m.Key), summarise(order, decodeErr, m.Value))
	}

	if err := a.process(ctx, m, order, decodeErr); err != nil {
		a.ctr.failed.Add(1)
		if a.dlq != nil {
			a.toDeadLetter(m, err)
		}
		// Deliberately NOT returning the error. One poison message must not stop
		// the consumer: it has been quarantined, and the offset advances past it.
		// Blocking on it instead would stall this partition and everything behind
		// it - the classic "consumer stuck at one offset" incident.
	} else {
		a.ctr.processed.Add(1)
	}

	// Offset handling AFTER processing. This ordering is the entire difference
	// between at-least-once and at-most-once.
	switch a.mode {
	case "manual":
		// StoreMessage marks offset+1 eligible. The background committer batches
		// the actual network calls, so this is both correct and efficient.
		if _, err := a.c.StoreMessage(m); err != nil {
			return fmt.Errorf("StoreMessage: %w", err)
		}
	case "autocommit":
		// Nothing to do - enable.auto.offset.store=true already stored it, before
		// we processed anything. That is the flaw being demonstrated.
	case "eos":
		// Offsets are committed as part of the transaction, in commitTransaction.
	}

	if a.progressN > 0 && a.ctr.received.Load()%int64(a.progressN) == 0 {
		fmt.Printf("  ... %d received, %d processed, %d failed\n",
			a.ctr.received.Load(), a.ctr.processed.Load(), a.ctr.failed.Load())
	}
	return nil
}

// process is the stand-in for real work.
func (a *app) process(ctx context.Context, m *kafka.Message, o model.Order, decodeErr error) error {
	if decodeErr != nil {
		return fmt.Errorf("undecodable message: %w", decodeErr)
	}
	if a.processTime > 0 {
		select {
		case <-time.After(a.processTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if a.failRate > 0 && a.rnd.Float64() < a.failRate {
		return fmt.Errorf("simulated processing failure for %s", o.OrderID)
	}
	if a.mode == "eos" && a.out != nil {
		// The "transform" half of consume-transform-produce. This write is inside
		// the open transaction, so it becomes visible only if the transaction
		// commits - together with the input offset.
		out := o
		out.SKU = strings.ToLower(o.SKU)
		val, err := out.Encode()
		if err != nil {
			return err
		}
		if err := a.out.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &a.outTopic, Partition: kafka.PartitionAny},
			Key:            out.Key(),
			Value:          val,
			Headers:        []kafka.Header{{Key: "source-topic", Value: []byte(a.topic)}},
		}, nil); err != nil {
			return fmt.Errorf("produce to output topic: %w", err)
		}
		a.ctr.produced.Add(1)
	}
	return nil
}

// toDeadLetter quarantines a message that cannot be processed, preserving the
// original key so related failures stay together, and recording WHY and WHERE it
// came from in headers. Without those headers a dead-letter topic is a pile of
// payloads nobody can act on.
func (a *app) toDeadLetter(m *kafka.Message, cause error) {
	err := a.dlq.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &a.dlqTopic, Partition: kafka.PartitionAny},
		Key:            m.Key,
		Value:          m.Value,
		Headers: []kafka.Header{
			{Key: "dlq-reason", Value: []byte(cause.Error())},
			{Key: "dlq-source-topic", Value: []byte(*m.TopicPartition.Topic)},
			{Key: "dlq-source-partition", Value: []byte(fmt.Sprintf("%d", m.TopicPartition.Partition))},
			{Key: "dlq-source-offset", Value: []byte(fmt.Sprintf("%d", int64(m.TopicPartition.Offset)))},
			{Key: "dlq-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}, nil)
	if err != nil {
		a.log.Error("could not write to the dead-letter topic", "err", err)
		return
	}
	a.ctr.dlq.Add(1)
}

// ---------------------------------------------------------------- transactions

func (a *app) commitTransaction(ctx context.Context) error {
	// The offsets THIS consumer would commit, sent through the producer so they
	// land atomically with the output messages. This single call is what makes
	// consume-transform-produce exactly-once.
	assigned, err := a.c.Assignment()
	if err != nil {
		return fmt.Errorf("Assignment: %w", err)
	}
	positions, err := a.c.Position(assigned)
	if err != nil {
		return fmt.Errorf("Position: %w", err)
	}
	meta, err := a.c.GetConsumerGroupMetadata()
	if err != nil {
		return fmt.Errorf("GetConsumerGroupMetadata: %w", err)
	}

	sendCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	err = a.out.SendOffsetsToTransaction(sendCtx, positions, meta)
	cancel()
	if err != nil {
		return a.abortTransaction(ctx, fmt.Errorf("SendOffsetsToTransaction: %w", err))
	}

	commitCtx, cancel2 := context.WithTimeout(ctx, 60*time.Second)
	err = a.out.CommitTransaction(commitCtx)
	cancel2()
	if err != nil {
		var kerr kafka.Error
		if errors.As(err, &kerr) && kerr.TxnRequiresAbort() {
			return a.abortTransaction(ctx, fmt.Errorf("commit rejected: %w", err))
		}
		return fmt.Errorf("CommitTransaction: %w", err)
	}
	a.ctr.commits.Add(1)
	return a.out.BeginTransaction()
}

func (a *app) abortTransaction(ctx context.Context, cause error) error {
	a.log.Warn("aborting transaction", "cause", cause)
	abortCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := a.out.AbortTransaction(abortCtx); err != nil {
		return fmt.Errorf("AbortTransaction after %v: %w", cause, err)
	}
	// After an abort the consumer must rewind to the committed offsets, or it
	// would carry on from messages whose output was just discarded.
	assigned, err := a.c.Assignment()
	if err != nil {
		return err
	}
	committed, err := a.c.Committed(assigned, 30000)
	if err != nil {
		return err
	}
	for _, tp := range committed {
		if err := a.c.Seek(tp, 5000); err != nil {
			a.log.Warn("seek after abort failed", "partition", tp.Partition, "err", err)
		}
	}
	return a.out.BeginTransaction()
}

// ---------------------------------------------------------------- rebalance

// onRebalance is called by librdkafka on the poll thread whenever the group
// assignment changes. It MUST call one of the assign/unassign methods for every
// event, or the consumer hangs: librdkafka waits for the application to
// acknowledge the new assignment before it resumes fetching.
func (a *app) onRebalance(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		a.onAssigned(e)
	case kafka.RevokedPartitions:
		a.onRevoked(a.ctx, e)
	}
	return nil
}

func (a *app) onAssigned(e kafka.AssignedPartitions) {
	a.ctr.rebalance.Add(1)
	proto := a.c.GetRebalanceProtocol()
	fmt.Printf("\n  >> REBALANCE (%s): assigned %d partition(s): %s\n", proto, len(e.Partitions), partList(e.Partitions))

	var err error
	if proto == "COOPERATIVE" {
		// Incremental: ADD these partitions to what we already hold. The ones we
		// keep were never revoked, so consumption of them never paused.
		err = a.c.IncrementalAssign(e.Partitions)
	} else {
		// Eager: this replaces the whole assignment. Every consumer in the group
		// stopped completely to get here.
		err = a.c.Assign(e.Partitions)
	}
	if err != nil {
		a.log.Error("assign failed", "err", err)
	}
}

func (a *app) onRevoked(ctx context.Context, e kafka.RevokedPartitions) {
	a.ctr.rebalance.Add(1)
	proto := a.c.GetRebalanceProtocol()
	fmt.Printf("\n  >> REBALANCE (%s): revoked %d partition(s): %s\n", proto, len(e.Partitions), partList(e.Partitions))

	// This is the ONLY chance to commit work for partitions about to move to
	// another consumer. Skip it and everything processed since the last commit is
	// redone by the new owner.
	if a.c.AssignmentLost() {
		// The assignment was taken away rather than handed back - we were evicted
		// after max.poll.interval.ms or session.timeout.ms. A commit now would be
		// rejected because we are no longer a member.
		fmt.Println("     assignment was LOST (we were evicted); offsets cannot be committed")
	} else if a.mode == "manual" || a.mode == "autocommit" {
		if _, err := a.c.Commit(); err != nil {
			var kerr kafka.Error
			if !errors.As(err, &kerr) || kerr.Code() != kafka.ErrNoOffset {
				a.log.Warn("final commit before revocation failed", "err", err)
			}
		}
	} else if a.mode == "eos" && a.out != nil {
		if err := a.commitTransaction(ctx); err != nil {
			a.log.Warn("committing transaction before revocation failed", "err", err)
		}
	}

	var err error
	if proto == "COOPERATIVE" {
		err = a.c.IncrementalUnassign(e.Partitions)
	} else {
		err = a.c.Unassign()
	}
	if err != nil {
		a.log.Error("unassign failed", "err", err)
	}
}

func (a *app) allPartitionsAtEOF() bool {
	assigned, err := a.c.Assignment()
	if err != nil || len(assigned) == 0 {
		return false
	}
	for _, tp := range assigned {
		if !a.ctr.eofSeen[tp.Partition] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- shutdown

func (a *app) shutdown() {
	fmt.Printf("\n=== shutting down ===\n")

	if a.mode == "eos" && a.out != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		if err := a.commitTransaction(ctx); err != nil {
			a.log.Warn("final transaction commit failed", "err", err)
		}
		cancel()
		a.out.Flush(15000)
	}

	if a.mode == "manual" || a.mode == "autocommit" {
		// A synchronous final commit. The background committer may not have run
		// since the last message, so without this the next run redoes up to
		// auto.commit.interval.ms of work.
		if _, err := a.c.Commit(); err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) && kerr.Code() == kafka.ErrNoOffset {
				fmt.Println("  nothing new to commit")
			} else {
				a.log.Warn("final commit failed", "err", err)
			}
		} else {
			a.ctr.commits.Add(1)
			fmt.Println("  final offsets committed")
		}
	}

	if a.dlq != nil {
		a.dlq.Flush(15000)
	}

	// Close leaves the group explicitly. Without it the group waits
	// session.timeout.ms before noticing, and nobody consumes our partitions in
	// the meantime.
	if err := a.c.Close(); err != nil {
		a.log.Warn("consumer close", "err", err)
	} else {
		fmt.Println("  left the consumer group cleanly")
	}
}

// ---------------------------------------------------------------- report

func (a *app) report(elapsed time.Duration) {
	c := a.ctr
	fmt.Printf("\n=== result ===\n\n")
	fmt.Printf("  mode          : %s\n", a.mode)
	fmt.Printf("  elapsed       : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  received      : %d\n", c.received.Load())
	fmt.Printf("  processed OK  : %d\n", c.processed.Load())
	fmt.Printf("  failed        : %d\n", c.failed.Load())
	if c.dlq.Load() > 0 {
		fmt.Printf("  dead-lettered : %d  -> topic %q\n", c.dlq.Load(), a.dlqTopic)
	}
	if c.produced.Load() > 0 {
		fmt.Printf("  produced      : %d  -> topic %q\n", c.produced.Load(), a.outTopic)
	}
	fmt.Printf("  commits       : %d\n", c.commits.Load())
	fmt.Printf("  rebalances    : %d\n", c.rebalance.Load())
	if elapsed > 0 && c.received.Load() > 0 {
		fmt.Printf("  throughput    : %.0f msg/s\n", float64(c.received.Load())/elapsed.Seconds())
	}

	if len(c.perPart) > 0 {
		parts := make([]int32, 0, len(c.perPart))
		for p := range c.perPart {
			parts = append(parts, p)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		fmt.Printf("\n  per partition:\n")
		fmt.Printf("    %-6s %-9s %s\n", "PART", "MESSAGES", "SEQUENCE RANGE")
		for _, p := range parts {
			fmt.Printf("    %-6d %-9d %d..%d\n", p, c.perPart[p], c.firstSeq[p], c.lastSeq[p])
		}
	}

	fmt.Printf("\n  out-of-order within a partition (per producer run): %d\n", c.outOfOrd)
	if c.outOfOrd == 0 && len(c.perPart) > 0 {
		fmt.Println("  Zero, as Kafka guarantees. Order holds WITHIN a partition, for one")
		fmt.Println("  producer, only. The sequence ranges above interleave across partitions,")
		fmt.Println("  and two producer runs interleave within one partition - both are correct.")
	} else if c.outOfOrd > 0 {
		fmt.Println("  Non-zero. With enable.idempotence=false and")
		fmt.Println("  max.in.flight.requests.per.connection>1, a retry can overtake an earlier")
		fmt.Println("  batch and reorder messages inside a partition.")
	}

	if a.mode == "autocommit" && c.failed.Load() > 0 {
		fmt.Printf("\n  %d message(s) failed but their offsets were committed anyway.\n", c.failed.Load())
		fmt.Println("  Restart this consumer: it will NOT redeliver them. They are gone.")
		fmt.Println("  Run app.mode=manual for the same workload handled correctly.")
	}
	fmt.Println()
}

// ---------------------------------------------------------------- helpers

func partList(tps []kafka.TopicPartition) string {
	ids := make([]int, 0, len(tps))
	for _, tp := range tps {
		ids = append(ids, int(tp.Partition))
	}
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func summarise(o model.Order, err error, raw []byte) string {
	if err != nil {
		return fmt.Sprintf("UNDECODABLE (%d bytes)", len(raw))
	}
	return o.Short()
}

func isPurge(err error) bool {
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return false
	}
	return kerr.Code() == kafka.ErrPurgeQueue || kerr.Code() == kafka.ErrPurgeInflight
}

func cloneConfig(m kafka.ConfigMap) kafka.ConfigMap {
	out := kafka.ConfigMap{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func parseFloat(s string) (float64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		return 0, fmt.Errorf("app.fail.rate: %q is not a number", s)
	}
	return f, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
