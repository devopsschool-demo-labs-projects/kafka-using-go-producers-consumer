// Command producer is PHASE 2: a producer whose every behaviour comes from
// config/producer.properties, so you can change one property, re-run, and see
// what it did.
//
// The one idea worth carrying away: Produce() is ASYNCHRONOUS. It appends to an
// in-memory queue and returns. A nil error from Produce means "queued", never
// "delivered". The delivery report that arrives later on the events channel is
// the only place the producer ever learns the truth. A program that ignores
// delivery reports cannot tell success from data loss - which is exactly what
// app.mode=fireforget demonstrates.
//
// Modes (app.mode):
//
//	async         queue, handle delivery reports on another goroutine, Flush at
//	              exit. The normal production pattern.
//	sync          block on each message's own delivery report. Correct, and about
//	              two orders of magnitude slower.
//	transactional group messages into transactions committed atomically.
//	fireforget    deliberately wrong: never look at a delivery report.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"example.com/kafkatraining/internal/config"
	"example.com/kafkatraining/internal/kclient"
	"example.com/kafkatraining/internal/model"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nproducer: %v\n", err)
		os.Exit(1)
	}
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// stats is the scoreboard. Counters are atomic because delivery reports are
// handled on a different goroutine from the one calling Produce.
type stats struct {
	produced  atomic.Int64 // handed to Produce() successfully
	delivered atomic.Int64 // confirmed by a delivery report
	failed    atomic.Int64 // rejected, reported by the broker or the client
	queueFull atomic.Int64 // times the local queue applied backpressure
	committed atomic.Int64 // messages inside committed transactions
	aborted   atomic.Int64 // messages inside aborted transactions
	purged    atomic.Int64 // discarded by an abort - expected, not a failure
	bytes     atomic.Int64

	mu        sync.Mutex
	perPart   map[int32]int64
	errKinds  map[string]int64
	firstErr  error
	latencies []time.Duration
}

func newStats() *stats {
	return &stats{perPart: map[int32]int64{}, errKinds: map[string]int64{}}
}

// purgedByAbort reports whether a delivery report failed only because
// AbortTransaction threw the message away. That is the abort working as designed,
// not a delivery failure, and counting it as one makes an aborted transaction
// look like an outage.
func purgedByAbort(err error) bool {
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return false
	}
	return kerr.Code() == kafka.ErrPurgeQueue || kerr.Code() == kafka.ErrPurgeInflight
}

func (s *stats) recordDelivery(m *kafka.Message) {
	if m.TopicPartition.Error != nil {
		if purgedByAbort(m.TopicPartition.Error) {
			s.purged.Add(1)
			return
		}
		s.failed.Add(1)
		s.mu.Lock()
		kind := m.TopicPartition.Error.Error()
		s.errKinds[kind]++
		if s.firstErr == nil {
			s.firstErr = m.TopicPartition.Error
		}
		s.mu.Unlock()
		return
	}
	s.delivered.Add(1)
	s.bytes.Add(int64(len(m.Value)))
	s.mu.Lock()
	s.perPart[m.TopicPartition.Partition]++
	s.mu.Unlock()
}

func run() error {
	var (
		clusterFile = flag.String("cluster", "config/cluster.properties", "connection properties (comma-separated to layer overlays)")
		prodFile    = flag.String("config", "config/producer.properties", "producer properties (comma-separated)")
		topicFlag   = flag.String("topic", "", "topic (defaults to app.topic)")
		sets        multiFlag
	)
	flag.Var(&sets, "set", "override one property, repeatable: -set acks=1 -set linger.ms=0")
	flag.Parse()

	// Connection settings first, producer settings on top, then -set overrides.
	// The layering is what makes the lab's "change one knob" instructions work
	// without ever editing a file.
	cfg, err := config.LoadList(*clusterFile)
	if err != nil {
		return err
	}
	for _, p := range strings.Split(*prodFile, ",") {
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

	log := kclient.Logger(cfg)
	topic := *topicFlag
	if topic == "" {
		topic = cfg.App("topic", "orders")
	}
	mode := strings.ToLower(cfg.App("mode", "async"))
	count, err := cfg.AppInt("message.count", 1000)
	if err != nil {
		return err
	}
	interval, err := cfg.AppDuration("interval", 0)
	if err != nil {
		return err
	}
	txnSize, err := cfg.AppInt("transaction.size", 100)
	if err != nil {
		return err
	}
	abortEvery, err := cfg.AppInt("transaction.abort.every", 0)
	if err != nil {
		return err
	}
	flushTimeout, err := cfg.AppDuration("flush.timeout", 30*time.Second)
	if err != nil {
		return err
	}
	cardinality, err := cfg.AppInt("key.cardinality", 20)
	if err != nil {
		return err
	}
	nullKeys, err := cfg.AppBool("null.keys", false)
	if err != nil {
		return err
	}

	cm := kclient.ConfigMap(cfg)
	cm["client.id"] = fmt.Sprintf("%v-producer", firstNonEmpty(cfg.Get("client.id"), "kafka-training"))

	// A transactional producer needs its identity BEFORE it is constructed.
	// Guard it here rather than letting InitTransactions fail with a bare
	// "Local: Invalid argument".
	if mode == "transactional" && cfg.Get("transactional.id") == "" {
		return errors.New("app.mode=transactional needs transactional.id set\n" +
			"  uncomment it in config/producer.properties, or pass\n" +
			"  -set transactional.id=orders-producer-1")
	}
	if cfg.Get("transactional.id") != "" && mode != "transactional" {
		log.Warn("transactional.id is set but app.mode is not transactional; " +
			"the producer will be idempotent but will not open transactions")
	}

	st := newStats()

	fmt.Print(config.Dump(cfg, fmt.Sprintf(
		"\n=== producer configuration (librdkafka %s) ===", kclient.LibraryVersion())))
	fmt.Printf("\n=== mode: %s -> topic %q ===\n\n", mode, topic)
	explainMode(mode)

	p, err := kafka.NewProducer(&cm)
	if err != nil {
		return fmt.Errorf("creating producer: %w", err)
	}

	if logs := p.Logs(); logs != nil {
		go kclient.DrainLogs(logs, log)
	}

	// ONE goroutine owns the events channel, for every mode except fireforget.
	// If nobody drains it, it fills and the producer stalls in a way that looks
	// exactly like a slow broker. fireforget deliberately does not drain, which
	// is precisely why it cannot account for its own messages.
	drained := make(chan struct{})
	if mode == "fireforget" {
		close(drained)
	} else {
		go func() {
			defer close(drained)
			for e := range p.Events() {
				handleEvent(e, st, log)
			}
		}()
	}

	ctx, cancel := kclient.SignalContext(log)
	defer cancel()

	gen := &generator{
		topic: topic, cardinality: cardinality, nullKeys: nullKeys,
		rnd: rand.New(rand.NewSource(1)),
		run: fmt.Sprintf("%d", time.Now().UnixNano()),
	}

	start := time.Now()
	var runErr error
	switch mode {
	case "async":
		runErr = runAsync(ctx, p, gen, st, log, count, interval)
	case "sync":
		runErr = runSync(ctx, p, gen, st, log, count, interval)
	case "fireforget":
		runErr = runFireForget(ctx, p, gen, st, log, count, interval)
	case "transactional":
		runErr = runTransactional(ctx, p, gen, st, log, count, interval, txnSize, abortEvery)
	default:
		return fmt.Errorf("unknown app.mode %q (async, sync, transactional, fireforget)", mode)
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}

	// Flush is the difference between a clean exit and losing whatever is still
	// sitting in the queue. It blocks until the queue drains or the timeout
	// expires, and returns how many messages it gave up on.
	fmt.Printf("\n=== flushing (up to %s) ===\n", flushTimeout)
	remaining := p.Flush(int(flushTimeout / time.Millisecond))
	// Close (below) ends the events channel, which lets the drain goroutine finish
	// having seen every delivery report. Reporting before that would undercount.
	if remaining > 0 {
		fmt.Printf("  WARNING: %d message(s) were still unsent when the flush timed out.\n", remaining)
		fmt.Printf("  They are gone. This is what happens to a producer that exits without\n")
		fmt.Printf("  flushing, or that does not allow enough time for it.\n")
	} else {
		fmt.Printf("  queue drained; every accepted message reached a final verdict\n")
	}

	p.Close()
	<-drained
	report(st, time.Since(start), mode)
	return nil
}

func explainMode(mode string) {
	switch mode {
	case "async":
		fmt.Println("  Produce() queues and returns immediately. Delivery reports are handled")
		fmt.Println("  on a separate goroutine. This is the pattern you want in production.")
	case "sync":
		fmt.Println("  Each message waits for its own delivery report before the next is sent.")
		fmt.Println("  Correct, and dramatically slower - compare the throughput line at the end")
		fmt.Println("  with the same run in async mode.")
	case "fireforget":
		fmt.Println("  DELIBERATELY WRONG. Delivery reports are discarded, so this producer")
		fmt.Println("  cannot distinguish a delivered message from a lost one. Compare the")
		fmt.Println("  'produced' and 'delivered' counts at the end.")
		fmt.Println()
		fmt.Println("  Expect the flush at the end to hang for its FULL timeout. Nothing is")
		fmt.Println("  draining the events channel, so the delivery reports pile up in it and")
		fmt.Println("  Flush can never see the queue empty. A producer that forgets to drain")
		fmt.Println("  Events() behaves exactly like this in production, and it is invariably")
		fmt.Println("  misdiagnosed as a slow broker.")
	case "transactional":
		fmt.Println("  Messages are grouped into transactions. Each commit is atomic: consumers")
		fmt.Println("  running isolation.level=read_committed see all of a transaction or none.")
	}
	fmt.Println()
}

// ---------------------------------------------------------------- generator

type generator struct {
	topic       string
	cardinality int
	nullKeys    bool
	rnd         *rand.Rand
	seq         int
	run         string
}

var skus = []string{"SKU-APPLE", "SKU-BANANA", "SKU-CHERRY", "SKU-DATE", "SKU-ELDER"}

func (g *generator) next() (*kafka.Message, model.Order, error) {
	g.seq++
	o := model.Order{
		OrderID:     fmt.Sprintf("ord-%06d", g.seq),
		CustomerID:  fmt.Sprintf("cust-%03d", g.rnd.Intn(g.cardinality)),
		SKU:         skus[g.rnd.Intn(len(skus))],
		Quantity:    1 + g.rnd.Intn(5),
		AmountCents: int64(500 + g.rnd.Intn(50000)),
		CreatedAt:   time.Now().UTC(),
		Sequence:    g.seq,
		Run:         g.run,
	}
	val, err := o.Encode()
	if err != nil {
		return nil, o, err
	}
	msg := &kafka.Message{
		// PartitionAny hands the choice to the configured partitioner. Naming an
		// explicit partition here would bypass it entirely - and would also mean
		// your producer, not Kafka, owns rebalancing when partitions change.
		TopicPartition: kafka.TopicPartition{Topic: &g.topic, Partition: kafka.PartitionAny},
		Value:          val,
		// Headers carry metadata without touching the payload, so a consumer can
		// route or trace without deserialising the value. Ideal for trace ids,
		// schema versions and the reason a message landed on a dead-letter topic.
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "producer", Value: []byte("kafka-training")},
			{Key: "seq", Value: []byte(fmt.Sprintf("%d", g.seq))},
		},
	}
	if !g.nullKeys {
		msg.Key = o.Key()
	}
	return msg, o, nil
}

// ---------------------------------------------------------------- async

func runAsync(ctx context.Context, p *kafka.Producer, gen *generator, st *stats, log *slog.Logger, count int, interval time.Duration) error {
	// Nothing to do here but queue. The events goroutine started by run() is
	// already draining delivery reports, and run() owns the flush and the close.
	return produceLoop(ctx, p, gen, st, log, count, interval, nil)
}

// ---------------------------------------------------------------- sync

func runSync(ctx context.Context, p *kafka.Producer, gen *generator, st *stats, log *slog.Logger, count int, interval time.Duration) error {
	// A per-message delivery channel. Passing one to Produce routes THIS
	// message's report here instead of to the shared events channel, which is how
	// you wait for one specific message.
	dr := make(chan kafka.Event, 1)
	defer close(dr)

	for i := 0; count == 0 || i < count; i++ {
		if ctx.Err() != nil {
			return nil
		}
		msg, o, err := gen.next()
		if err != nil {
			return err
		}
		sent := time.Now()
		if err := produceWithBackpressure(ctx, p, msg, st, log, dr); err != nil {
			return err
		}
		st.produced.Add(1)

		// This wait is the entire cost of sync mode: one network round trip per
		// message, no batching, no pipelining.
		select {
		case e := <-dr:
			m := e.(*kafka.Message)
			st.mu.Lock()
			st.latencies = append(st.latencies, time.Since(sent))
			st.mu.Unlock()
			st.recordDelivery(m)
			if m.TopicPartition.Error != nil && !purgedByAbort(m.TopicPartition.Error) {
				log.Error("delivery failed", "order", o.OrderID, "err", m.TopicPartition.Error)
			} else if m.TopicPartition.Error == nil {
				log.Debug("delivered", "order", o.OrderID,
					"partition", m.TopicPartition.Partition, "offset", m.TopicPartition.Offset)
			}
		case <-ctx.Done():
			return nil
		}
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	return nil
}

// ---------------------------------------------------------------- fire and forget

func runFireForget(ctx context.Context, p *kafka.Producer, gen *generator, st *stats, log *slog.Logger, count int, interval time.Duration) error {
	// No events goroutine at all. Reports pile up on a channel nobody reads, so
	// this producer has no idea what happened to anything. Deliberately wrong.
	return produceLoop(ctx, p, gen, st, log, count, interval, nil)
}

// ---------------------------------------------------------------- transactional

func runTransactional(ctx context.Context, p *kafka.Producer, gen *generator, st *stats, log *slog.Logger, count int, interval time.Duration, txnSize, abortEvery int) error {
	// InitTransactions does three things, and every one of them matters:
	//   1. registers transactional.id with the transaction coordinator
	//   2. FENCES any older producer using the same id, so a zombie instance that
	//      was network-partitioned can never write again
	//   3. aborts any transaction a previous run of THIS id left dangling
	// It blocks until the coordinator is ready, so give it a real timeout.
	initCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	fmt.Println("  InitTransactions: registering with the coordinator and fencing older instances...")
	if err := p.InitTransactions(initCtx); err != nil {
		return fmt.Errorf("InitTransactions: %w\n"+
			"  the transaction coordinator was unreachable, or another producer holds this\n"+
			"  transactional.id. On a local cluster check that\n"+
			"  transaction.state.log.replication.factor and min.isr can be satisfied", err)
	}
	fmt.Println("  registered.")

	txn := 0
	for i := 0; count == 0 || i < count; {
		if ctx.Err() != nil {
			// Ctrl+C mid-transaction: abort rather than leave it open. An open
			// transaction blocks every read_committed consumer on those partitions
			// until it times out.
			fmt.Println("\n  interrupted mid-transaction; aborting it so read_committed consumers are not blocked")
			abortCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			if err := p.AbortTransaction(abortCtx); err != nil {
				log.Error("AbortTransaction", "err", err)
			}
			return nil
		}

		if err := p.BeginTransaction(); err != nil {
			return fmt.Errorf("BeginTransaction: %w", err)
		}
		txn++

		batch := txnSize
		if count > 0 && count-i < batch {
			batch = count - i
		}
		for j := 0; j < batch; j++ {
			msg, _, err := gen.next()
			if err != nil {
				return err
			}
			if err := produceWithBackpressure(ctx, p, msg, st, log, nil); err != nil {
				return err
			}
			st.produced.Add(1)
			i++
		}

		// A deliberate abort, so you can prove that a read_committed consumer
		// never sees these messages while a read_uncommitted one does.
		if abortEvery > 0 && txn%abortEvery == 0 {
			// Flush BEFORE aborting, or this demonstrates the wrong thing.
			//
			// AbortTransaction purges whatever is still sitting in the LOCAL
			// queue - those messages never reach the broker, so there is nothing
			// for a read_uncommitted consumer to see and the abort looks like a
			// no-op. Flushing first pushes them to the broker, where they are
			// written to the log and then marked aborted. Only then is the
			// difference between read_committed and read_uncommitted visible.
			p.Flush(30000)
			fmt.Printf("  txn %d: ABORTING deliberately (%d messages already on the broker, "+
				"about to be marked aborted)\n", txn, batch)
			abortCtx, c := context.WithTimeout(ctx, 30*time.Second)
			err := p.AbortTransaction(abortCtx)
			c()
			if err != nil {
				return fmt.Errorf("AbortTransaction: %w", err)
			}
			st.aborted.Add(int64(batch))
			continue
		}

		commitCtx, c := context.WithTimeout(ctx, 60*time.Second)
		err := p.CommitTransaction(commitCtx)
		c()
		if err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) {
				switch {
				case kerr.TxnRequiresAbort():
					// The coordinator says this transaction cannot commit. Abort it
					// and carry on; the messages in it never become visible.
					log.Warn("transaction must be aborted", "txn", txn, "err", kerr)
					abortCtx, ac := context.WithTimeout(ctx, 30*time.Second)
					if aerr := p.AbortTransaction(abortCtx); aerr != nil {
						ac()
						return fmt.Errorf("AbortTransaction after failed commit: %w", aerr)
					}
					ac()
					st.aborted.Add(int64(batch))
					continue
				case kerr.IsFatal():
					// Fenced by a newer producer with the same transactional.id, or
					// the producer id was lost. This instance is finished.
					return fmt.Errorf("fatal transaction error; this producer has been fenced "+
						"and must not continue: %w", kerr)
				}
			}
			return fmt.Errorf("CommitTransaction: %w", err)
		}
		st.committed.Add(int64(batch))
		log.Debug("transaction committed", "txn", txn, "messages", batch)

		if interval > 0 {
			time.Sleep(interval)
		}
	}

	fmt.Printf("\n  %d transaction(s) closed\n", txn)
	return nil
}

// ---------------------------------------------------------------- shared

func produceLoop(ctx context.Context, p *kafka.Producer, gen *generator, st *stats, log *slog.Logger, count int, interval time.Duration, dr chan kafka.Event) error {
	for i := 0; count == 0 || i < count; i++ {
		if ctx.Err() != nil {
			log.Info("stopping early", "produced", st.produced.Load())
			return nil
		}
		msg, _, err := gen.next()
		if err != nil {
			return err
		}
		if err := produceWithBackpressure(ctx, p, msg, st, log, dr); err != nil {
			return err
		}
		st.produced.Add(1)
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	return nil
}

// produceWithBackpressure is the part most example code gets wrong.
//
// When the local queue is full, Produce returns ErrQueueFull. That is NOT a
// failure - it is the producer telling you it cannot accept work as fast as you
// are offering it, usually because the broker is briefly slow. The correct
// response is to wait and try again. Treating it as fatal turns a survivable
// 30-second leader election into an outage.
func produceWithBackpressure(ctx context.Context, p *kafka.Producer, msg *kafka.Message, st *stats, log *slog.Logger, dr chan kafka.Event) error {
	const maxWait = 60 * time.Second
	deadline := time.Now().Add(maxWait)
	backoff := 5 * time.Millisecond
	for {
		err := p.Produce(msg, dr)
		if err == nil {
			return nil
		}
		var kerr kafka.Error
		if errors.As(err, &kerr) && kerr.Code() == kafka.ErrQueueFull {
			st.queueFull.Add(1)
			if time.Now().After(deadline) {
				return fmt.Errorf("local queue still full after %s: the broker is not keeping up; "+
					"raise queue.buffering.max.messages or slow the producer down", maxWait)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 500*time.Millisecond {
				backoff *= 2
			}
			continue
		}
		// Anything else is a real rejection: message too large, unknown topic,
		// authorisation denied. Retrying will not help.
		return fmt.Errorf("produce: %w", err)
	}
}

func handleEvent(e kafka.Event, st *stats, log *slog.Logger) {
	switch ev := e.(type) {
	case *kafka.Message:
		st.recordDelivery(ev)
		if ev.TopicPartition.Error != nil {
			if purgedByAbort(ev.TopicPartition.Error) {
				// Expected: AbortTransaction discards everything still queued.
				log.Debug("discarded by transaction abort",
					"partition", ev.TopicPartition.Partition)
				return
			}
			log.Error("delivery failed",
				"partition", ev.TopicPartition.Partition, "err", ev.TopicPartition.Error)
		} else {
			log.Debug("delivered",
				"partition", ev.TopicPartition.Partition, "offset", ev.TopicPartition.Offset)
		}
	case kafka.Error:
		// A generic client error. IsFatal means the producer is unusable and must
		// be recreated - with an idempotent producer that happens if it loses its
		// producer id and can no longer guarantee de-duplication.
		if ev.IsFatal() {
			log.Error("FATAL client error; this producer can no longer guarantee "+
				"idempotence and must be recreated", "err", ev)
		} else {
			log.Warn("client error", "err", ev, "retriable", ev.IsRetriable())
		}
	case *kafka.Stats:
		log.Debug("librdkafka statistics", "json", len(ev.String()))
	}
}

// ---------------------------------------------------------------- report

func report(st *stats, elapsed time.Duration, mode string) {
	produced, delivered, failed := st.produced.Load(), st.delivered.Load(), st.failed.Load()

	fmt.Printf("\n=== result ===\n\n")
	fmt.Printf("  mode                : %s\n", mode)
	fmt.Printf("  elapsed             : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  produced (queued)   : %d\n", produced)
	fmt.Printf("  delivered (confirmed): %d\n", delivered)
	fmt.Printf("  failed              : %d\n", failed)
	if st.committed.Load() > 0 || st.aborted.Load() > 0 {
		fmt.Printf("  in committed txns   : %d\n", st.committed.Load())
		fmt.Printf("  in ABORTED txns     : %d   (a read_committed consumer will never see these)\n", st.aborted.Load())
		fmt.Printf("\n  The aborted messages WERE written to the log and then marked aborted.\n")
		fmt.Printf("  Run the consumer with isolation.level=read_committed and then\n")
		fmt.Printf("  read_uncommitted: the difference in the counts is the guarantee.\n")
	}
	if st.queueFull.Load() > 0 {
		fmt.Printf("  queue-full waits    : %d   (backpressure worked; nothing was lost)\n", st.queueFull.Load())
	}
	if elapsed > 0 && delivered > 0 {
		fmt.Printf("  throughput          : %.0f msg/s, %.2f MB/s\n",
			float64(delivered)/elapsed.Seconds(),
			float64(st.bytes.Load())/elapsed.Seconds()/(1024*1024))
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	if len(st.latencies) > 0 {
		sort.Slice(st.latencies, func(i, j int) bool { return st.latencies[i] < st.latencies[j] })
		fmt.Printf("  latency p50/p99     : %s / %s\n",
			st.latencies[len(st.latencies)*50/100].Round(time.Microsecond),
			st.latencies[min(len(st.latencies)*99/100, len(st.latencies)-1)].Round(time.Microsecond))
	}

	if len(st.perPart) > 0 {
		parts := make([]int32, 0, len(st.perPart))
		for p := range st.perPart {
			parts = append(parts, p)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		fmt.Printf("\n  distribution across %d partition(s):\n", len(parts))
		for _, p := range parts {
			fmt.Printf("    partition %-3d %6d  %s\n", p, st.perPart[p], bar(st.perPart[p], delivered))
		}
		fmt.Printf("\n  Keyed messages are NOT spread evenly - they are spread by hash(key).\n")
		fmt.Printf("  Even distribution is a consequence of having many distinct keys, never\n")
		fmt.Printf("  a guarantee. One hot key means one hot partition.\n")
	}

	if len(st.errKinds) > 0 {
		fmt.Printf("\n  failures by kind:\n")
		for k, n := range st.errKinds {
			fmt.Printf("    %-52s %d\n", truncate(k, 52), n)
		}
	}

	if st.purged.Load() > 0 {
		fmt.Printf("  purged by abort     : %d   (expected: AbortTransaction discards the queue)\n", st.purged.Load())
	}
	if produced != delivered+failed+st.purged.Load() {
		fmt.Printf("\n  *** %d message(s) were queued but never reached a verdict. ***\n", produced-delivered-failed-st.purged.Load())
		if mode == "fireforget" {
			fmt.Printf("  That is the point of this mode: with delivery reports ignored, the\n")
			fmt.Printf("  producer cannot tell you whether they arrived. Run app.mode=async to\n")
			fmt.Printf("  see the same workload accounted for properly.\n")
		}
	}
	fmt.Println()
}

func bar(n, total int64) string {
	if total == 0 {
		return ""
	}
	w := int(n * 40 / total)
	return strings.Repeat("#", w)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
