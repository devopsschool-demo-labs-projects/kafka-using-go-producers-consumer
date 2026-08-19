// Command topicadmin is PHASE 1: create and configure a topic, its partitions
// and its durability settings, entirely from a .properties file.
//
// A topic is not a detail you get to revisit cheaply. Partition count can be
// raised but never lowered, and raising it re-maps which partition a key hashes
// to, which breaks per-key ordering. min.insync.replicas is half of a contract
// whose other half lives in the producer. Retention is your replay window, and
// you only discover it was too short on the day you need it. So the topic is
// created deliberately, from a reviewed file, not implicitly by a producer that
// happened to name it.
//
// Subcommands:
//
//	create      create the topic from config/topic.properties
//	describe    show partitions, replicas, ISR and EVERY config entry with its
//	            source and whether this cluster lets you change it
//	alter       apply changed topic settings to a topic that already exists
//	partitions  increase the partition count (one way only)
//	offsets     low and high watermark per partition
//	list        list topics on the cluster
//	delete      delete the topic
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"example.com/kafkatraining/internal/config"
	"example.com/kafkatraining/internal/kclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\ntopicadmin: %v\n", err)
		os.Exit(1)
	}
}

type flags struct {
	clusterFile string
	topicFile   string
	cmd         string
	topic       string
	partitions  int
	dryRun      bool
	timeout     time.Duration
	sets        multiFlag
	showAll     bool
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func run() error {
	var f flags
	flag.StringVar(&f.clusterFile, "cluster", "config/cluster.properties", "connection properties (comma-separated to layer overlays)")
	flag.StringVar(&f.topicFile, "topic-config", "config/topic.properties", "topic properties")
	flag.StringVar(&f.cmd, "cmd", "describe", "create | describe | alter | partitions | offsets | list | delete")
	flag.StringVar(&f.topic, "topic", "", "topic name (defaults to app.topic from the config)")
	flag.IntVar(&f.partitions, "to", 0, "new partition count for -cmd partitions")
	flag.BoolVar(&f.dryRun, "dry-run", false, "ask the broker to validate without applying (create and alter)")
	flag.BoolVar(&f.showAll, "all", false, "describe: include entries left at their cluster default")
	flag.DurationVar(&f.timeout, "timeout", 30*time.Second, "admin request timeout")
	flag.Var(&f.sets, "set", "override one property, repeatable: -set retention.ms=3600000")
	flag.Parse()

	// Two separate config objects, deliberately. cluster.properties holds CLIENT
	// properties that librdkafka understands; topic.properties holds BROKER-side
	// topic config entries. Merging them would hand "retention.ms" to the client,
	// which rejects unknown properties at startup.
	cluster, err := config.LoadList(f.clusterFile)
	if err != nil {
		return err
	}
	topicCfg, err := config.LoadList(f.topicFile)
	if err != nil {
		return err
	}
	// -set applies to the topic settings, since that is what this tool tunes.
	if err := topicCfg.Apply(f.sets); err != nil {
		return err
	}

	// Every ${VAR} must be satisfied by now, after all files and -set overrides.
	if err := cluster.Resolve(); err != nil {
		return err
	}
	if err := topicCfg.Resolve(); err != nil {
		return err
	}

	log := kclient.Logger(cluster)
	if err := kclient.RequireCloudCredentials(cluster); err != nil {
		return err
	}

	topic := f.topic
	if topic == "" {
		topic = topicCfg.App("topic", cluster.App("topic", "orders"))
	}

	fmt.Print(config.Dump(cluster, fmt.Sprintf(
		"\n=== admin client configuration (librdkafka %s) ===", kclient.LibraryVersion())))

	admin, err := kafka.NewAdminClient(toConfigMap(cluster))
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	defer admin.Close()

	if logs := admin.Logs(); logs != nil {
		go kclient.DrainLogs(logs, log)
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.timeout+15*time.Second)
	defer cancel()

	switch f.cmd {
	case "create":
		return create(ctx, admin, topicCfg, topic, f)
	case "describe":
		return describe(ctx, admin, topic, f)
	case "alter":
		return alter(ctx, admin, topicCfg, topic, f)
	case "partitions":
		return addPartitions(ctx, admin, topic, f)
	case "offsets":
		return offsets(ctx, admin, topic, f)
	case "list":
		return list(admin, f)
	case "delete":
		return deleteTopic(ctx, admin, topic, f)
	default:
		return fmt.Errorf("unknown -cmd %q", f.cmd)
	}
}

func toConfigMap(c *config.Config) *kafka.ConfigMap {
	m := kclient.ConfigMap(c)
	if _, ok := m["client.id"]; ok {
		m["client.id"] = fmt.Sprintf("%v-topicadmin", m["client.id"])
	}
	return &m
}

// topicEntries returns every non-app key from topic.properties: the topic config
// entries, spelled exactly as Kafka spells them.
func topicEntries(c *config.Config) map[string]string { return c.Kafka() }

// ---------------------------------------------------------------- create

func create(ctx context.Context, admin *kafka.AdminClient, c *config.Config, topic string, f flags) error {
	parts, err := c.AppInt("num.partitions", 6)
	if err != nil {
		return err
	}
	rf, err := c.AppInt("replication.factor", 3)
	if err != nil {
		return err
	}
	entries := topicEntries(c)

	fmt.Printf("\n=== creating topic %q ===\n", topic)
	fmt.Printf("  partitions         : %d\n", parts)
	if rf < 0 {
		fmt.Printf("  replication.factor : cluster default (Confluent Cloud enforces 3)\n")
		rf = 0 // 0 tells the broker to apply its own default
	} else {
		fmt.Printf("  replication.factor : %d\n", rf)
	}
	fmt.Printf("  config entries     : %d\n", len(entries))
	for _, k := range sortedKeys(entries) {
		fmt.Printf("      %-32s = %s\n", k, entries[k])
	}

	opts := []kafka.CreateTopicsAdminOption{kafka.SetAdminOperationTimeout(f.timeout)}
	if f.dryRun {
		// The broker validates the whole request and applies none of it. This is
		// how you find out that min.insync.replicas=3 is rejected BEFORE you have
		// half-created a topic.
		opts = append(opts, kafka.SetAdminValidateOnly(true))
		fmt.Println("\n  -dry-run: the broker will validate this request and change nothing")
	}

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     parts,
		ReplicationFactor: rf,
		Config:            entries,
	}}, opts...)
	if err != nil {
		return fmt.Errorf("CreateTopics: %w", err)
	}

	var failed error
	for _, r := range results {
		switch r.Error.Code() {
		case kafka.ErrNoError:
			if f.dryRun {
				fmt.Printf("\n  OK   %s would be created; the broker accepted every setting\n", r.Topic)
			} else {
				fmt.Printf("\n  OK   %s created\n", r.Topic)
			}
		case kafka.ErrTopicAlreadyExists:
			// Not an error worth failing on: creation is meant to be re-runnable.
			// Use -cmd alter to change the settings of a topic that already exists.
			fmt.Printf("\n  SKIP %s already exists; use -cmd alter to change its settings\n", r.Topic)
		default:
			fmt.Printf("\n  FAIL %s: %v\n", r.Topic, r.Error)
			failed = errors.Join(failed, fmt.Errorf("%s: %w", r.Topic, r.Error))
		}
	}
	if failed != nil {
		return failed
	}
	if f.dryRun {
		return nil
	}
	// CreateTopics returns once the controller has accepted the topic, but this
	// client's metadata cache does not know about it yet, and describing straight
	// away reports "topic does not exist". The same lag is why a producer started
	// the instant a topic is created can briefly see UNKNOWN_TOPIC_OR_PART.
	if err := waitForTopic(admin, topic, 30*time.Second); err != nil {
		return err
	}
	fmt.Println()
	return describe(ctx, admin, topic, f)
}

// waitForTopic polls until the topic appears in metadata with its partitions.
func waitForTopic(admin *kafka.AdminClient, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		md, err := admin.GetMetadata(&topic, false, 5000)
		if err == nil {
			if tm, ok := md.Topics[topic]; ok &&
				tm.Error.Code() == kafka.ErrNoError && len(tm.Partitions) > 0 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %q was created but has not appeared in metadata after %s", topic, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- describe

func describe(ctx context.Context, admin *kafka.AdminClient, topic string, f flags) error {
	// Part 1: the physical layout. Metadata answers "where does this data live".
	md, err := admin.GetMetadata(&topic, false, int(f.timeout/time.Millisecond))
	if err != nil {
		return fmt.Errorf("GetMetadata: %w", err)
	}
	tm, ok := md.Topics[topic]
	if !ok || tm.Error.Code() == kafka.ErrUnknownTopicOrPart {
		return fmt.Errorf("topic %q does not exist (run -cmd create)", topic)
	}
	if tm.Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("topic %q: %w", topic, tm.Error)
	}

	fmt.Printf("\n=== topic %q ===\n", topic)
	fmt.Printf("  partitions: %d\n\n", len(tm.Partitions))
	fmt.Printf("  %-6s %-8s %-18s %-18s\n", "PART", "LEADER", "REPLICAS", "IN-SYNC (ISR)")
	parts := append([]kafka.PartitionMetadata(nil), tm.Partitions...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })
	underReplicated := 0
	for _, p := range parts {
		if len(p.Isrs) < len(p.Replicas) {
			underReplicated++
		}
		fmt.Printf("  %-6d %-8d %-18s %-18s\n", p.ID, p.Leader, ints(p.Replicas), ints(p.Isrs))
	}
	// ISR shorter than the replica list means a replica is lagging. If it drops
	// below min.insync.replicas, every acks=all produce to that partition starts
	// failing with NOT_ENOUGH_REPLICAS.
	if underReplicated > 0 {
		fmt.Printf("\n  WARNING: %d partition(s) are under-replicated - a replica has fallen out of the ISR\n", underReplicated)
	}

	// Part 2: the config entries, straight from the broker. This is the honest
	// answer to "what can I actually change on this cluster?" - it comes from the
	// cluster itself, so it is right for Confluent Cloud Basic, Standard,
	// Dedicated and a local broker alike.
	res, err := admin.DescribeConfigs(ctx,
		[]kafka.ConfigResource{{Type: kafka.ResourceTopic, Name: topic}},
		kafka.SetAdminRequestTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("DescribeConfigs: %w", err)
	}
	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("DescribeConfigs %s: %w", r.Name, r.Error)
		}
		names := make([]string, 0, len(r.Config))
		for n := range r.Config {
			names = append(names, n)
		}
		sort.Strings(names)

		fmt.Printf("\n  === configuration (%d entries) ===\n", len(names))
		fmt.Printf("  %-40s %-26s %-22s %s\n", "PROPERTY", "VALUE", "SOURCE", "EDITABLE HERE?")
		shown, hidden, readOnly := 0, 0, 0
		for _, n := range names {
			e := r.Config[n]
			// By default show only what someone deliberately set on this topic;
			// -all reveals the full surface including cluster defaults.
			if !f.showAll && e.IsDefault {
				hidden++
				continue
			}
			editable := "yes"
			if e.IsReadOnly {
				editable = "NO - fixed by the cluster"
				readOnly++
			}
			fmt.Printf("  %-40s %-26s %-22s %s\n", n, truncate(e.Value, 26), sourceName(e.Source), editable)
			shown++
		}
		fmt.Printf("\n  %d shown, %d read-only", shown, readOnly)
		if hidden > 0 {
			fmt.Printf(", %d left at the cluster default (re-run with -all to see them)", hidden)
		}
		fmt.Println()
	}
	return nil
}

func sourceName(s kafka.ConfigSource) string {
	switch s {
	case kafka.ConfigSourceDynamicTopic:
		return "set on this topic"
	case kafka.ConfigSourceDynamicBroker:
		return "set on the broker"
	case kafka.ConfigSourceDynamicDefaultBroker:
		return "broker cluster default"
	case kafka.ConfigSourceStaticBroker:
		return "broker startup config"
	case kafka.ConfigSourceDefault:
		return "Kafka built-in default"
	default:
		return s.String()
	}
}

// ---------------------------------------------------------------- alter

func alter(ctx context.Context, admin *kafka.AdminClient, c *config.Config, topic string, f flags) error {
	entries := topicEntries(c)
	if len(entries) == 0 {
		return errors.New("no topic config entries to apply")
	}

	// IncrementalAlterConfigs, not AlterConfigs. The older AlterConfigs is
	// destructive: any entry you omit is RESET to its default by the broker. That
	// has taken retention settings out behind the shed more than once. The
	// incremental form touches only the entries you name.
	ce := make([]kafka.ConfigEntry, 0, len(entries))
	fmt.Printf("\n=== altering topic %q ===\n", topic)
	for _, k := range sortedKeys(entries) {
		fmt.Printf("  set %-32s = %s\n", k, entries[k])
		ce = append(ce, kafka.ConfigEntry{
			Name:                 k,
			Value:                entries[k],
			IncrementalOperation: kafka.AlterConfigOpTypeSet,
		})
	}

	opts := []kafka.AlterConfigsAdminOption{kafka.SetAdminRequestTimeout(f.timeout)}
	if f.dryRun {
		opts = append(opts, kafka.SetAdminValidateOnly(true))
		fmt.Println("\n  -dry-run: the broker will validate and change nothing")
	}

	res, err := admin.IncrementalAlterConfigs(ctx,
		[]kafka.ConfigResource{{Type: kafka.ResourceTopic, Name: topic, Config: ce}}, opts...)
	if err != nil {
		// A managed cluster refusing a locked property lands here, naming it.
		return fmt.Errorf("IncrementalAlterConfigs: %w", err)
	}
	var failed error
	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			fmt.Printf("  FAIL %s: %v\n", r.Name, r.Error)
			failed = errors.Join(failed, fmt.Errorf("%s: %w", r.Name, r.Error))
			continue
		}
		fmt.Printf("  OK   %s updated\n", r.Name)
	}
	if failed != nil {
		return failed
	}
	if f.dryRun {
		return nil
	}
	return describe(ctx, admin, topic, f)
}

// ---------------------------------------------------------------- partitions

func addPartitions(ctx context.Context, admin *kafka.AdminClient, topic string, f flags) error {
	if f.partitions <= 0 {
		return errors.New("-to is required: the new total partition count, e.g. -cmd partitions -to 12")
	}
	fmt.Printf("\n=== increasing %q to %d partitions ===\n", topic, f.partitions)
	fmt.Println(`
  READ THIS BEFORE YOU RUN IT IN PRODUCTION

  Partition count can only ever go UP. There is no way back short of creating a
  new topic and copying the data.

  The default partitioner picks a partition as hash(key) % partition_count. Change
  the count and the same key starts landing somewhere new, so messages for that
  key now exist in TWO partitions with no ordering between them. Any consumer
  that assumed per-key ordering is now wrong, and the old messages do not move.

  Safe when: your keys are null, or ordering per key genuinely does not matter.
  Otherwise: create a new topic with the partition count you want, migrate, and
  cut over.`)

	res, err := admin.CreatePartitions(ctx,
		[]kafka.PartitionsSpecification{{Topic: topic, IncreaseTo: f.partitions}},
		kafka.SetAdminOperationTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("CreatePartitions: %w", err)
	}
	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("%s: %w", r.Topic, r.Error)
		}
		fmt.Printf("\n  OK   %s now has %d partitions\n", r.Topic, f.partitions)
	}
	// CreatePartitions returns as soon as the CONTROLLER accepts the change, but
	// every client caches its own metadata view and only refreshes it every
	// topic.metadata.refresh.interval.ms. Describing straight away would report
	// the OLD partition count and look like the change had silently failed.
	// This is not a quirk of this tool - your producers will not start using the
	// new partitions until their own metadata refreshes either.
	if err := waitForPartitions(admin, topic, f.partitions, 20*time.Second); err != nil {
		fmt.Printf("\n  NOTE: %v\n", err)
	}
	return describe(ctx, admin, topic, f)
}

// waitForPartitions polls metadata until it reflects the new partition count.
func waitForPartitions(admin *kafka.AdminClient, topic string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		md, err := admin.GetMetadata(&topic, false, 5000)
		if err == nil {
			if tm, ok := md.Topics[topic]; ok && len(tm.Partitions) >= want {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("metadata still reports fewer than %d partitions after %s; "+
				"the change was accepted, your client cache has not caught up yet", want, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- offsets

// offsets prints the low and high watermark of every partition. high-low is the
// number of messages currently retained, which is how you prove a producer wrote
// where you expected and watch retention actually delete things.
func offsets(ctx context.Context, admin *kafka.AdminClient, topic string, f flags) error {
	md, err := admin.GetMetadata(&topic, false, int(f.timeout/time.Millisecond))
	if err != nil {
		return fmt.Errorf("GetMetadata: %w", err)
	}
	tm, ok := md.Topics[topic]
	if !ok || tm.Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("topic %q not available", topic)
	}

	earliest := map[kafka.TopicPartition]kafka.OffsetSpec{}
	latest := map[kafka.TopicPartition]kafka.OffsetSpec{}
	for _, p := range tm.Partitions {
		tp := kafka.TopicPartition{Topic: &topic, Partition: p.ID}
		earliest[tp] = kafka.EarliestOffsetSpec
		latest[tp] = kafka.LatestOffsetSpec
	}

	lo, err := admin.ListOffsets(ctx, earliest, kafka.SetAdminRequestTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("ListOffsets(earliest): %w", err)
	}
	hi, err := admin.ListOffsets(ctx, latest, kafka.SetAdminRequestTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("ListOffsets(latest): %w", err)
	}

	fmt.Printf("\n=== offsets for %q ===\n\n", topic)
	fmt.Printf("  %-6s %-14s %-14s %s\n", "PART", "EARLIEST", "LATEST", "RETAINED")
	ids := make([]int32, 0, len(tm.Partitions))
	for _, p := range tm.Partitions {
		ids = append(ids, p.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Index the results by PARTITION NUMBER, not by TopicPartition.
	//
	// kafka.TopicPartition holds Topic as a *string, so using it as a map key
	// compares pointer identity, not the topic name. The library builds its
	// result map with its own string pointers, so looking up with a
	// TopicPartition we constructed here silently misses every time and reports
	// zero for everything.
	byPartition := func(r kafka.ListOffsetsResult) map[int32]kafka.ListOffsetsResultInfo {
		m := make(map[int32]kafka.ListOffsetsResultInfo, len(r.ResultInfos))
		for tp, info := range r.ResultInfos {
			m[tp.Partition] = info
		}
		return m
	}
	loByPart, hiByPart := byPartition(lo), byPartition(hi)

	var total int64
	for _, id := range ids {
		l, h := loByPart[id], hiByPart[id]
		if l.Error.Code() != kafka.ErrNoError {
			fmt.Printf("  %-6d %v\n", id, l.Error)
			continue
		}
		n := int64(h.Offset) - int64(l.Offset)
		total += n
		fmt.Printf("  %-6d %-14d %-14d %d\n", id, int64(l.Offset), int64(h.Offset), n)
	}
	fmt.Printf("\n  %d message(s) retained across %d partition(s)\n", total, len(ids))
	fmt.Println("\n  EARLIEST is not always 0: retention deletes from the head, so a topic")
	fmt.Println("  that has expired data starts partway in. LATEST is the offset the next")
	fmt.Println("  message will get, not the last one written.")
	return nil
}

// ---------------------------------------------------------------- list, delete

func list(admin *kafka.AdminClient, f flags) error {
	md, err := admin.GetMetadata(nil, true, int(f.timeout/time.Millisecond))
	if err != nil {
		return fmt.Errorf("GetMetadata: %w", err)
	}
	names := make([]string, 0, len(md.Topics))
	for n := range md.Topics {
		if strings.HasPrefix(n, "__") && !f.showAll {
			continue // internal topics such as __consumer_offsets
		}
		names = append(names, n)
	}
	sort.Strings(names)

	fmt.Printf("\n=== %d topic(s) on the cluster ===\n\n", len(names))
	fmt.Printf("  %-44s %s\n", "TOPIC", "PARTITIONS")
	for _, n := range names {
		fmt.Printf("  %-44s %d\n", n, len(md.Topics[n].Partitions))
	}
	if !f.showAll {
		fmt.Println("\n  internal topics hidden; re-run with -all to include them")
	}
	fmt.Printf("\n  brokers: %d\n", len(md.Brokers))
	return nil
}

func deleteTopic(ctx context.Context, admin *kafka.AdminClient, topic string, f flags) error {
	fmt.Printf("\n=== deleting topic %q ===\n", topic)
	res, err := admin.DeleteTopics(ctx, []string{topic}, kafka.SetAdminOperationTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("DeleteTopics: %w", err)
	}
	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("%s: %w", r.Topic, r.Error)
		}
		fmt.Printf("  OK   %s deleted\n", r.Topic)
	}
	fmt.Println("\n  Deletion is asynchronous. The topic may linger in metadata for a few")
	fmt.Println("  seconds, and recreating it immediately can fail with TOPIC_ALREADY_EXISTS.")
	return nil
}

// ---------------------------------------------------------------- helpers

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func ints(v []int32) string {
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = strconv.Itoa(int(n))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
