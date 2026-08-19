// Package config loads Java-style .properties files.
//
// Why .properties and not YAML or JSON?
//
// Every key in these files that does NOT start with "app." is a REAL Kafka or
// librdkafka configuration property name, spelled exactly as Confluent's own
// documentation spells it. It is handed to the client library verbatim. That
// means anything you learn here transfers directly to kafka-console-producer,
// to the Java client, to Kafka Connect, and to the Confluent Cloud UI. There is
// no translation layer to memorise.
//
// Keys that DO start with "app." are consumed by this training program itself
// (how many messages to send, which delivery mode to demonstrate, and so on).
// They are never passed to Kafka. The prefix is the whole rule.
package config

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AppPrefix marks a key as belonging to this program rather than to Kafka.
const AppPrefix = "app."

// Config is an ordered set of key/value pairs assembled from one or more
// .properties files plus command-line overrides. Order is preserved so that
// Dump reproduces the layering a reader can reason about.
type Config struct {
	order []string          // keys, in first-seen order
	kv    map[string]string // key -> final value
	src   map[string]string // key -> where the winning value came from
}

// envRef matches ${NAME} for environment expansion. A value of ${KAFKA_API_KEY}
// keeps the real secret out of the file, which is the only safe way to commit a
// config that talks to Confluent Cloud.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// New returns an empty Config.
func New() *Config {
	return &Config{kv: map[string]string{}, src: map[string]string{}}
}

// Load reads each path in turn. Later files override earlier ones, so the usual
// call is Load("config/cluster.properties", "config/producer.properties") —
// shared connection settings first, role-specific settings second.
func Load(paths ...string) (*Config, error) {
	c := New()
	for _, p := range paths {
		if err := c.LoadFile(p); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// LoadList loads a comma-separated list of paths, in order, so a command-line
// flag can layer an overlay on top of a base file:
//
//	-cluster config/cluster.properties,config/local.properties
func LoadList(spec string) (*Config, error) {
	var paths []string
	for _, p := range strings.Split(spec, ",") {
		if p = strings.TrimSpace(p); p != "" {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("config: no file given")
	}
	return Load(paths...)
}

// LoadFile merges one .properties file into c.
func (c *Config) LoadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())

		// Blank lines and comments. Both '#' and '!' start a comment, which is
		// what the Java .properties format specifies; every alternative value
		// documented in these files lives in such a comment.
		if raw == "" || strings.HasPrefix(raw, "#") || strings.HasPrefix(raw, "!") {
			continue
		}

		k, v, ok := strings.Cut(raw, "=")
		if !ok {
			return fmt.Errorf("config: %s:%d: no '=' in %q", path, line, raw)
		}
		key := strings.TrimSpace(k)
		if key == "" {
			return fmt.Errorf("config: %s:%d: empty key", path, line)
		}
		val := strings.TrimSpace(v)

		c.set(key, expandEnv(val), fmt.Sprintf("%s:%d", path, line))
	}
	return sc.Err()
}

// Apply layers "key=value" overrides on top of everything loaded so far. This is
// what -set on the command line feeds, and it is how the lab asks you to change
// one property at a time without editing a file.
func (c *Config) Apply(overrides []string) error {
	for _, o := range overrides {
		k, v, ok := strings.Cut(o, "=")
		if !ok {
			return fmt.Errorf("config: override %q is not key=value", o)
		}
		key := strings.TrimSpace(k)
		if key == "" {
			return fmt.Errorf("config: override %q has an empty key", o)
		}
		c.set(key, expandEnv(strings.TrimSpace(v)), "-set")
	}
	return nil
}

func (c *Config) set(key, val, src string) {
	if _, seen := c.kv[key]; !seen {
		c.order = append(c.order, key)
	}
	c.kv[key] = val
	c.src[key] = src
}

// expandEnv replaces every ${NAME} that IS set in the environment, and leaves
// any unset reference in place as a literal.
//
// Leaving it rather than failing here is deliberate: an overlay loaded later may
// replace the whole value. config/local.properties overrides sasl.password to
// empty for a local PLAINTEXT broker, so failing at parse time on an unset
// KAFKA_API_SECRET would make the offline path impossible to use. Resolve
// reports whatever is still unresolved once every layer has been applied.
func expandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		if v, ok := os.LookupEnv(m[2 : len(m)-1]); ok && v != "" {
			return v
		}
		return m
	})
}

// Resolve reports any ${NAME} still unexpanded after all files and overrides
// have been layered. Call it once, immediately before handing the config to a
// client: a named missing variable beats an opaque SASL authentication timeout
// ten minutes later.
func (c *Config) Resolve() error {
	var problems []string
	for _, k := range c.order {
		for _, m := range envRef.FindAllStringSubmatch(c.kv[k], -1) {
			problems = append(problems, fmt.Sprintf(
				"  %s (needed by %q at %s)", m[1], k, c.src[k]))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("these environment variables are not set:\n%s\n\nexport them, or layer an overlay that replaces those keys:\n  -cluster config/cluster.properties,config/local.properties",
		strings.Join(problems, "\n"))
}

// Kafka returns every property that is NOT app-scoped: the set handed straight
// to the client library. Keys keep their exact Kafka spelling.
func (c *Config) Kafka() map[string]string {
	out := make(map[string]string, len(c.kv))
	for _, k := range c.order {
		if !strings.HasPrefix(k, AppPrefix) {
			out[k] = c.kv[k]
		}
	}
	return out
}

// Has reports whether a key was set at all, which lets a caller distinguish
// "explicitly configured" from "left at the library default".
func (c *Config) Has(key string) bool { _, ok := c.kv[key]; return ok }

// Get returns a raw value, app-scoped or not.
func (c *Config) Get(key string) string { return c.kv[key] }

// Source reports where the winning value for key came from, as "file:line" or
// "-set". Printed by Dump so a student can always answer "why is it that?".
func (c *Config) Source(key string) string { return c.src[key] }

// App reads an "app."-scoped key, returning def when it is absent or empty.
func (c *Config) App(key, def string) string {
	if v, ok := c.kv[AppPrefix+key]; ok && v != "" {
		return v
	}
	return def
}

// AppInt reads an app-scoped integer.
func (c *Config) AppInt(key string, def int) (int, error) {
	raw := c.App(key, "")
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s%s: %q is not an integer", AppPrefix, key, raw)
	}
	return n, nil
}

// AppBool reads an app-scoped boolean.
func (c *Config) AppBool(key string, def bool) (bool, error) {
	raw := c.App(key, "")
	if raw == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("config: %s%s: %q is not a boolean", AppPrefix, key, raw)
	}
	return b, nil
}

// AppDuration reads an app-scoped Go duration such as "250ms" or "5s".
func (c *Config) AppDuration(key string, def time.Duration) (time.Duration, error) {
	raw := c.App(key, "")
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s%s: %q is not a duration (try 250ms, 5s)", AppPrefix, key, raw)
	}
	return d, nil
}

// Keys returns every key in first-seen order.
func (c *Config) Keys() []string {
	out := make([]string, len(c.order))
	copy(out, c.order)
	return out
}

// secretish reports whether a value must never be printed in full. On Confluent
// Cloud sasl.username is the API key and sasl.password is its secret, so both
// are treated as credentials.
func secretish(key string) bool {
	k := strings.ToLower(key)
	return strings.Contains(k, "password") ||
		strings.Contains(k, "secret") ||
		k == "sasl.username"
}

// Redact masks a credential, keeping a short prefix so you can still tell two
// keys apart in a log without leaking either.
func Redact(key, val string) string {
	if !secretish(key) || val == "" {
		return val
	}
	if len(val) <= 4 {
		return "****"
	}
	return val[:4] + strings.Repeat("*", 8)
}

// Dump writes the effective configuration, sorted, with credentials redacted and
// the origin of each value shown. Every program in this package prints this at
// startup: knowing exactly what the client was configured with is the difference
// between debugging Kafka and guessing at it.
func Dump(c *Config, header string) string {
	keys := c.Keys()
	sort.Strings(keys)

	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", header)
	width := 0
	for _, k := range keys {
		if len(k) > width {
			width = len(k)
		}
	}
	for _, k := range keys {
		fmt.Fprintf(&b, "  %-*s = %-28s  (%s)\n", width, k, Redact(k, c.kv[k]), c.src[k])
	}
	return b.String()
}
