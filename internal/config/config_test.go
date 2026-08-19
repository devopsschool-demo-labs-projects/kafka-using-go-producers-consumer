package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestParseCommentsAndBlanks(t *testing.T) {
	p := write(t, "a.properties", `
# a hash comment
! a bang comment

acks=all
linger.ms = 5
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Get("acks"); got != "all" {
		t.Errorf("acks = %q, want all", got)
	}
	if got := c.Get("linger.ms"); got != "5" {
		t.Errorf("linger.ms = %q, want 5 (whitespace must be trimmed)", got)
	}
}

func TestValueMayContainEquals(t *testing.T) {
	// sasl.oauthbearer.config and SASL passwords routinely contain '='.
	p := write(t, "a.properties", "sasl.password=abc=def==\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Get("sasl.password"); got != "abc=def==" {
		t.Errorf("got %q, want abc=def==", got)
	}
}

func TestLaterFileOverridesEarlier(t *testing.T) {
	a := write(t, "a.properties", "acks=1\nclient.id=base\n")
	b := write(t, "b.properties", "acks=all\n")
	c, err := Load(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Get("acks"); got != "all" {
		t.Errorf("acks = %q, want all", got)
	}
	if got := c.Get("client.id"); got != "base" {
		t.Errorf("client.id = %q, want base", got)
	}
	if src := c.Source("acks"); !strings.Contains(src, "b.properties") {
		t.Errorf("source = %q, want it to name b.properties", src)
	}
}

func TestApplyOverrides(t *testing.T) {
	p := write(t, "a.properties", "acks=all\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply([]string{"acks=0", "compression.type=lz4"}); err != nil {
		t.Fatal(err)
	}
	if got := c.Get("acks"); got != "0" {
		t.Errorf("acks = %q, want 0", got)
	}
	if got := c.Source("acks"); got != "-set" {
		t.Errorf("source = %q, want -set", got)
	}
	if got := c.Get("compression.type"); got != "lz4" {
		t.Errorf("compression.type = %q, want lz4", got)
	}
}

func TestEnvExpansionAndMissingVar(t *testing.T) {
	t.Setenv("TEST_KAFKA_KEY", "AKIA1234")
	p := write(t, "a.properties", "sasl.username=${TEST_KAFKA_KEY}\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Get("sasl.username"); got != "AKIA1234" {
		t.Errorf("got %q, want AKIA1234", got)
	}

	// An unset variable is NOT an error at parse time, because a later overlay
	// may replace the whole value. Resolve is what reports it.
	q := write(t, "b.properties", "sasl.password=${TEST_KAFKA_DEFINITELY_UNSET}\n")
	c2, err := Load(q)
	if err != nil {
		t.Fatalf("parsing must not fail on an unset variable: %v", err)
	}
	err = c2.Resolve()
	if err == nil {
		t.Fatal("want Resolve to report the unset variable, got nil")
	}
	if !strings.Contains(err.Error(), "TEST_KAFKA_DEFINITELY_UNSET") {
		t.Errorf("error %q should name the missing variable", err)
	}
	if !strings.Contains(err.Error(), "sasl.password") {
		t.Errorf("error %q should name the key that needs it", err)
	}
}

func TestOverlayReplacesAnUnresolvedValue(t *testing.T) {
	// The offline path: cluster.properties wants Confluent Cloud credentials,
	// local.properties blanks them out for a PLAINTEXT broker. Loading both must
	// succeed with no cloud environment variables set at all.
	base := write(t, "cluster.properties", "bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}\nsasl.password=${KAFKA_API_SECRET}\n")
	overlay := write(t, "local.properties", "bootstrap.servers=localhost:9092\nsasl.password=\n")
	c, err := Load(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Resolve(); err != nil {
		t.Fatalf("overlay should have satisfied every reference: %v", err)
	}
	if got := c.Get("bootstrap.servers"); got != "localhost:9092" {
		t.Errorf("bootstrap.servers = %q, want localhost:9092", got)
	}
}

func TestAppKeysAreNotPassedToKafka(t *testing.T) {
	p := write(t, "a.properties", "acks=all\napp.message.count=100\napp.mode=transactional\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	k := c.Kafka()
	if _, bad := k["app.message.count"]; bad {
		t.Error("app-scoped keys must never reach the Kafka client")
	}
	if _, ok := k["acks"]; !ok {
		t.Error("acks must reach the Kafka client")
	}
	n, err := c.AppInt("message.count", 1)
	if err != nil || n != 100 {
		t.Errorf("AppInt = %d, %v; want 100, nil", n, err)
	}
	if got := c.App("mode", "sync"); got != "transactional" {
		t.Errorf("App(mode) = %q, want transactional", got)
	}
	if got := c.App("absent", "fallback"); got != "fallback" {
		t.Errorf("App(absent) = %q, want fallback", got)
	}
}

func TestAppTypedAccessors(t *testing.T) {
	p := write(t, "a.properties", "app.flush.timeout=15s\napp.verbose=true\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.AppDuration("flush.timeout", time.Second)
	if err != nil || d != 15*time.Second {
		t.Errorf("AppDuration = %v, %v; want 15s, nil", d, err)
	}
	b, err := c.AppBool("verbose", false)
	if err != nil || !b {
		t.Errorf("AppBool = %v, %v; want true, nil", b, err)
	}
	if _, err := c.AppInt("verbose", 0); err == nil {
		t.Error("want a typed error for a non-integer, got nil")
	}
}

func TestRedactionHidesCredentials(t *testing.T) {
	cases := []struct{ key, val, want string }{
		{"sasl.password", "supersecretvalue", "supe********"},
		{"sasl.username", "AKIAEXAMPLE", "AKIA********"},
		{"ssl.key.password", "abc", "****"},
		{"acks", "all", "all"},
		{"bootstrap.servers", "pkc-1.confluent.cloud:9092", "pkc-1.confluent.cloud:9092"},
	}
	for _, tc := range cases {
		if got := Redact(tc.key, tc.val); got != tc.want {
			t.Errorf("Redact(%q, %q) = %q, want %q", tc.key, tc.val, got, tc.want)
		}
	}
}

func TestDumpRedactsAndCitesSource(t *testing.T) {
	t.Setenv("TEST_SECRET", "hunter2hunter2")
	p := write(t, "a.properties", "acks=all\nsasl.password=${TEST_SECRET}\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	out := Dump(c, "effective:")
	if strings.Contains(out, "hunter2hunter2") {
		t.Fatal("Dump leaked a credential")
	}
	if !strings.Contains(out, "a.properties") {
		t.Error("Dump should cite the source file of each value")
	}
}

func TestMalformedLineIsReportedWithLineNumber(t *testing.T) {
	p := write(t, "a.properties", "acks=all\nthis line has no equals sign\n")
	_, err := Load(p)
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	if !strings.Contains(err.Error(), ":2:") {
		t.Errorf("error %q should carry the line number", err)
	}
}
