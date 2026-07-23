package config

import (
	"reflect"
	"testing"
)

// TestLoadDefaults checks Load()'s fallback values -- the ones that make
// `make up && make demo` work end to end with no environment variables set
// at all. Setting each var to the empty string via t.Setenv is equivalent
// to it being unset for getenv's purposes (it only treats a non-empty
// value as "set"), and t.Setenv automatically restores the previous value
// once the test finishes, so this can't leak into other tests.
func TestLoadDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("KAFKA_GROUP_ID", "")
	t.Setenv("KAFKA_TOPIC_ORDERS", "")
	t.Setenv("KAFKA_TOPIC_ORDERS_PROCESSED", "")
	t.Setenv("KAFKA_TOPIC_ORDERS_DLQ", "")

	c := Load()

	wantBrokers := []string{"localhost:9092"}
	if !reflect.DeepEqual(c.Brokers, wantBrokers) {
		t.Errorf("Brokers = %v, want %v", c.Brokers, wantBrokers)
	}
	if c.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", c.HTTPAddr, ":8080")
	}
	if c.GroupID != "orderflow" {
		t.Errorf("GroupID = %q, want %q", c.GroupID, "orderflow")
	}
	if c.TopicOrders != "orders" {
		t.Errorf("TopicOrders = %q, want %q", c.TopicOrders, "orders")
	}
	if c.TopicOrdersProcessed != "orders.processed" {
		t.Errorf("TopicOrdersProcessed = %q, want %q", c.TopicOrdersProcessed, "orders.processed")
	}
	if c.TopicOrdersDLQ != "orders.dlq" {
		t.Errorf("TopicOrdersDLQ = %q, want %q", c.TopicOrdersDLQ, "orders.dlq")
	}
}

// TestLoadEnvOverrides checks that every field actually reads from its
// documented environment variable, not just that defaults exist.
func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-a:9092, broker-b:9092")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("KAFKA_GROUP_ID", "custom-group")
	t.Setenv("KAFKA_TOPIC_ORDERS", "custom.orders")
	t.Setenv("KAFKA_TOPIC_ORDERS_PROCESSED", "custom.orders.processed")
	t.Setenv("KAFKA_TOPIC_ORDERS_DLQ", "custom.orders.dlq")

	c := Load()

	wantBrokers := []string{"broker-a:9092", "broker-b:9092"}
	if !reflect.DeepEqual(c.Brokers, wantBrokers) {
		t.Errorf("Brokers = %v, want %v (and should be split/trimmed)", c.Brokers, wantBrokers)
	}
	if c.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q", c.HTTPAddr, ":9090")
	}
	if c.GroupID != "custom-group" {
		t.Errorf("GroupID = %q, want %q", c.GroupID, "custom-group")
	}
	if c.TopicOrders != "custom.orders" {
		t.Errorf("TopicOrders = %q, want %q", c.TopicOrders, "custom.orders")
	}
	if c.TopicOrdersProcessed != "custom.orders.processed" {
		t.Errorf("TopicOrdersProcessed = %q, want %q", c.TopicOrdersProcessed, "custom.orders.processed")
	}
	if c.TopicOrdersDLQ != "custom.orders.dlq" {
		t.Errorf("TopicOrdersDLQ = %q, want %q", c.TopicOrdersDLQ, "custom.orders.dlq")
	}
}

// TestSplitCSV exercises the comma-parsing edge cases called out in its
// doc comment: trailing commas, blank elements, and whitespace padding
// around a broker address -- all things a human typing KAFKA_BROKERS by
// hand is likely to produce by accident.
func TestSplitCSV(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{name: "single value", in: "localhost:9092", want: []string{"localhost:9092"}},
		{name: "multiple values", in: "a:9092,b:9092,c:9092", want: []string{"a:9092", "b:9092", "c:9092"}},
		{name: "trims whitespace around each element", in: " a:9092 , b:9092 ", want: []string{"a:9092", "b:9092"}},
		{name: "drops empty element from a trailing comma", in: "a:9092,", want: []string{"a:9092"}},
		{name: "drops empty elements in the middle", in: "a:9092, ,b:9092", want: []string{"a:9092", "b:9092"}},
		{name: "empty string yields empty slice", in: "", want: []string{}},
		{name: "only whitespace and commas yields empty slice", in: " , , ", want: []string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitCSV(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitCSV(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestGetenv checks the "empty string counts as unset" behavior that
// TestLoadDefaults relies on above.
func TestGetenv(t *testing.T) {
	t.Setenv("ORDERFLOW_TEST_VAR", "")
	if got := getenv("ORDERFLOW_TEST_VAR", "fallback"); got != "fallback" {
		t.Errorf("getenv() with unset var = %q, want fallback %q", got, "fallback")
	}

	t.Setenv("ORDERFLOW_TEST_VAR", "actual-value")
	if got := getenv("ORDERFLOW_TEST_VAR", "fallback"); got != "actual-value" {
		t.Errorf("getenv() with set var = %q, want %q", got, "actual-value")
	}
}
