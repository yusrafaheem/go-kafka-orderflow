// Package config centralizes the environment-variable-driven settings
// shared by all three binaries in this repo. Not every field is used by
// every binary -- cmd/producer never reads GroupID, for instance -- but one
// small struct with a couple of unused fields per caller is simpler than
// three near-identical config types.
package config

import (
	"os"
	"strings"
)

// Config holds every setting read from the environment.
type Config struct {
	// Brokers is the comma-separated KAFKA_BROKERS list, already split.
	Brokers []string
	// HTTPAddr is where cmd/producer's HTTP server listens.
	HTTPAddr string
	// GroupID is the Kafka consumer group ID for whichever binary reads
	// it -- cmd/consumer and cmd/notifier each set their own via
	// KAFKA_GROUP_ID so they land in independent groups by default.
	GroupID string
	// TopicOrders is the topic cmd/producer publishes OrderCreated events
	// to, and cmd/consumer reads from.
	TopicOrders string
	// TopicOrdersProcessed is the topic cmd/consumer publishes
	// ProcessedOrder events to, and cmd/notifier reads from.
	TopicOrdersProcessed string
	// TopicOrdersDLQ is where cmd/consumer republishes messages it could
	// not process, instead of blocking the partition on them forever.
	TopicOrdersDLQ string
}

// Load reads every setting from the environment, falling back to values
// that make `make up && make demo` work end to end with zero extra setup.
func Load() Config {
	return Config{
		Brokers:               splitCSV(getenv("KAFKA_BROKERS", "localhost:9092")),
		HTTPAddr:              getenv("HTTP_ADDR", ":8080"),
		GroupID:               getenv("KAFKA_GROUP_ID", "orderflow"),
		TopicOrders:           getenv("KAFKA_TOPIC_ORDERS", "orders"),
		TopicOrdersProcessed:  getenv("KAFKA_TOPIC_ORDERS_PROCESSED", "orders.processed"),
		TopicOrdersDLQ:        getenv("KAFKA_TOPIC_ORDERS_DLQ", "orders.dlq"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitCSV splits a comma-separated list and trims whitespace around each
// element, dropping any that end up empty (so a trailing comma, or
// "a, ,b", doesn't produce a bogus broker address).
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
