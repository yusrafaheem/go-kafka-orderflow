// Package streaming wraps segmentio/kafka-go behind two narrow interfaces
// (Publisher and the consume loop in consumer.go). The rest of the codebase
// -- internal/httpapi and the business logic each cmd/ binary wires
// together -- depends only on those interfaces, never on *kafka.Writer or
// *kafka.Reader directly. That's what lets internal/httpapi's tests run
// against an in-memory fake instead of a live broker.
//
// The package is named "streaming" rather than "kafka" so that files in
// this package can still `import "github.com/segmentio/kafka-go"` as
// `kafka` without a name collision against the package's own identifier.
package streaming

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher is the interface the rest of the codebase depends on instead of
// a concrete Kafka writer.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
	Close() error
}

// Producer wraps a *kafka.Writer configured for durability over raw
// throughput: RequireAll waits for every in-sync replica to acknowledge a
// write before WriteMessages returns. Topic is left unset on the
// underlying writer so each Publish call can target a different topic --
// cmd/consumer uses one Producer to publish to both "orders.processed" and
// the dead-letter topic.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer never dials a socket itself: kafka-go's Writer connects
// lazily on the first WriteMessages call, so constructing a Producer never
// blocks or fails just because the brokers aren't reachable yet.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Hash{}, // same key -> same partition, every time
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			WriteTimeout:           10 * time.Second,
		},
	}
}

// maxPublishAttempts bounds the retry loop in Publish. Kafka brokers reject
// writes for plenty of transient, retry-worthy reasons (a leader election
// in progress, a broker mid-restart) that have nothing to do with the
// message itself, so a single failed WriteMessages call is not treated as
// final -- but it also isn't retried forever, since an unbounded retry loop
// on a message that's genuinely too large or malformed would just wedge
// the caller.
const maxPublishAttempts = 3

// Publish writes a single message, retrying transient failures with linear
// backoff (200ms, 400ms) before giving up.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	var lastErr error
	for attempt := 1; attempt <= maxPublishAttempts; attempt++ {
		lastErr = p.writer.WriteMessages(ctx, msg)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("streaming: publish to %s canceled: %w", topic, ctx.Err())
		}
		if attempt < maxPublishAttempts {
			backoff := time.Duration(attempt*200) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return fmt.Errorf("streaming: publish to %s canceled: %w", topic, ctx.Err())
			}
		}
	}
	return fmt.Errorf("streaming: publish to %s failed after %d attempts: %w", topic, maxPublishAttempts, lastErr)
}

// Close flushes and releases the underlying connection pool. It should be
// called exactly once, during graceful shutdown.
func (p *Producer) Close() error {
	return p.writer.Close()
}
