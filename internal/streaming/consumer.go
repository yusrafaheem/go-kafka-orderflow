package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps a *kafka.Reader configured as part of a consumer group.
// Membership, partition assignment, and rebalancing are all handled by the
// broker and the kafka-go client -- this type's only job is to run the
// fetch -> handle -> commit loop and decide what happens when a handler
// fails.
type Consumer struct {
	reader *kafka.Reader
	topic  string
}

// ConsumerConfig groups the parameters NewConsumer needs. GroupID is what
// makes this a consumer *group* member rather than a lone reader: every
// process sharing the same GroupID and Topic divides that topic's
// partitions among themselves, and the broker tracks one committed offset
// per (GroupID, Topic, Partition). That's exactly why cmd/consumer and
// cmd/notifier -- subscribed to different topics under different GroupIDs
// -- have completely independent progress through their respective logs,
// even though cmd/notifier is reading what cmd/consumer produced.
type ConsumerConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

// NewConsumer configures the reader with CommitInterval left at its zero
// value, so CommitMessages commits synchronously on every call instead of
// batching on a timer. That trades a little throughput for a simpler
// correctness story: once Run's handler has returned for a message, that
// message genuinely will not be redelivered on a clean shutdown, rather
// than "probably won't, depending on where the commit timer happened to
// land."
func NewConsumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{
		topic: cfg.Topic,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			GroupID:  cfg.GroupID,
			Topic:    cfg.Topic,
			MinBytes: 1,
			MaxBytes: 10e6, // 10MB
		}),
	}
}

// Handler processes one message. Returning a non-nil error tells Run the
// message failed processing (see Run's doc comment for what happens next);
// it does not stop the loop.
type Handler func(ctx context.Context, msg kafka.Message) error

// Run fetches messages one at a time and calls handler on each, committing
// the offset after every call regardless of whether the handler succeeded.
//
// That "commit anyway" choice is deliberate: retrying a failed handler
// in-place, forever, would stall this partition behind one bad message and
// eventually trigger a consumer-group rebalance from a missed heartbeat.
// Run instead trusts the caller's handler to have already routed genuine
// failures somewhere durable before returning its error -- cmd/consumer's
// handler publishes the offending message to a dead-letter topic first --
// so Run's only remaining job is guaranteeing every message is handled by
// *some* code path exactly once, not retrying indefinitely.
//
// Run returns nil when ctx is canceled (including via Close, which
// unblocks a pending FetchMessage with io.EOF) and a non-nil error only
// when a fetch or commit against the broker itself fails for a reason
// unrelated to shutdown.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if isShutdown(err) {
				return nil
			}
			return fmt.Errorf("streaming: fetch from %s failed: %w", c.topic, err)
		}

		if handleErr := handler(ctx, msg); handleErr != nil {
			slog.Error("streaming: handler failed, committing offset anyway",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", handleErr)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if isShutdown(err) {
				return nil
			}
			return fmt.Errorf("streaming: commit offset %d on %s/%d failed: %w", msg.Offset, msg.Topic, msg.Partition, err)
		}
	}
}

func isShutdown(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF)
}

// Close leaves the consumer group, releasing this member's partition
// assignment immediately rather than waiting for a session-timeout-
// triggered rebalance to notice it's gone.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
