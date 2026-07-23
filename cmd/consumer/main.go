// Command consumer runs the order-processing service: it reads
// OrderCreated events from the "orders" topic, prices each order, and
// publishes the outcome -- success or rejection -- to "orders.processed".
// Messages that can't even be unmarshaled are republished to a dead-letter
// topic instead of blocking their partition forever.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/yusrafaheem/go-kafka-orderflow/internal/config"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/order"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/streaming"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	producer := streaming.NewProducer(cfg.Brokers)
	consumer := streaming.NewConsumer(streaming.ConsumerConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupID,
		Topic:   cfg.TopicOrders,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler := newOrderHandler(producer, cfg.TopicOrdersProcessed, cfg.TopicOrdersDLQ, logger)

	logger.Info("consumer starting",
		"group_id", cfg.GroupID, "topic_in", cfg.TopicOrders,
		"topic_out", cfg.TopicOrdersProcessed, "topic_dlq", cfg.TopicOrdersDLQ,
		"brokers", cfg.Brokers)

	if err := consumer.Run(ctx, handler); err != nil {
		logger.Error("consumer loop exited with error", "error", err)
	}

	logger.Info("shutting down")
	if err := consumer.Close(); err != nil {
		logger.Error("error closing consumer", "error", err)
	}
	if err := producer.Close(); err != nil {
		logger.Error("error closing producer", "error", err)
	}
}

// newOrderHandler returns a streaming.Handler closing over the topics and
// producer it needs. Keeping the decode/process/publish logic in a
// standalone function -- rather than inline in main -- is what would let
// it be unit tested against a fake Publisher the same way
// internal/httpapi's handler is; this repo doesn't add that test file, but
// the seam exists on purpose for the same reason.
func newOrderHandler(pub streaming.Publisher, topicOut, topicDLQ string, logger *slog.Logger) streaming.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		var o order.Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			return deadLetter(ctx, pub, topicDLQ, msg, "unmarshal failed: "+err.Error())
		}

		if err := order.Validate(o); err != nil {
			rejected := order.Reject(o, err.Error(), time.Now().UTC())
			return publishProcessed(ctx, pub, topicOut, rejected)
		}

		processed := order.Process(o, time.Now().UTC())
		if err := publishProcessed(ctx, pub, topicOut, processed); err != nil {
			// Best effort: if the broker is unreachable enough that
			// publishing the *processed* result failed, the DLQ publish
			// attempted here will likely fail too. Run's "commit anyway"
			// policy means we won't loop forever on this message either
			// way -- deadLetter's returned error just ends up logged.
			return deadLetter(ctx, pub, topicDLQ, msg, "publish of processed order failed: "+err.Error())
		}

		logger.Info("order processed",
			"order_id", processed.OrderID, "customer_id", processed.CustomerID,
			"status", processed.Status, "total_cents", processed.TotalCents)
		return nil
	}
}

func publishProcessed(ctx context.Context, pub streaming.Publisher, topic string, p order.ProcessedOrder) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return pub.Publish(ctx, topic, p.CustomerID, payload)
}

// deadLetterEnvelope is the JSON shape written to the dead-letter topic --
// the original bytes plus enough context to triage without replaying the
// original topic from scratch.
type deadLetterEnvelope struct {
	Reason         string `json:"reason"`
	OriginalTopic  string `json:"original_topic"`
	OriginalOffset int64  `json:"original_offset"`
	OriginalValue  string `json:"original_value"`
}

// deadLetter republishes an unprocessable message to the dead-letter
// topic, preserving the original Kafka key so a human triaging the DLQ
// later can still tell which customer/partition it came from.
func deadLetter(ctx context.Context, pub streaming.Publisher, topic string, msg kafka.Message, reason string) error {
	payload, err := json.Marshal(deadLetterEnvelope{
		Reason:         reason,
		OriginalTopic:  msg.Topic,
		OriginalOffset: msg.Offset,
		OriginalValue:  string(msg.Value),
	})
	if err != nil {
		return err
	}
	return pub.Publish(ctx, topic, string(msg.Key), payload)
}
