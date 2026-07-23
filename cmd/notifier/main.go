// Command notifier is a second, independent consumer of
// "orders.processed" -- it exists to demonstrate Kafka's pub/sub fan-out:
// same topic, a different consumer group ID, and therefore a completely
// independent set of committed offsets from cmd/consumer's, even though
// both processes are ultimately reacting to the same underlying orders.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/yusrafaheem/go-kafka-orderflow/internal/config"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/order"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/streaming"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	// config.Load's default GroupID is "orderflow", same default
	// cmd/consumer uses. That's fine for cmd/consumer (it reads a
	// different topic), but two processes sharing one GroupID on the
	// *same* topic would split its partitions between them instead of
	// each seeing every message. NOTIFIER_GROUP_ID lets this binary opt
	// into its own group without requiring an operator to override
	// KAFKA_GROUP_ID globally just for this one process.
	groupID := getenvDefault("NOTIFIER_GROUP_ID", "orderflow-notifier")

	consumer := streaming.NewConsumer(streaming.ConsumerConfig{
		Brokers: cfg.Brokers,
		GroupID: groupID,
		Topic:   cfg.TopicOrdersProcessed,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("notifier starting", "group_id", groupID, "topic", cfg.TopicOrdersProcessed, "brokers", cfg.Brokers)

	handler := func(_ context.Context, msg kafka.Message) error {
		var p order.ProcessedOrder
		if err := json.Unmarshal(msg.Value, &p); err != nil {
			logger.Error("notifier: failed to decode processed order", "error", err)
			return err
		}
		fmt.Println(describe(p))
		return nil
	}

	if err := consumer.Run(ctx, handler); err != nil {
		logger.Error("notifier loop exited with error", "error", err)
	}

	logger.Info("shutting down")
	if err := consumer.Close(); err != nil {
		logger.Error("error closing consumer", "error", err)
	}
}

func describe(p order.ProcessedOrder) string {
	if p.Status == order.StatusRejected {
		return fmt.Sprintf("[REJECTED]  order %s for customer %s: %s", p.OrderID, p.CustomerID, p.Reason)
	}
	return fmt.Sprintf("[PROCESSED] order %s for customer %s: subtotal=$%.2f discount=$%.2f total=$%.2f",
		p.OrderID, p.CustomerID,
		centsToDollars(p.SubtotalCents), centsToDollars(p.DiscountCents), centsToDollars(p.TotalCents))
}

func centsToDollars(cents int64) float64 {
	return float64(cents) / 100
}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
