// Package order holds the domain model and pure business logic for the
// order-processing pipeline. Nothing in this package talks to Kafka, HTTP,
// or any other I/O boundary -- that separation is what lets Validate and
// Process be unit-tested without a live broker (see service_test.go).
package order

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Item is a single line item within an order.
type Item struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Order is the event published to the "orders" topic when a customer
// checks out. It is intentionally denormalized (no external lookups
// required to process it) so a consumer can process it using only the
// bytes in the Kafka message.
type Order struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Items      []Item    `json:"items"`
	CreatedAt  time.Time `json:"created_at"`
}

// Status describes the outcome of processing an Order.
type Status string

const (
	StatusProcessed Status = "processed"
	StatusRejected  Status = "rejected"
)

// ProcessedOrder is the event published to the "orders.processed" topic
// once a consumer has validated and priced an Order. Rejected orders are
// still published here with Status == StatusRejected and Reason set --
// downstream consumers (like the notifier) see every outcome, not just
// the successful ones.
type ProcessedOrder struct {
	OrderID       string    `json:"order_id"`
	CustomerID    string    `json:"customer_id"`
	SubtotalCents int64     `json:"subtotal_cents"`
	DiscountCents int64     `json:"discount_cents"`
	TotalCents    int64     `json:"total_cents"`
	Status        Status    `json:"status"`
	Reason        string    `json:"reason,omitempty"`
	ProcessedAt   time.Time `json:"processed_at"`
}

// NewID returns a random 16-hex-character identifier. Order IDs don't need
// to be globally unique across all of time the way a UUID does -- they only
// need to be unique enough to dedupe within this pipeline's retention
// window, so a 8-byte random value (64 bits of entropy) is a deliberate,
// dependency-free substitute for pulling in a UUID library for one call
// site.
func NewID() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
