package order

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrEmptyCustomerID = errors.New("order: customer_id is required")
	ErrNoItems         = errors.New("order: at least one item is required")
)

// bulkDiscountThresholdCents is the subtotal at or above which an order
// qualifies for the bulk discount. Expressed in cents to avoid floating
// point entirely -- every money value in this package is an int64 count
// of cents, right up to the JSON boundary.
const bulkDiscountThresholdCents int64 = 10_000 // $100.00

// bulkDiscountRateNumerator / Denominator express the discount rate as an
// exact integer fraction (10%) rather than a float, so DiscountCents is
// always an exact integer number of cents with no rounding ambiguity.
const (
	bulkDiscountRateNumerator   = 1
	bulkDiscountRateDenominator = 10
)

// Validate checks the fields an Order must have before it's safe to price.
// It deliberately does not check anything that requires I/O (e.g. "does
// this SKU exist," "is this customer real") -- those are the consumer's
// job, and conflating them here would mean tests need a live catalog
// service to exercise pure validation logic.
func Validate(o Order) error {
	if o.CustomerID == "" {
		return ErrEmptyCustomerID
	}
	if len(o.Items) == 0 {
		return ErrNoItems
	}
	for i, item := range o.Items {
		if item.SKU == "" {
			return fmt.Errorf("order: item %d: sku is required", i)
		}
		if item.Quantity <= 0 {
			return fmt.Errorf("order: item %d (%s): quantity must be positive, got %d", i, item.SKU, item.Quantity)
		}
		if item.UnitPriceCents < 0 {
			return fmt.Errorf("order: item %d (%s): unit_price_cents cannot be negative, got %d", i, item.SKU, item.UnitPriceCents)
		}
	}
	return nil
}

// Process prices a validated Order into a ProcessedOrder. Callers must run
// Validate first -- Process assumes its input is already well-formed and
// will happily compute a (meaningless) total for a malformed order rather
// than re-checking everything itself.
func Process(o Order, now time.Time) ProcessedOrder {
	var subtotal int64
	for _, item := range o.Items {
		subtotal += int64(item.Quantity) * item.UnitPriceCents
	}

	var discount int64
	if subtotal >= bulkDiscountThresholdCents {
		discount = subtotal * bulkDiscountRateNumerator / bulkDiscountRateDenominator
	}

	return ProcessedOrder{
		OrderID:       o.OrderID,
		CustomerID:    o.CustomerID,
		SubtotalCents: subtotal,
		DiscountCents: discount,
		TotalCents:    subtotal - discount,
		Status:        StatusProcessed,
		ProcessedAt:   now,
	}
}

// Reject builds the ProcessedOrder emitted when an order fails validation
// (or any other processing precondition). Rejections are published to the
// same "orders.processed" topic as successes, with Status set accordingly,
// rather than silently dropped -- a consumer of that topic should be able
// to account for every OrderID it ever sees on "orders", not just the ones
// that happened to succeed.
func Reject(o Order, reason string, now time.Time) ProcessedOrder {
	return ProcessedOrder{
		OrderID:     o.OrderID,
		CustomerID:  o.CustomerID,
		Status:      StatusRejected,
		Reason:      reason,
		ProcessedAt: now,
	}
}
