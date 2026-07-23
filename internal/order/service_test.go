package order

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		order   Order
		wantErr error // nil means "any non-nil error is fine, just check it's set"
		wantOK  bool
	}{
		{
			name: "valid single item order",
			order: Order{
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: 500}},
			},
			wantOK: true,
		},
		{
			name: "valid multi item order",
			order: Order{
				CustomerID: "cust-1",
				Items: []Item{
					{SKU: "sku-1", Quantity: 2, UnitPriceCents: 500},
					{SKU: "sku-2", Quantity: 1, UnitPriceCents: 1200},
				},
			},
			wantOK: true,
		},
		{
			name:    "missing customer id",
			order:   Order{Items: []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: 500}}},
			wantErr: ErrEmptyCustomerID,
		},
		{
			name:    "no items",
			order:   Order{CustomerID: "cust-1"},
			wantErr: ErrNoItems,
		},
		{
			name: "zero quantity",
			order: Order{
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 0, UnitPriceCents: 500}},
			},
		},
		{
			name: "negative quantity",
			order: Order{
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: -1, UnitPriceCents: 500}},
			},
		},
		{
			name: "negative price",
			order: Order{
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: -1}},
			},
		},
		{
			name: "missing sku",
			order: Order{
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "", Quantity: 1, UnitPriceCents: 500}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.order)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestProcess(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name         string
		order        Order
		wantSubtotal int64
		wantDiscount int64
		wantTotal    int64
	}{
		{
			name: "below discount threshold: no discount",
			order: Order{
				OrderID:    "order-1",
				CustomerID: "cust-1",
				Items: []Item{
					{SKU: "sku-1", Quantity: 2, UnitPriceCents: 500}, // 1000
					{SKU: "sku-2", Quantity: 1, UnitPriceCents: 250}, // 250
				},
			},
			wantSubtotal: 1250,
			wantDiscount: 0,
			wantTotal:    1250,
		},
		{
			name: "exactly at discount threshold: discount applies",
			order: Order{
				OrderID:    "order-2",
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: 10_000}},
			},
			wantSubtotal: 10_000,
			wantDiscount: 1_000,
			wantTotal:    9_000,
		},
		{
			name: "one cent below discount threshold: no discount",
			order: Order{
				OrderID:    "order-3",
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: 9_999}},
			},
			wantSubtotal: 9_999,
			wantDiscount: 0,
			wantTotal:    9_999,
		},
		{
			name: "well above discount threshold",
			order: Order{
				OrderID:    "order-4",
				CustomerID: "cust-1",
				Items:      []Item{{SKU: "sku-1", Quantity: 2, UnitPriceCents: 10_000}},
			},
			wantSubtotal: 20_000,
			wantDiscount: 2_000,
			wantTotal:    18_000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Process(tc.order, now)

			if got.SubtotalCents != tc.wantSubtotal {
				t.Errorf("SubtotalCents = %d, want %d", got.SubtotalCents, tc.wantSubtotal)
			}
			if got.DiscountCents != tc.wantDiscount {
				t.Errorf("DiscountCents = %d, want %d", got.DiscountCents, tc.wantDiscount)
			}
			if got.TotalCents != tc.wantTotal {
				t.Errorf("TotalCents = %d, want %d", got.TotalCents, tc.wantTotal)
			}
			if got.Status != StatusProcessed {
				t.Errorf("Status = %q, want %q", got.Status, StatusProcessed)
			}
			if !got.ProcessedAt.Equal(now) {
				t.Errorf("ProcessedAt = %v, want %v", got.ProcessedAt, now)
			}
			if got.OrderID != tc.order.OrderID {
				t.Errorf("OrderID = %q, want %q", got.OrderID, tc.order.OrderID)
			}
		})
	}
}

func TestReject(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	o := Order{OrderID: "order-5", CustomerID: "cust-1"}

	got := Reject(o, "at least one item is required", now)

	if got.Status != StatusRejected {
		t.Errorf("Status = %q, want %q", got.Status, StatusRejected)
	}
	if got.Reason != "at least one item is required" {
		t.Errorf("Reason = %q, want the rejection reason", got.Reason)
	}
	if got.TotalCents != 0 {
		t.Errorf("TotalCents = %d, want 0 for a rejected order", got.TotalCents)
	}
	if got.OrderID != o.OrderID || got.CustomerID != o.CustomerID {
		t.Errorf("Reject() did not carry over OrderID/CustomerID: got %+v", got)
	}
}

func TestNewID(t *testing.T) {
	a, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	b, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}

	if len(a) != 16 {
		t.Errorf("NewID() length = %d, want 16 hex chars", len(a))
	}
	if a == b {
		t.Errorf("NewID() returned the same value twice: %q", a)
	}
}

// TestValidateStopsAtFirstInvalidItem checks that the item index reported
// in the error is the actual offending item, not always item 0 -- a
// regression here would make the error message actively misleading for
// any order with more than one line item.
func TestValidateStopsAtFirstInvalidItem(t *testing.T) {
	o := Order{
		CustomerID: "cust-1",
		Items: []Item{
			{SKU: "sku-1", Quantity: 1, UnitPriceCents: 500},
			{SKU: "sku-2", Quantity: 0, UnitPriceCents: 500},
		},
	}

	err := Validate(o)
	if err == nil {
		t.Fatal("Validate() = nil, want an error for the second item's zero quantity")
	}
	if !strings.Contains(err.Error(), "item 1") {
		t.Errorf("Validate() error = %q, want it to reference item 1 (the invalid one), not item 0", err.Error())
	}
}

// TestProcessMultipleItemsAtThreshold makes sure the discount threshold is
// checked against the *summed* subtotal across every line item, not just
// a single item's price -- two $60 items should trigger the same discount
// as one $120 item.
func TestProcessMultipleItemsAtThreshold(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	o := Order{
		OrderID:    "order-6",
		CustomerID: "cust-1",
		Items: []Item{
			{SKU: "sku-1", Quantity: 3, UnitPriceCents: 2_000}, // 6,000
			{SKU: "sku-2", Quantity: 2, UnitPriceCents: 2_000}, // 4,000
		},
	}

	got := Process(o, now)

	if got.SubtotalCents != 10_000 {
		t.Fatalf("SubtotalCents = %d, want 10000 (summed across both line items)", got.SubtotalCents)
	}
	if got.DiscountCents != 1_000 {
		t.Errorf("DiscountCents = %d, want 1000 -- a multi-item order at exactly the threshold should discount the same as a single-item order at that total", got.DiscountCents)
	}
	if got.TotalCents != 9_000 {
		t.Errorf("TotalCents = %d, want 9000", got.TotalCents)
	}
}

// TestRejectZeroesOutMoneyFields guards against a Reject() that
// accidentally computes a partial subtotal/total for a rejected order --
// a rejection should never carry a dollar amount into "orders.processed".
func TestRejectZeroesOutMoneyFields(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	o := Order{OrderID: "order-7", CustomerID: "cust-1"}

	got := Reject(o, "no items", now)

	if got.SubtotalCents != 0 || got.DiscountCents != 0 || got.TotalCents != 0 {
		t.Errorf("Reject() left a nonzero money field: %+v, want all-zero for a rejected order", got)
	}
}

// TestNewIDManyAreUnique generates 1000 IDs and checks both the format
// (16 lowercase hex characters, matching hex.EncodeToString of an 8-byte
// value) and that none collide -- TestNewID above only checks two calls,
// which wouldn't catch a generator that's biased toward a small range.
func TestNewIDManyAreUnique(t *testing.T) {
	const n = 1000
	idPattern := regexp.MustCompile(`^[0-9a-f]{16}$`)
	seen := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		id, err := NewID()
		if err != nil {
			t.Fatalf("NewID() error on call %d: %v", i, err)
		}
		if !idPattern.MatchString(id) {
			t.Fatalf("NewID() = %q, want 16 lowercase hex characters", id)
		}
		if seen[id] {
			t.Fatalf("NewID() produced a duplicate after %d calls: %q", i, id)
		}
		seen[id] = true
	}
}
