package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/yusrafaheem/go-kafka-orderflow/internal/order"
)

var errPublishBoom = errors.New("boom: broker unreachable")

// fakePublisher records every Publish call in memory instead of talking to
// a broker, and can be told to fail on demand. It implements
// streaming.Publisher entirely to keep these tests broker-free.
type fakePublisher struct {
	mu       sync.Mutex
	calls    []publishCall
	failWith error
}

type publishCall struct {
	topic string
	key   string
	value []byte
}

func (f *fakePublisher) Publish(_ context.Context, topic, key string, value []byte) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, publishCall{topic: topic, key: key, value: value})
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) lastCall() (publishCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return publishCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

func TestOrdersHandlerValidOrder(t *testing.T) {
	pub := &fakePublisher{}
	h := &OrdersHandler{Publisher: pub, Topic: "orders"}

	body := `{"customer_id":"cust-1","items":[{"sku":"sku-1","quantity":2,"unit_price_cents":500}]}`
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp createOrderResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.OrderID == "" {
		t.Error("response order_id is empty")
	}

	call, ok := pub.lastCall()
	if !ok {
		t.Fatal("Publisher.Publish was never called")
	}
	if call.topic != "orders" {
		t.Errorf("published topic = %q, want %q", call.topic, "orders")
	}
	if call.key != "cust-1" {
		t.Errorf("published key = %q, want customer id %q", call.key, "cust-1")
	}

	var published order.Order
	if err := json.Unmarshal(call.value, &published); err != nil {
		t.Fatalf("failed to decode published payload: %v", err)
	}
	if published.OrderID != resp.OrderID {
		t.Errorf("published order_id = %q, want %q (matching the HTTP response)", published.OrderID, resp.OrderID)
	}
	if len(published.Items) != 1 || published.Items[0].SKU != "sku-1" {
		t.Errorf("published items = %+v, want the single sku-1 line item", published.Items)
	}
}

func TestOrdersHandlerInvalidJSON(t *testing.T) {
	pub := &fakePublisher{}
	h := &OrdersHandler{Publisher: pub, Topic: "orders"}

	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(`not json`))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if _, ok := pub.lastCall(); ok {
		t.Error("Publisher.Publish was called for a request that never should have reached it")
	}
}

func TestOrdersHandlerFailsValidation(t *testing.T) {
	pub := &fakePublisher{}
	h := &OrdersHandler{Publisher: pub, Topic: "orders"}

	// No items -- fails order.Validate.
	body := `{"customer_id":"cust-1","items":[]}`
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if _, ok := pub.lastCall(); ok {
		t.Error("Publisher.Publish was called for an order that failed validation")
	}
}

func TestOrdersHandlerPublishFailure(t *testing.T) {
	pub := &fakePublisher{failWith: errPublishBoom}
	h := &OrdersHandler{Publisher: pub, Topic: "orders"}

	body := `{"customer_id":"cust-1","items":[{"sku":"sku-1","quantity":1,"unit_price_cents":500}]}`
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestOrdersHandlerWrongMethod(t *testing.T) {
	pub := &fakePublisher{}
	h := &OrdersHandler{Publisher: pub, Topic: "orders"}

	req := httptest.NewRequest(http.MethodGet, "/orders", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
