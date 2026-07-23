// Package httpapi is the HTTP ingestion boundary for cmd/producer. It
// depends only on streaming.Publisher (an interface), never on a concrete
// Kafka writer -- see handler_test.go, whose tests run entirely against an
// in-memory fake and never touch a broker.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/yusrafaheem/go-kafka-orderflow/internal/order"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/streaming"
)

// OrdersHandler serves POST /orders: decode, assign an ID, validate, and
// publish an OrderCreated event.
type OrdersHandler struct {
	Publisher streaming.Publisher
	Topic     string
	Logger    *slog.Logger
}

type createOrderRequest struct {
	CustomerID string       `json:"customer_id"`
	Items      []order.Item `json:"items"`
}

type createOrderResponse struct {
	OrderID string `json:"order_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *OrdersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	id, err := order.NewID()
	if err != nil {
		h.logger().Error("failed to generate order id", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	o := order.Order{
		OrderID:    id,
		CustomerID: req.CustomerID,
		Items:      req.Items,
		CreatedAt:  time.Now().UTC(),
	}

	if err := order.Validate(o); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	payload, err := json.Marshal(o)
	if err != nil {
		h.logger().Error("failed to marshal order", "order_id", o.OrderID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Partition by customer ID: every order from the same customer lands
	// on the same partition and is therefore processed in the order it
	// was published -- which matters the moment a future feature (e.g.
	// "cancel my most recent order") needs to see one customer's orders
	// in submission order.
	if err := h.Publisher.Publish(r.Context(), h.Topic, o.CustomerID, payload); err != nil {
		h.logger().Error("failed to publish order", "order_id", o.OrderID, "error", err)
		writeError(w, http.StatusServiceUnavailable, "failed to accept order, please retry")
		return
	}

	h.logger().Info("order accepted", "order_id", o.OrderID, "customer_id", o.CustomerID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(createOrderResponse{OrderID: o.OrderID})
}

func (h *OrdersHandler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}
