// Command producer runs the HTTP ingestion service: it accepts orders over
// HTTP and publishes each one as an OrderCreated event on the "orders"
// topic, partitioned by customer ID.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yusrafaheem/go-kafka-orderflow/internal/config"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/httpapi"
	"github.com/yusrafaheem/go-kafka-orderflow/internal/streaming"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	producer := streaming.NewProducer(cfg.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("error closing producer", "error", err)
		}
	}()

	handler := &httpapi.OrdersHandler{
		Publisher: producer,
		Topic:     cfg.TopicOrders,
		Logger:    logger,
	}

	mux := http.NewServeMux()
	mux.Handle("/orders", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("producer listening", "addr", cfg.HTTPAddr, "topic", cfg.TopicOrders, "brokers", cfg.Brokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("error during http shutdown", "error", err)
	}
}
