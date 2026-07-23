# go-kafka-orderflow

A small order-processing pipeline in Go, built around a real Kafka
producer and two independent consumer groups. Three binaries, one Kafka
broker:

```
                    POST /orders                 orders.processed
   client  ────────────────────►  producer                                notifier
                                      │           (topic)                 (logs every
                                      │ publish                           processed order,
                                      ▼                                   independent
                              ┌───────────────┐                           consumer group)
                              │  orders topic │
                              └───────┬───────┘                                ▲
                                      │ consume                                │
                                      ▼                                        │
                                  consumer  ───── publish ─────────────────────┘
                                      │
                                      │ (unprocessable messages)
                                      ▼
                              ┌───────────────┐
                              │ orders.dlq    │
                              └───────────────┘
```

`producer` is an HTTP service: `POST /orders` validates the request and
publishes an `OrderCreated` event to the `orders` topic, keyed by customer
ID. `consumer` is a consumer-group member that reads `orders`, prices each
order, and publishes the outcome -- success or rejection -- to
`orders.processed`. `notifier` is a *second*, independent consumer group
subscribed to `orders.processed`, purely to demonstrate that Kafka's
pub/sub fan-out actually works: the same topic, two unrelated sets of
committed offsets.

## Why this exists

"Used Kafka" is easy to write on a resume and easy to leave shallow --
`docker run kafka` and a five-line producer loop don't require
understanding partitioning, consumer groups, offset commit semantics, or
what happens when a message can't be processed. This project is scoped
specifically to force those decisions: a partitioning key that actually
matters (customer ordering), two consumer groups that have to not collide
on GroupID, a manual commit strategy chosen over the default for a
specific reason, and a dead-letter topic instead of an infinite retry
loop or a silently dropped message.

## Kafka concepts this project actually exercises

**Partitioning by key.** `producer` publishes every order keyed by
`customer_id` (`internal/httpapi/handler.go`). Kafka guarantees all
messages with the same key land on the same partition, and a single
partition is read in publish order -- so every order from one customer is
guaranteed to be *processed* in the order it was *submitted*, without the
service needing to coordinate that itself. Two different customers'
orders can and do land on different partitions and process out of order
relative to each other, which is fine: nothing in this system needs
global ordering, only per-customer ordering.

**Consumer groups and independent offsets.** `consumer` and `notifier`
are both consumer-group members, but with different `GroupID`s
(`orderflow-consumer` and `orderflow-notifier` by default). Kafka tracks
one committed offset per `(GroupID, Topic, Partition)` triple, so these
two processes progress through their respective topics completely
independently -- restarting `notifier` never affects what `consumer` has
already committed, and vice versa. This is also why `notifier` deliberately
does *not* share `consumer`'s default `KAFKA_GROUP_ID`: two processes on
the *same* GroupID and topic split that topic's partitions between them
(that's how you'd scale out `consumer` horizontally), which is the
opposite of what `notifier` needs -- it wants to see every message.

**Manual, synchronous offset commits.** `internal/streaming/consumer.go`
sets `CommitInterval` to its zero value, which makes `CommitMessages`
commit synchronously on every call instead of batching on a timer. The
default timer-based commit is faster under load, but it means a message
your handler already processed can still get redelivered after a crash,
because the offset commit for it hadn't fired yet. Committing after every
single message trades some throughput for a much simpler guarantee: by
the time a message has been handled, it will not be redelivered on a
clean shutdown.

**At-least-once delivery, not exactly-once.** This project does not claim
exactly-once semantics, because kafka-go's `Writer` doesn't implement the
transactional-producer protocol that real exactly-once guarantees require
(that's a genuinely different, heavier mechanism -- see Kafka's EOS
design). What it *does* do: `RequiredAcks: kafka.RequireAll` on the
producer (`internal/streaming/producer.go`) waits for every in-sync
replica to acknowledge a write before considering it durable, and the
consumer's "commit after handling" ordering means a crash can cause a
message to be *reprocessed*, never silently dropped. A production version
of this would additionally make `order.Process` idempotent by `OrderID`
(e.g. an upsert keyed by order ID instead of an unconditional insert) so
that reprocessing the same order twice after a crash is harmless -- this
repo doesn't add that persistence layer, but the design leaves room for it
deliberately.

**Dead-letter topic instead of blocking or dropping.**
`internal/streaming/consumer.go`'s `Run` loop commits the offset after
every message regardless of whether the handler succeeded. That's on
purpose: retrying a broken handler in-place, forever, stalls the entire
partition behind one bad message and eventually triggers a rebalance from
a missed consumer-group heartbeat. Instead, `cmd/consumer`'s handler
routes anything it can't process -- malformed JSON, a downstream publish
failure -- to `orders.dlq` before returning, so `Run`'s "commit anyway"
policy never actually means "silently discard." A human (or a future
reprocessing job) can replay `orders.dlq` later; the live pipeline just
doesn't wait on that decision.

## How this was built

This project was written in a sandbox with no Go toolchain installed and
no network access to `proxy.golang.org`, `registry.npmjs.org`, or even
`apt`'s package mirrors -- every outbound request came back a hard `403`
from a local proxy, confirmed with `curl`, `npm install`, and `apt-get
download` before concluding there was genuinely no path to a working
local `go build`. Rather than write Go blind and hope, this project
leans on the same "CI as compiler and test oracle" pattern used
elsewhere in this account's repos when a local toolchain wasn't
available: GitHub Actions' runners have full network access, so `go mod
tidy`, `gofmt`, `go vet`, `go build`, and `go test` all run for real on
every push, and CI's failures -- not local intuition -- are what actually
verify this code compiles and behaves as intended.

One concrete consequence: `go.sum` is not committed to this repo. It's a
cryptographic record of exact module content hashes, and there was no way
to generate a *real* one without network access to fetch and hash the
actual `segmentio/kafka-go` module. `go mod tidy` regenerates it fresh at
the start of both the CI workflow and each Docker build, which is a
reasonable tradeoff for a project built this way, but is a deliberate
departure from normal Go project hygiene (a real project should commit
`go.sum` for reproducible, verifiable builds) -- worth calling out
explicitly rather than leaving unexplained.

## Testing

`internal/order` and `internal/httpapi` are unit tested with the standard
library's `testing` package and no live broker:

- `internal/order/service_test.go` -- table-driven tests for `Validate`
  (missing customer ID, no items, non-positive quantity, negative price,
  missing SKU) and `Process` (subtotal arithmetic, and specifically the
  discount-threshold boundary: one cent below $100 gets no discount, ten
  thousand cents exactly gets the discount applied).
- `internal/httpapi/handler_test.go` -- exercises `OrdersHandler` against
  an in-memory fake implementing `streaming.Publisher`, so these tests
  assert on exactly what *would* have been published (topic, partition
  key, JSON payload) without a broker anywhere in the test binary. Covers
  a valid order, invalid JSON, a validation failure, a publisher error,
  and a disallowed HTTP method.

`internal/streaming` (the actual Kafka wire-protocol wrapper) and the
`cmd/*` binaries are intentionally *not* covered by `go test` -- verifying
real produce/consume/commit behavior needs a real broker, and pulling
that into the default `go test ./...` target (via testcontainers or a
Kafka service container) would make CI flaky on infrastructure this
project doesn't otherwise need to depend on. The actual end-to-end path
is verified manually via `make up && make demo` (see below) and by the
`docker` CI job, which builds all three service images for real on every
push -- if the code didn't compile inside its own Dockerfile, that job
fails too.

## Running it

Requires Docker and Docker Compose (the Go services themselves don't need
a local Go toolchain to run this way -- they're built inside their own
containers):

```bash
make up      # builds and starts kafka + producer + consumer + notifier
make demo    # posts 3 sample orders: small, discount-eligible, and invalid
make logs    # tail consumer + notifier output -- watch the 3 orders flow through
make down    # tear everything down
```

With a local Go toolchain:

```bash
go mod tidy
make fmt     # gofmt -l . -- should print nothing
make vet     # go vet ./...
make build   # go build ./...
make test    # go test ./... -v
```

## Scope

What this project deliberately simplifies, and why:

- **Single broker, single partition count.** Kafka's real value shows up
  at multiple brokers and multiple partitions per topic; this repo runs
  one broker in KRaft mode with default topic settings (auto-created on
  first publish) because the goal is demonstrating the client-side
  patterns -- partitioning keys, consumer groups, commit strategy, DLQ --
  not standing up a multi-broker cluster.
- **No transactional / exactly-once producer.** Covered above under
  "at-least-once delivery" -- this is a real, named limitation, not an
  oversight.
- **In-memory-only business logic.** `order.Process` is a pure function;
  there's no database making `OrderID` actually idempotent on reprocessing.
  Real order-processing would persist processed orders keyed by ID and
  upsert instead of blindly recomputing and republishing.
- **No schema registry.** Messages are plain JSON, not Avro/Protobuf
  against a registry. JSON keeps the project readable without pulling in
  Confluent-specific tooling that would be a bigger dependency than the
  Kafka client itself.

## Prior art

Built on [segmentio/kafka-go](https://github.com/segmentio/kafka-go), a
pure-Go Kafka client (no cgo, unlike bindings built on librdkafka) --
chosen specifically because CI needed to build this project's Docker
images with nothing but `go build`, no C toolchain in the container.

The single-node broker runs on
[bitnami/kafka](https://hub.docker.com/r/bitnami/kafka) in KRaft mode
(Kafka's post-ZooKeeper consensus mechanism), configured with the
dual-listener setup documented in `docker-compose.yml` -- the standard fix
for the single most common first-time mistake in Kafka-via-Docker
tutorials (a broker whose advertised address only resolves from inside
its own container).
