# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY . .
# go.sum is intentionally not committed to this repo (see README, "How
# this was built") -- `go mod tidy` resolves the module graph and writes a
# fresh go.sum on every build instead of trusting a checked-in one.
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer

FROM alpine:3.19
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/producer /usr/local/bin/producer
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/producer"]
