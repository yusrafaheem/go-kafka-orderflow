# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY . .
# See cmd/producer/Dockerfile for why go.sum is regenerated here rather
# than copied in from the repo.
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer

FROM alpine:3.19
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/consumer /usr/local/bin/consumer
ENTRYPOINT ["/usr/local/bin/consumer"]
