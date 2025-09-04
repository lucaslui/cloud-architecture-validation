# syntax=docker/dockerfile:1.7

#############################
# Build
#############################
FROM golang:1.22.4 AS build
WORKDIR /app

# 1) Manifests primeiro (maximiza cache)
COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# 2) Código (apenas a pasta src/)
COPY src ./src

# 3) Gera/atualiza go.sum com base nos imports reais
RUN --mount=type=cache,target=/go/pkg/mod go mod tidy

# 4) Compila o binário apontando para ./src (onde está o main)
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GO111MODULE=on \
    go build -trimpath -buildvcs=false \
    -ldflags="-s -w" \
    -o /out/collector ./src

#############################
# Runtime (distroless, nonroot)
#############################
FROM gcr.io/distroless/base-debian12:nonroot

# Defaults (pode sobrepor via compose/ENV)
ENV MQTT_BROKER_URL=tcp://vernemq:1883 \
    MQTT_TOPIC=ingestion/telemetry \
    MQTT_QOS=0 \
    KAFKA_BROKERS=kafka:9092 \
    KAFKA_TOPIC=raw-events-topic \
    KAFKA_DLQ_TOPIC=raw-events-dlq

COPY --from=build /out/collector /collector

USER nonroot:nonroot
ENTRYPOINT ["/collector"]
