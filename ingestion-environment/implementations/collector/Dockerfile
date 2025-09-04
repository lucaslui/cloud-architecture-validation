FROM golang:1.22.4 AS build
WORKDIR /src

COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod tidy

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GO111MODULE=on go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/collector .

FROM gcr.io/distroless/base-debian12:nonroot AS runtime

ENV MQTT_BROKER_URL=tcp://vernemq:1883 \
    MQTT_TOPIC=ingestion/telemetry \
    KAFKA_BROKERS=kafka:9092 \
    KAFKA_TOPIC=raw-events-topic \
    KAFKA_DLQ_TOPIC=raw-events-dlq \
    MQTT_QOS=0

COPY --from=build /out/collector /collector

USER nonroot:nonroot
ENTRYPOINT ["/collector"]
