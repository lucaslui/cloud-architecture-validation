FROM golang:1.25.1 AS build
WORKDIR /build

COPY . .

RUN go build -o /out/collector ./cmd/collector

FROM gcr.io/distroless/base-debian12:latest AS runtime
WORKDIR /app

ENV MQTT_BROKER_URL=tcp://vernemq:1883 \
    MQTT_TOPIC=ingestion/telemetry \
    MQTT_QOS=0 \
    KAFKA_BROKERS=kafka:9092 \
    KAFKA_TOPIC=raw-events-topic \
    KAFKA_DLQ_TOPIC=raw-events-dlq

COPY --from=build /out/collector /app/collector

ENTRYPOINT ["/app/collector"]
