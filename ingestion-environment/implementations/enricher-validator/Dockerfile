FROM golang:1.25.1 AS build
WORKDIR /build

COPY . .

RUN go build -o /out/enricher-validator ./cmd/enricher-validator

FROM gcr.io/distroless/base-debian12:latest AS runtime
WORKDIR /app

ENV CONTEXT_STORE_PATH=/app/device-context.json

COPY --from=build /out/enricher-validator /app/enricher-validator
COPY ./resources/device-context.json /app/device-context.json

ENTRYPOINT ["/app/enricher-validator"]
