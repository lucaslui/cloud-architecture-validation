FROM golang:1.25.1 AS build

WORKDIR /build

COPY . .

RUN go build -o /out/enricher ./cmd/enricher

FROM gcr.io/distroless/base-debian12:latest AS runtime

WORKDIR /app

ENV CONTEXT_STORE_PATH=/app/device-context.json

COPY --from=build /out/enricher /app/enricher

COPY ./resources/device-context.json /app/device-context.json

ENTRYPOINT ["/app/enricher"]
