FROM golang:1.25.1 AS build
WORKDIR /build

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /out && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/enricher-validator ./cmd/enricher-validator

FROM gcr.io/distroless/base-debian12:nonroot AS runtime
WORKDIR /app

ENV CONTEXT_STORE_PATH=/app/device-context.json

COPY --from=build /out/enricher-validator /app/enricher-validator
COPY device-context.json /app/device-context.json

USER nonroot:nonroot
ENTRYPOINT ["/app/enricher-validator"]
