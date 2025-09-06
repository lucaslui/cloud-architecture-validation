FROM golang:1.25.1 AS build

WORKDIR /build

COPY . .

RUN go build -o /out/collector ./cmd/collector

FROM gcr.io/distroless/base-debian12:latest AS runtime

WORKDIR /app

COPY --from=build /out/collector /app/collector

ENTRYPOINT ["/app/collector"]
