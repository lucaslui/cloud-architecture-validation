FROM golang:1.25.1 AS build

WORKDIR /build

COPY . .

RUN go build -o /out/batch-loader ./cmd/batch-loader

FROM gcr.io/distroless/base-debian12:latest AS runtime

WORKDIR /app

COPY --from=build /out/batch-loader /app/batch-loader

ENTRYPOINT ["/app/batch-loader"]
