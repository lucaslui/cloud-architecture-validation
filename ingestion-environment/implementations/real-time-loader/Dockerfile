FROM golang:1.25.1 AS build

WORKDIR /build

COPY . .

RUN go build -o /out/real-time-loader ./cmd/real-time-loader

FROM gcr.io/distroless/base-debian12:latest AS runtime

WORKDIR /app

COPY --from=build /out/real-time-loader /app/real-time-loader

ENTRYPOINT ["/app/real-time-loader"]
