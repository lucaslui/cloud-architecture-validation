FROM golang:1.25.1 AS build
WORKDIR /build

COPY . .

RUN go build -o /out/schema-loader ./cmd/schema-loader

FROM gcr.io/distroless/base-debian12:latest AS runtime
WORKDIR /app

COPY ./resources/schemas /app/schemas
COPY --from=build /out/schema-loader /app/schema-loader

ENTRYPOINT ["/app/schema-loader"]