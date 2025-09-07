FROM golang:1.25.1 AS build

WORKDIR /build

COPY . .

RUN go build -o /out/validator ./cmd/validator

FROM gcr.io/distroless/base-debian12:latest AS runtime

WORKDIR /app

COPY --from=build /out/validator /app/validator

ENTRYPOINT ["/app/validator"]
