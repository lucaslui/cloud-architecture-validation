FROM golang:1.25.1 AS build
WORKDIR /src

COPY . .

RUN cd src && go build -o /out/schema-loader

FROM gcr.io/distroless/base-debian12:latest AS runtime

COPY --from=build /out/schema-loader /app/schema-loader

ENTRYPOINT ["/app/schema-loader"]