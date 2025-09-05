FROM golang:1.22 as build
WORKDIR /src

COPY . .

RUN cd src && go build -o /out/schema-loader

FROM gcr.io/distroless/base-debian12 as runtime

COPY --from=build /out/schema-loader /app/schema-loader

ENTRYPOINT ["/app/schema-loader"]