# Postgres driver is pure Go, so the bridge no longer needs CGO/sqlite.
FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -o /out/whatsapp-bridge .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/whatsapp-bridge /app/whatsapp-bridge
RUN mkdir -p /app/store

EXPOSE 8080
VOLUME ["/app/store"]
CMD ["/app/whatsapp-bridge"]
