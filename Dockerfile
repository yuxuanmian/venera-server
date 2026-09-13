# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/server/ ./cmd/server/
COPY internal/ ./internal/

ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/venera-server ./cmd/server
RUN mkdir -p /runtime/data && chown 65532:65532 /runtime/data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/venera-server /venera-server
COPY --from=build --chown=65532:65532 /runtime/data /data
WORKDIR /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/venera-server"]
