# Build fully offline from the vendored module cache. On a CONNECTED host run
#   make vendor
# first so vendor/ + go.sum exist; after that this build needs no network.
#
# Two stages: a Go builder and a distroless static runtime (no shell, runs as a
# non-root UID — OpenShift `restricted` SCC compatible).

FROM golang:1.24 AS build
WORKDIR /src

# Copy everything (including vendor/). .dockerignore keeps junk out.
COPY . .

# Static, stripped, reproducible binary for linux/amd64. -mod=vendor guarantees
# no module downloads.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -trimpath -ldflags="-s -w" \
    -o /out/spangen ./cmd/spangen

# Distroless static: ~2MB base, no package manager, includes CA certs, runs as
# UID 65532 (nonroot). OpenShift may override the UID; the binary needs no
# specific UID and writes nothing to the filesystem, so arbitrary UIDs are fine.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/spangen /usr/local/bin/spangen
USER 65532:65532
EXPOSE 8888
ENTRYPOINT ["/usr/local/bin/spangen"]
