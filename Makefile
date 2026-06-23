# spangen build/packaging. Run `make vendor` once on a CONNECTED host; after
# that, `make image` builds with no network access.

IMAGE ?= spangen:latest
TAR   ?= spangen-image.tar

.PHONY: vendor build vet image save load run-local-ch run-local-otlp clean

## vendor: resolve deps, generate go.sum, populate vendor/ (needs internet)
vendor:
	go mod tidy
	go mod vendor

## build: compile the binary locally
build:
	CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o bin/spangen ./cmd/spangen

## vet: static checks
vet:
	go vet ./...

## image: build the container image (offline once vendored)
image:
	docker build -f deploy/Dockerfile -t $(IMAGE) .

## save: export the image to a tar for air-gapped transfer
save:
	docker save $(IMAGE) | gzip > $(TAR).gz
	@echo "wrote $(TAR).gz  ->  copy to the air-gapped registry host and: gunzip -c $(TAR).gz | docker load"

## load: import the image tar (run on the air-gapped host)
load:
	gunzip -c $(TAR).gz | docker load

## run-local-ch: smoke test against a local ClickHouse on :9000
run-local-ch:
	SPANGEN_SINK=clickhouse SPANGEN_CH_ENDPOINTS=localhost:9000 SPANGEN_CH_MODE=local \
	SPANGEN_CH_TABLE=otel_traces SPANGEN_RATE=20000 SPANGEN_RAMP=5s \
	go run ./cmd/spangen

## run-local-otlp: smoke test against a local collector on :4317
run-local-otlp:
	SPANGEN_SINK=otlp SPANGEN_OTLP_ENDPOINT=localhost:4317 SPANGEN_OTLP_INSECURE=true \
	SPANGEN_RATE=20000 SPANGEN_RAMP=5s \
	go run ./cmd/spangen

clean:
	rm -rf bin $(TAR) $(TAR).gz
