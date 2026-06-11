# syntax=docker/dockerfile:1
#
# Builds a tiny static image holding the single carboot binary. The build stage
# runs natively on the build host and cross-compiles to the target arch (no
# emulation), so building an amd64 image from an arm64 Mac is fast.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags='-s -w' -o /out/carboot ./cmd/carboot

# distroless/static carries CA certs (advertise dials cid.contact over HTTPS).
# This is the root (uid 0) variant on purpose, not :nonroot: reindex must WRITE
# the index, and a read-only gateway over a WAL index still creates the -shm
# sidecar. Under rootless podman, container uid 0 maps to the invoking host user,
# so a host-owned index/cars volume is writable with no --user flag or chown. The
# binary lands in /usr/local/bin; the default command is the gateway, with
# reindex and advertise available as subcommands.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/carboot /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/carboot"]
CMD ["gateway"]
