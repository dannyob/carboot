# syntax=docker/dockerfile:1
#
# Builds a tiny static image holding both carboot binaries. The build stage
# runs natively on the build host and cross-compiles to the target arch (no
# emulation), so building an amd64 image from an arm64 Mac is fast.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags='-s -w' -o /out/cargw  ./cmd/cargw \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags='-s -w' -o /out/carpni ./cmd/carpni

# distroless/static carries CA certs (carpni dials cid.contact over HTTPS) and
# runs as nonroot. Both binaries land in /usr/local/bin; pick one as the
# container command, e.g. `... carboot /usr/local/bin/carpni --index ...`.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cargw /out/carpni /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/cargw"]
