FROM golang:1.14-alpine as builder

ARG GOPROXY

WORKDIR /tagd
COPY . .

RUN go mod download

# Build the binary
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOPROXY=${GOPROXY} \
  go build -ldflags="-w -s" -a -o bin/tagd cmd/tagd/*

############################

FROM alpine:3.12

RUN addgroup -S tagd \
  && adduser -S -g tagd tagd

WORKDIR /home/tagd
# Copy our static executable
COPY --from=builder /tagd/bin/tagd .
RUN chown -R tagd:tagd ./

# Use an unprivileged user.
USER tagd

CMD ["./tagd"]