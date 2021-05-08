# Use base golang image from Docker Hub
FROM golang:1.14.13-buster as build

WORKDIR /src/github.com/iyashu/pulsar-hack

# Copy go.mod and go.sum
ADD ./go.mod /src/github.com/iyashu/pulsar-hack
ADD ./go.sum /src/github.com/iyashu/pulsar-hack
# Install dependencies in go.mod and go.sum
RUN go mod download

# Copy application source code
COPY ./server.go /src/github.com/iyashu/pulsar-hack
COPY ./externalscaler/ /src/github.com/iyashu/pulsar-hack/externalscaler

# Compile the application to /app.
RUN go build -o /app -v .

# Now create separate deployment image
FROM gcr.io/distroless/base
COPY --from=build /app /app
# Cause full tracebacks; also serves to identify this image as a Go image for `skaffold debug`
ENV GOTRACEBACK=all
ENTRYPOINT ["/app"]
