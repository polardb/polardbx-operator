# This Dockerfile is intended to be built with the repository root as the build context:
#
#   docker build -f tools/dashboard/backend/Dockerfile -t polardbx-dashboard-backend:dev .
#
# The backend module relies on a local `replace github.com/alibaba/polardbx-operator => ../../..`,
# so we copy the repo root `go.mod/go.sum` and the `api/` package into the image.

# --- Stage 1: Build ---
FROM golang:1.21-alpine AS builder

WORKDIR /src

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG GO_VERSION=unknown

# Copy module files first (better layer caching)
COPY go.mod go.sum ./
COPY tools/dashboard/backend/go.mod tools/dashboard/backend/go.sum tools/dashboard/backend/

# Copy backend source and operator API definitions needed by the replace directive.
COPY api api
COPY pkg pkg
COPY tools/dashboard/backend tools/dashboard/backend

# Download dependencies. The replace directive will be respected.
RUN cd tools/dashboard/backend && go mod tidy && go mod download

# Build the Go app, creating a static binary
# CGO_ENABLED=0 is important for a static build
# -ldflags="-w -s" strips debug information to reduce binary size
RUN cd tools/dashboard/backend && CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-w -s \
    -X 'polardbx-dashboard-backend/pkg/api/router.Version=${VERSION}' \
    -X 'polardbx-dashboard-backend/pkg/api/router.Commit=${COMMIT}' \
    -X 'polardbx-dashboard-backend/pkg/api/router.BuildDate=${BUILD_DATE}' \
    -X 'polardbx-dashboard-backend/pkg/api/router.GoVersion=${GO_VERSION}'" \
    -o /polardbx-dashboard-backend ./main.go

# --- Stage 2: Final ---
# Use a minimal image for the final stage
FROM scratch

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /polardbx-dashboard-backend /app/polardbx-dashboard-backend

# Bundle charts needed by monitoring pages (Grafana dashboard templates, PrometheusRule templates, etc.)
COPY charts/polardbx-monitor/ /app/charts/polardbx-monitor/

# Make Grafana template directory explicit (can be overridden by env)
ENV POLARDBX_GRAFANA_TEMPLATES_DIR=/app/charts/polardbx-monitor/dashboard

# Expose the port the app runs on
EXPOSE 8080

# The command to run the application
ENTRYPOINT ["/app/polardbx-dashboard-backend"] 
