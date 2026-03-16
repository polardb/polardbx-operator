FROM node:18-alpine AS frontend-build

WORKDIR /workspace/dashboard-frontend
COPY tools/dashboard/frontend/package*.json ./
RUN npm ci
COPY tools/dashboard/frontend/ ./
RUN npm run build

FROM golang:1.21-alpine AS backend-build

WORKDIR /workspace
# Copy root go.mod first (for replace directives)
COPY go.mod go.sum ./
# Copy source code: backend, api, and pkg
# Note: backend has its own pkg/api/router, root pkg/ is for operator packages
COPY tools/dashboard/backend/ ./backend/
COPY api/ ./api/
COPY pkg/ ./pkg/
# Fix replace path in go.mod: ../../.. -> ../ (needed for Docker build context)
# The replace directive in go.mod points to ../../.. (from tools/dashboard/backend/),
# but in Docker it should be ../ (from /workspace/backend/)
WORKDIR /workspace/backend
RUN sed -i 's|=> \.\./\.\./\.\./|=> ../|g' go.mod && \
    sed -i 's|=> \.\./\.\./\.\.|=> ../|g' go.mod && \
    echo "✓ Updated replace path:" && \
    grep "replace.*polardbx-operator" go.mod && \
    echo "---" && \
    cat go.mod | grep "replace" | head -5
# Download dependencies (from workspace root, after fixing replace path)
WORKDIR /workspace
RUN go mod download
# Update backend go.mod (run tidy from backend directory)
WORKDIR /workspace/backend
RUN go mod tidy
# Build from backend directory
WORKDIR /workspace/backend
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/polardbx-dashboard-backend ./main.go

FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=backend-build /bin/polardbx-dashboard-backend /app/polardbx-dashboard-backend
# Angular build outputs to dist/polardbx-ui/browser/ (new Angular build system)
COPY --from=frontend-build /workspace/dashboard-frontend/dist/polardbx-ui/browser/ /app/ui/

# Bundle charts needed by monitoring pages (Grafana dashboard templates, PrometheusRule templates, etc.)
COPY charts/polardbx-monitor/ /app/charts/polardbx-monitor/

# Make Grafana template directory explicit (can be overridden by env)
ENV POLARDBX_GRAFANA_TEMPLATES_DIR=/app/charts/polardbx-monitor/dashboard

ENV UI_STATIC_DIR=/app/ui
EXPOSE 8080

ENTRYPOINT ["/app/polardbx-dashboard-backend"]


