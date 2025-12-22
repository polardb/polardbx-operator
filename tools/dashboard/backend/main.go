package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"polardbx-dashboard-backend/pkg/api/router"
	"polardbx-dashboard-backend/pkg/cache"
	"polardbx-dashboard-backend/pkg/config"
	"polardbx-dashboard-backend/pkg/logger"

	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// main is the entrypoint of the PolarDB-X UI backend HTTP server.
// It initializes logging, validates configuration, sets up routes, and runs the server with graceful shutdown.
func main() {
	// Initialize unified logger
	// Note: We use os.Stderr here because logger might not be initialized yet
	if err := logger.InitDefault(); err != nil {
		// Use stderr directly since logger initialization failed
		os.Stderr.WriteString("FATAL: Failed to initialize logger: " + err.Error() + "\n")
		os.Exit(1)
	}
	defer func() {
		if err := logger.Sync(); err != nil {
			// Log sync errors are usually non-critical (e.g., on Windows)
			os.Stderr.WriteString("WARN: Failed to sync logger: " + err.Error() + "\n")
		}
	}()

	// Initialize controller-runtime logger (for K8s client compatibility)
	zapLogger := zap.New(zap.UseDevMode(true))
	ctrllog.SetLogger(zapLogger)

	// Load and validate configuration
	cfg := config.GetAppConfig()
	if errors := cfg.Validate(); len(errors) > 0 {
		for _, err := range errors {
			logger.Error("Configuration error", "error", err)
		}
		os.Exit(1)
	}

	// Validate build metadata (warn when ldflags are missing)
	if buildStatus := router.BuildInfoStatus(); buildStatus.Status != "ok" {
		logger.Warn("Build metadata not fully injected", "detail", buildStatus.Detail)
	} else {
		logger.Info("Build metadata loaded",
			"version", router.Version,
			"commit", router.Commit,
			"buildDate", router.BuildDate,
			"goVersion", router.GoVersion,
		)
	}

	// Print configuration in debug mode
	if cfg.Server.IsDebug() {
		cfg.PrintConfig()
	}

	// Initialize global cache
	_ = cache.GetGlobalCache()
	logger.Info("Cache initialized")

	// Setup router
	r := router.SetupRouter()

	// Log routes in debug mode
	if os.Getenv("GIN_LOG_ROUTES") == "1" || cfg.Server.IsDebug() {
		router.LogRoutes(r)
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:         cfg.Server.Address(),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("Starting server",
			"address", srv.Addr,
			"mode", cfg.Server.Mode,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrCh <- err
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	var shutdownReason string
	select {
	case sig := <-quit:
		shutdownReason = "signal:" + sig.String()
		logger.Info("Received shutdown signal", "signal", sig.String())
	case err := <-serverErrCh:
		shutdownReason = "server_error"
		logger.Error("Server failed", "error", err)
	}

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	// Graceful shutdown sequence
	logger.Info("Starting graceful shutdown...", "reason", shutdownReason)

	// 1. Stop accepting new requests
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	// 2. Close cache
	cache.GetGlobalCache().Stop()
	logger.Info("Cache stopped")

	// 3. Flush logs
	logger.Info("Server exited gracefully")
}
