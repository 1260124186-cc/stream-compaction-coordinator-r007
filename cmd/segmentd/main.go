package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/config"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/runtimecheck"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "segmentd:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return usageError()
	}
	switch arguments[0] {
	case "serve":
		if len(arguments) != 1 {
			return usageError()
		}
		return serve()
	case "verify":
		if len(arguments) != 2 {
			return usageError()
		}
		return verify(arguments[1])
	case "version":
		fmt.Println("segmentd 1.0.0")
		return nil
	default:
		return usageError()
	}
}

func serve() error {
	settings, err := config.FromEnvironment()
	if err != nil {
		return err
	}
	logger := observability.NewJSONLogger(os.Stderr)
	repository, err := storage.NewFileRepository(settings.StatePath)
	if err != nil {
		return err
	}
	service, err := application.NewService(
		repository,
		&idgen.Random{},
		clock.System{},
		logger,
		application.Limits{
			MaxAppendRecords: uint64(settings.MaxAppendRecords),
			MaxAppendBytes:   settings.MaxAppendBytes,
		},
	)
	if err != nil {
		return err
	}
	api := httpapi.New(service, httpapi.Options{
		MaxRequestBytes: settings.MaxRequestBytes,
		RequestTimeout:  settings.RequestTimeout,
		Logger:          logger,
	})
	server := &http.Server{
		Addr:              settings.Address,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	signalContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	result := make(chan error, 1)
	go func() {
		logger.Info("server.started", map[string]any{
			"address":    settings.Address,
			"state_path": repository.Path(),
		})
		result <- server.ListenAndServe()
	}()

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-signalContext.Done():
		shutdownContext, cancel := context.WithTimeout(
			context.Background(),
			settings.ShutdownTimeout,
		)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		err := <-result
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		logger.Info("server.stopped", map[string]any{
			"state_path": repository.Path(),
		})
		return nil
	}
}

func verify(workflow string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := runtimecheck.Run(ctx, workflow)
	if err != nil {
		return err
	}
	fmt.Printf(
		"workflow=%s assertions=%d status=ok\n",
		result.Workflow,
		result.Assertions,
	)
	return nil
}

func usageError() error {
	return fmt.Errorf(
		"usage: segmentd serve | segmentd verify <append-rollover|compaction-lineage|compaction-recovery|replica-watermark> | segmentd version",
	)
}
