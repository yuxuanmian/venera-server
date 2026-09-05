package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"venera-server/internal/api"
	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/engine"
	"venera-server/internal/scheduler"
	"venera-server/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if cfg.DebugOpenAuth {
		log.Printf("WARNING: debug open auth enabled; /api and /admin authentication is disabled")
	}

	st, err := store.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if n, err := st.ResetRunningJobs(); err != nil {
		log.Fatalf("reset running jobs: %v", err)
	} else if n > 0 {
		log.Printf("recovered %d running job(s) to pending", n)
	}

	rec, err := debugrecorder.New(cfg.DataDir, cfg.DebugRecord)
	if err != nil {
		log.Fatalf("open debug recorder: %v", err)
	}
	defer rec.Close()

	eng, err := engine.New(cfg, st, rec)
	if err != nil {
		log.Fatalf("new engine: %v", err)
	}

	srv, err := api.NewServer(cfg, st, rec)
	if err != nil {
		log.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.StartTracking(ctx); err != nil {
		log.Printf("start tracking scanner: %v", err)
	}
	sched := scheduler.New(st, eng, 30*time.Second)
	sched.Configure(cfg.WorkerCount, cfg.RequestInterval, cfg.ChunkCooldown)
	go sched.Run(ctx)

	httpServer := &http.Server{
		Addr:    cfg.Addr,
		Handler: srv,
	}

	go func() {
		log.Printf("venera-server listening on %s", cfg.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down...")
	cancel()
	_ = srv.Close()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
