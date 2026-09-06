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

	"venera-server/internal/adminweb"
	"venera-server/internal/api"
	"venera-server/internal/catalog"
	"venera-server/internal/config"
)

func main() {
	cfg, err := config.LoadCatalog()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store := catalog.NewStore(cfg.DataDir)
	manager, err := catalog.NewManager(catalog.CatalogConfig{
		ConfigFile: cfg.ConfigFile,
		Addr:       cfg.Addr,
		DataDir:    cfg.DataDir,
		CatalogURL: cfg.CatalogURL,
	}, store, nil)
	if err != nil {
		log.Fatalf("new catalog manager: %v", err)
	}
	if err := manager.Restore(context.Background()); err != nil {
		log.Printf("catalog state unavailable; admin remains available: %v", err)
	}
	log.Printf("WARNING: catalog admin has no business authentication; keep the listener local or isolated")
	handler := api.NewCatalogHandler(manager, http.StripPrefix("/admin/", adminweb.NewHandler()))

	httpServer := &http.Server{
		Addr:    cfg.Addr,
		Handler: handler,
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
