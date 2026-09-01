package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"venera-server/internal/v2api"
	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2runtime"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
	"venera-server/internal/v2worker"
)

type workerPool struct {
	mu      sync.Mutex
	workers map[string][]*v2worker.Worker
	next    map[string]int
	closed  bool
}

func newWorkerPool(cfg v2config.Config, manifest v2manifest.Manifest) (*workerPool, error) {
	pool := &workerPool{workers: make(map[string][]*v2worker.Worker), next: make(map[string]int)}
	for _, pkg := range v2manifest.SortedPackages(manifest) {
		var scanning *v2manifest.ExtensionRef
		for index := range pkg.Extensions {
			extension := &pkg.Extensions[index]
			if extension.Runtime == "server" && extension.Capabilities["scanning"] == 1 {
				scanning = extension
				break
			}
		}
		if scanning == nil {
			continue
		}
		if err := v2manifest.ValidatePackageFiles(cfg.ManifestCacheDir, pkg); err != nil {
			return nil, fmt.Errorf("validate %s worker artifacts: %w", pkg.ArtifactID, err)
		}
		core, err := os.ReadFile(filepath.Join(cfg.ManifestCacheDir, filepath.FromSlash(pkg.Core.Path)))
		if err != nil {
			return nil, fmt.Errorf("load %s core: %w", pkg.ArtifactID, err)
		}
		extension, err := os.ReadFile(filepath.Join(cfg.ManifestCacheDir, filepath.FromSlash(scanning.Path)))
		if err != nil {
			return nil, fmt.Errorf("load %s scanning extension: %w", pkg.ArtifactID, err)
		}
		workerConfig := v2worker.WorkerConfig{
			CoreScript: core, ExtensionScript: extension,
			AllowedOrigins:          pkg.SessionExportProfile.AllowedOrigins,
			MaxOutputBytes:          pkg.ScanPolicy.Limits.MaxOutputBytes,
			MaxResponseBytes:        pkg.ScanPolicy.Limits.MaxResponseBytes,
			MaxItems:                pkg.ScanPolicy.Limits.MaxSnapshotItems,
			DefaultOperationTimeout: cfg.WorkerOperationTimeout,
			MinRequestStartInterval: time.Duration(pkg.ScanPolicy.Limits.RequestIntervalFloorMS) * time.Millisecond,
		}
		for index := 0; index < cfg.WorkerCount; index++ {
			worker, err := v2worker.NewWorker(workerConfig)
			if err != nil {
				return nil, fmt.Errorf("create %s worker: %w", pkg.ArtifactID, err)
			}
			pool.workers[pkg.ArtifactID] = append(pool.workers[pkg.ArtifactID], worker)
		}
	}
	if len(pool.workers) == 0 {
		return nil, errors.New("manifest has no server scanning worker")
	}
	return pool, nil
}

func (pool *workerPool) WorkerForArtifact(artifactID string) (*v2worker.Worker, error) {
	if pool == nil || artifactID == "" {
		return nil, errors.New("scanning worker artifact is required")
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	workers := pool.workers[artifactID]
	if pool.closed || len(workers) == 0 {
		return nil, errors.New("scanning worker is unavailable for artifact")
	}
	index := pool.next[artifactID] % len(workers)
	pool.next[artifactID] = (index + 1) % len(workers)
	return workers[index], nil
}

func (pool *workerPool) Close() error {
	if pool == nil {
		return nil
	}
	pool.mu.Lock()
	pool.closed = true
	pool.mu.Unlock()
	return nil
}

type serverV2Runtime struct {
	cfg        v2config.Config
	keys       v2crypto.KeySet
	db         *v2store.DB
	repo       *v2store.Repository
	manifest   v2manifest.Manifest
	manager    *v2manifest.Manager
	workers    *workerPool
	planner    *v2scan.Planner
	dispatcher *v2scan.Dispatcher
	resident   *v2runtime.Runtime
	router     *v2api.Router
	httpServer *http.Server
}

func initializeRuntime(ctx context.Context, cfg v2config.Config) (*serverV2Runtime, error) {
	// The construction order is part of the v2 startup contract:
	// config/keys -> DB/migrations -> manifest LKG -> worker pool -> planner/
	// dispatcher -> HTTP.  Each later failure closes all earlier resources.
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("load v2 config: %w", err)
	}
	keys, err := v2crypto.DeriveKeys(cfg.RootSecret)
	if err != nil {
		return nil, fmt.Errorf("derive v2 keys: %w", err)
	}
	db, err := v2store.Open(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open v2 database: %w", err)
	}
	runtime := &serverV2Runtime{cfg: cfg, keys: keys, db: db, repo: v2store.NewRepository(db, keys)}
	cleanup := true
	defer func() {
		if cleanup {
			_ = runtime.Close(context.Background())
		}
	}()

	manager := v2manifest.NewManager(cfg, runtime.repo, nil)
	manifest, err := loadManifestLKG(ctx, cfg, manager)
	if err != nil {
		return nil, fmt.Errorf("load manifest LKG: %w", err)
	}
	runtime.manager, runtime.manifest = manager, manifest
	workers, err := newWorkerPool(cfg, manifest)
	if err != nil {
		return nil, err
	}
	runtime.workers = workers
	demandRepo := v2scan.NewDemandRepository(runtime.repo)
	runtime.planner = v2scan.NewPlanner(runtime.repo, v2scan.PlannerOptions{})
	runtime.dispatcher = v2scan.NewDispatcher(demandRepo, v2scan.DispatcherOptions{
		MaxConcurrent: cfg.WorkerCount, LeaseDuration: cfg.WorkerOperationTimeout, WorkerID: "server-v2",
	})
	for _, pkg := range v2manifest.SortedPackages(manifest) {
		runtime.dispatcher.AddArtifact(pkg.ArtifactID)
	}
	packages := make([]v2runtime.PackageRuntime, 0, len(manifest.SourcePackages))
	for _, pkg := range v2manifest.SortedPackages(manifest) {
		var scanningHash string
		for _, extension := range pkg.Extensions {
			if extension.Runtime == "server" && extension.Capabilities["scanning"] == 1 {
				scanningHash = extension.SHA256
				break
			}
		}
		packages = append(packages, v2runtime.PackageRuntime{
			ArtifactID: pkg.ArtifactID, PackageReleaseID: pkg.PackageReleaseID,
			CoreHash: pkg.Core.SHA256, ScanningExtensionHash: scanningHash,
			ObservationContractID:        pkg.Tracking.ObservationContractID,
			AccountObservationContractID: pkg.Tracking.AccountObservationContractID,
			MarkerSchemes:                append([]string(nil), pkg.Tracking.MarkerSchemes...),
			AccountProbeContract:         pkg.AccountProbeContract, SessionExportProfile: pkg.SessionExportProfile,
			ScanPolicy: pkg.ScanPolicy, DetailEnabled: true,
		})
	}
	resident, err := v2runtime.New(runtime.repo, v2runtime.Options{
		Clock: runtime.db.Now, WorkerProvider: v2runtime.WorkerProviderFunc(func(artifactID string) (v2runtime.Worker, error) {
			return workers.WorkerForArtifact(artifactID)
		}), Packages: packages, Planner: runtime.planner, DemandRepository: demandRepo,
		Dispatcher: runtime.dispatcher, WorkerCount: cfg.WorkerCount, WorkerOperationTimeout: cfg.WorkerOperationTimeout,
		DetailCapabilities: nil,
	})
	if err != nil {
		return nil, fmt.Errorf("construct v2 resident runtime: %w", err)
	}
	if err := resident.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("initial v2 plan: %w", err)
	}
	runtime.resident = resident
	router := v2api.NewServer(cfg, runtime.repo)
	router.SetAccountProbeRunner(&artifactProbeRunner{pool: workers})
	router.SetRuntimeWake(resident.Wake)
	runtime.router = router
	runtime.httpServer = &http.Server{Addr: serverAddress(), Handler: router}
	cleanup = false
	return runtime, nil
}

func loadManifestLKG(ctx context.Context, cfg v2config.Config, manager *v2manifest.Manager) (v2manifest.Manifest, error) {
	path := filepath.Join(cfg.ManifestCacheDir, "index-v2.json")
	body, err := os.ReadFile(path)
	if err != nil {
		return v2manifest.Manifest{}, err
	}
	return manager.ValidateAndActivate(ctx, body)
}

func serverAddress() string {
	if value := os.Getenv("VENERA_V2_ADDR"); value != "" {
		return value
	}
	return "127.0.0.1:8080"
}

func (runtime *serverV2Runtime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var firstErr error
	if runtime.httpServer != nil {
		if err := runtime.httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			firstErr = err
		}
	}
	if runtime.resident != nil {
		if err := runtime.resident.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if runtime.workers != nil {
		if err := runtime.workers.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if runtime.db != nil {
		if err := runtime.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func runServer(ctx context.Context, runtime *serverV2Runtime) error {
	if runtime == nil || runtime.httpServer == nil {
		return errors.New("v2 runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	serverErr := make(chan error, 1)
	runtimeErr := make(chan error, 1)
	go func() {
		err := runtime.httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverErr <- err
	}()
	go func() {
		runtimeErr <- runtime.resident.Start(ctx)
	}()
	select {
	case err := <-serverErr:
		_ = runtime.resident.Close(context.Background())
		return err
	case err := <-runtimeErr:
		if err != nil {
			_ = runtime.httpServer.Shutdown(context.Background())
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := runtime.Close(shutdownContext)
		if shutdownErr != nil {
			return shutdownErr
		}
		return nil
	}
}

type artifactProbeRunner struct {
	pool *workerPool
}

func (runner *artifactProbeRunner) Probe(ctx context.Context, request v2scan.ProbeRequest) (v2scan.ProbeResult, error) {
	if runner == nil || runner.pool == nil {
		return v2scan.ProbeResult{}, &v2scan.ProbeError{Code: "extension_failure"}
	}
	worker, err := runner.pool.WorkerForArtifact(request.ArtifactID)
	if err != nil {
		return v2scan.ProbeResult{}, &v2scan.ProbeError{Code: "extension_failure", Cause: err}
	}
	probe := v2scan.NewAccountProbeRunner(worker)
	probe.Clock = time.Now
	return probe.Probe(ctx, request)
}

func detailEnabledFromPackageJSON(packageJSON string) bool {
	var value struct {
		DetailEnabled bool `json:"detailEnabled"`
		Scanning      struct {
			Operations []string `json:"operations"`
		} `json:"scanning"`
	}
	if json.Unmarshal([]byte(packageJSON), &value) == nil && value.DetailEnabled {
		return true
	}
	for _, operation := range value.Scanning.Operations {
		if operation == "scanComic" {
			return true
		}
	}
	return false
}

func main() {
	cfg, err := v2config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "v2 configuration is invalid: %v\n", err)
		return
	}
	if cfg.DevOpenEnrollment {
		fmt.Fprintln(os.Stderr, "WARNING: development open enrollment is enabled; any client that can reach this Server may register")
	}
	runtime, err := initializeRuntime(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "v2 server startup failed: %v\n", err)
		return
	}
	defer runtime.Close(context.Background())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	serverContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-stop
		cancel()
	}()
	if err := runServer(serverContext, runtime); err != nil {
		fmt.Fprintln(os.Stderr, "v2 server stopped with an error")
	}
}
