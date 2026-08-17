package scheduler

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"venera-server/internal/engine"
	"venera-server/internal/store"
)

const chunkSize = 30
const defaultWorkerCount = 5
const defaultRequestInterval = 2 * time.Second
const defaultChunkCooldown = 10 * time.Second

type Scheduler struct {
	store       *store.Store
	engine      *engine.Engine
	interval    time.Duration
	batch       int
	workerCount int

	requestInterval time.Duration
	chunkCooldown   time.Duration

	mu              sync.Mutex
	busy            map[string]bool
	nextAllowed     map[string]time.Time
	lastChunk       map[string]int64
	workers         chan store.Job
	dispatcherCtx   context.Context
	cleanupInterval time.Duration
	lastCleanup     time.Time
}

func New(st *store.Store, eng *engine.Engine, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Scheduler{
		store:           st,
		engine:          eng,
		interval:        interval,
		batch:           20,
		workerCount:     defaultWorkerCount,
		requestInterval: defaultRequestInterval,
		chunkCooldown:   defaultChunkCooldown,
		busy:            map[string]bool{},
		nextAllowed:     map[string]time.Time{},
		lastChunk:       map[string]int64{},
		cleanupInterval: 10 * time.Minute,
	}
}

func (s *Scheduler) Configure(workerCount int, requestInterval, chunkCooldown time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if workerCount > 0 {
		s.workerCount = workerCount
	}
	if requestInterval > 0 {
		s.requestInterval = requestInterval
	}
	if chunkCooldown > 0 {
		s.chunkCooldown = chunkCooldown
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	log.Printf("scheduler started (interval=%s workers=%d)", s.interval, s.workerCount)

	s.workers = make(chan store.Job, s.workerCount)
	for i := 0; i < s.workerCount; i++ {
		go s.worker(ctx)
	}
	go s.dispatcher(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler stopped")
			return
		case <-ticker.C:
			if err := ScheduleDue(s.store); err != nil {
				log.Printf("scheduler error: %v", err)
			}
			s.maybeCleanup()
		}
	}
}

func (s *Scheduler) dispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			s.dispatchOnce(ctx)
		}
	}
}

func (s *Scheduler) dispatchOnce(ctx context.Context) {
	jobs, err := s.store.GetPendingJobs(s.batch)
	if err != nil {
		log.Printf("dispatcher: get pending: %v", err)
		return
	}
	now := time.Now()
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := job.Source
		s.mu.Lock()
		if s.busy[key] {
			s.mu.Unlock()
			continue
		}
		if allowed, ok := s.nextAllowed[key]; ok && now.Before(allowed) {
			s.mu.Unlock()
			continue
		}
		s.busy[key] = true
		s.mu.Unlock()

		select {
		case s.workers <- job:
		default:
			// workers full: release and stop trying.
			s.mu.Lock()
			s.busy[key] = false
			s.mu.Unlock()
			return
		}
	}
}

func (s *Scheduler) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.workers:
			if err := s.engine.RunJob(ctx, job); err != nil {
				log.Printf("job %d failed: %v", job.JobID, err)
			}
			s.releaseSource(job)
		}
	}
}

func (s *Scheduler) releaseSource(job store.Job) {
	key := job.Source
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.busy[key] = false

	next := now.Add(s.requestInterval)

	var chunkID int64
	chunkValid := job.ChunkID.Valid
	if chunkValid {
		chunkID = job.ChunkID.Int64
	}
	if last, ok := s.lastChunk[key]; ok && last != chunkID {
		if c := now.Add(s.chunkCooldown); c.After(next) {
			next = c
		}
	}
	s.lastChunk[key] = chunkID
	if cur, ok := s.nextAllowed[key]; ok && cur.After(next) {
		next = cur
	}
	s.nextAllowed[key] = next
}

func (s *Scheduler) maybeCleanup() {
	s.mu.Lock()
	if time.Since(s.lastCleanup) < s.cleanupInterval {
		s.mu.Unlock()
		return
	}
	s.lastCleanup = time.Now()
	s.mu.Unlock()
	if err := s.store.CleanupResults(7 * 24 * time.Hour); err != nil {
		log.Printf("cleanup results: %v", err)
	}
}

// ProcessPending is the sequential fallback used by tests and small batches.
func (s *Scheduler) ProcessPending(ctx context.Context) error {
	jobs, err := s.store.GetPendingJobs(s.batch)
	if err != nil {
		return err
	}
	processed := 0
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := s.engine.RunJob(ctx, job); err != nil {
			log.Printf("job %d failed: %v", job.JobID, err)
		}
		processed++
	}
	if processed > 0 {
		log.Printf("scheduler processed %d jobs", processed)
	}
	return nil
}

// ScheduleDue scans due mirror entries and creates pending chunks/jobs.
func ScheduleDue(st *store.Store) error {
	now := time.Now().UTC().Format(time.RFC3339)
	mirrors, err := st.ListDueMirror(now)
	if err != nil {
		return err
	}
	if len(mirrors) == 0 {
		return nil
	}

	type group struct {
		userID  string
		source  string
		mirrors []store.Mirror
	}
	groups := map[string]*group{}
	var order []string
	for _, m := range mirrors {
		key := m.UserID + "|" + m.Source
		g := groups[key]
		if g == nil {
			g = &group{userID: m.UserID, source: m.Source}
			groups[key] = g
			order = append(order, key)
		}
		g.mirrors = append(g.mirrors, m)
	}

	createdJobs := 0
	for _, key := range order {
		g := groups[key]
		var chunkID sql.NullInt64
		count := 0
		for _, m := range g.mirrors {
			active, err := st.HasActiveJob(m.UserID, m.Source, m.ComicID)
			if err != nil {
				return err
			}
			if active {
				continue
			}
			if count%chunkSize == 0 {
				id, err := st.InsertChunk(g.userID, g.source)
				if err != nil {
					return err
				}
				chunkID = sql.NullInt64{Int64: id, Valid: true}
			}
			if err := st.InsertJob(store.Job{
				UserID:      m.UserID,
				Source:      m.Source,
				ComicID:     m.ComicID,
				ChunkID:     chunkID,
				State:       "pending",
				Priority:    m.Priority,
				DueAt:       sql.NullString{String: now, Valid: true},
				ScheduledAt: sql.NullString{String: now, Valid: true},
				CreatedAt:   now,
			}); err != nil {
				return err
			}
			count++
			createdJobs++
		}
	}
	log.Printf("scheduler tick: %d due, %d jobs created", len(mirrors), createdJobs)
	return nil
}
