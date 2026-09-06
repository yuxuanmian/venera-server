package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type Manager struct {
	config CatalogConfig
	store  *Store
	client *Client

	operation sync.Mutex
	mu        sync.RWMutex
	active    *PublishedCatalog
	history   []PublishedCatalog
	candidate *CheckedCatalog
	lastError *ErrorInfo
	stateErr  error
	busy      bool
}

// CatalogConfig is kept in this package as the minimum dependency surface for
// embedders that do not use internal/config.
type CatalogConfig struct {
	ConfigFile string
	Addr       string
	DataDir    string
	CatalogURL string
}

func NewManager(config CatalogConfig, store *Store, client *Client) (*Manager, error) {
	if store == nil {
		return nil, errors.New("catalog store is required")
	}
	if client == nil {
		client = NewClient(nil)
	}
	if config.CatalogURL == "" {
		return nil, errors.New("catalog URL is required")
	}
	if _, err := ParseConfiguredURL(config.CatalogURL); err != nil {
		return nil, err
	}
	return &Manager{config: config, store: store, client: client}, nil
}

// Restore loads only local state. A corrupt state is retained as a diagnostic
// and returned to the caller, while the manager remains usable for /admin.
func (m *Manager) Restore(contexts ...context.Context) error {
	state, err := m.store.ReadState()
	if errors.Is(err, os.ErrNotExist) {
		m.mu.Lock()
		m.active, m.history, m.stateErr = nil, nil, nil
		m.mu.Unlock()
		return nil
	}
	if err != nil {
		m.mu.Lock()
		m.active, m.history, m.stateErr = nil, nil, err
		m.mu.Unlock()
		return err
	}
	if state.Active != nil {
		if _, err := m.store.ReadSnapshot(state.Active.CatalogPointer); err != nil {
			m.mu.Lock()
			m.active, m.history, m.stateErr = nil, nil, err
			m.mu.Unlock()
			return fmt.Errorf("active snapshot is invalid: %w", err)
		}
	}
	for _, item := range state.History {
		if _, err := m.store.ReadSnapshot(item.CatalogPointer); err != nil {
			m.mu.Lock()
			m.active, m.history, m.stateErr = nil, nil, err
			m.mu.Unlock()
			return fmt.Errorf("history snapshot is invalid: %w", err)
		}
	}
	m.mu.Lock()
	m.active = clonePublished(state.Active)
	m.history = append([]PublishedCatalog(nil), state.History...)
	m.stateErr = nil
	m.mu.Unlock()
	return nil
}

func (m *Manager) Check(ctx context.Context) (*CheckedCatalog, error) {
	if !m.operation.TryLock() {
		return nil, newOperationError("catalog_busy", "已有操作正在进行", nil)
	}
	m.setBusy(true)
	defer func() {
		m.setBusy(false)
		m.operation.Unlock()
	}()

	m.mu.Lock()
	m.candidate = nil
	m.lastError = nil
	m.mu.Unlock()

	checkCtx, cancel := context.WithTimeout(ctx, serverCheckTimeout)
	defer cancel()
	pointer, raw, index, err := m.client.FetchCatalog(checkCtx, m.config.CatalogURL)
	if err != nil {
		m.recordError(err)
		return nil, err
	}
	if err := m.store.SaveCheckedSnapshot(pointer, raw, index); err != nil {
		opErr := newOperationError("catalog_storage_failed", "保存漫画源配置失败", err)
		m.recordError(opErr)
		return nil, opErr
	}
	candidate := &CheckedCatalog{
		CatalogPointer: pointer,
		CheckedAt:      utcNow(),
		SourceCount:    len(index),
	}
	m.mu.Lock()
	m.candidate = cloneChecked(candidate)
	m.lastError = nil
	m.mu.Unlock()
	return candidate, nil
}

// Activate accepts catalogId and revision from the JSON API. It only reads a
// previously checked or successfully activated local snapshot.
func (m *Manager) Activate(ctx context.Context, identity ...string) (*PublishedCatalog, error) {
	if len(identity) != 2 {
		return nil, errors.New("activate requires catalogId and revision")
	}
	return m.ActivatePointer(ctx, CatalogPointer{CatalogID: identity[0], Revision: identity[1]})
}

func (m *Manager) ActivatePointer(ctx context.Context, requested CatalogPointer) (*PublishedCatalog, error) {
	if !m.operation.TryLock() {
		return nil, newOperationError("catalog_busy", "已有操作正在进行", nil)
	}
	m.setBusy(true)
	defer func() {
		m.setBusy(false)
		m.operation.Unlock()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	m.mu.RLock()
	active := clonePublished(m.active)
	candidate := cloneChecked(m.candidate)
	history := append([]PublishedCatalog(nil), m.history...)
	stateErr := m.stateErr
	m.mu.RUnlock()
	if stateErr != nil {
		return nil, newOperationError("catalog_state_invalid", "漫画源发布状态损坏", stateErr)
	}
	if candidate == nil || candidate.CatalogID != requested.CatalogID || candidate.Revision != requested.Revision {
		found := false
		for _, item := range history {
			if item.CatalogID == requested.CatalogID && item.Revision == requested.Revision {
				requested.IndexURL = item.IndexURL
				found = true
				break
			}
		}
		if !found {
			return nil, newOperationError("catalog_snapshot_not_found", "未找到可激活的漫画源配置快照", nil)
		}
	} else {
		requested.IndexURL = candidate.IndexURL
	}
	if active != nil && SameIdentity(active.CatalogPointer, requested) {
		return active, nil
	}
	if err := ValidatePointer(requested); err != nil {
		return nil, newOperationError("catalog_state_invalid", "漫画源发布状态损坏", err)
	}
	if _, err := m.store.ReadSnapshot(requested); err != nil {
		opErr := newOperationError("catalog_storage_failed", "读取漫画源配置快照失败", err)
		m.recordError(opErr)
		return nil, opErr
	}
	next := &PublishedCatalog{
		CatalogPointer: requested,
		ActivatedAt:    utcNow(),
	}
	if snapshot, err := m.store.ReadSnapshot(requested); err == nil {
		next.SourceCount = len(snapshot.Index)
	} else {
		return nil, newOperationError("catalog_storage_failed", "读取漫画源配置快照失败", err)
	}
	nextHistory := make([]PublishedCatalog, 0, len(history)+1)
	nextHistory = append(nextHistory, *next)
	for _, item := range history {
		if !SameIdentity(item.CatalogPointer, requested) {
			nextHistory = append(nextHistory, item)
		}
	}
	nextState := ServerCatalogState{SchemaVersion: 1, Active: next, History: nextHistory}
	if err := m.store.WriteState(nextState); err != nil {
		opErr := newOperationError("catalog_storage_failed", "保存漫画源发布状态失败", err)
		m.recordError(opErr)
		return nil, opErr
	}
	m.mu.Lock()
	m.active = clonePublished(next)
	m.history = append([]PublishedCatalog(nil), nextHistory...)
	m.candidate = nil
	m.lastError = nil
	m.stateErr = nil
	m.mu.Unlock()
	return next, nil
}

func (m *Manager) Authority() (CatalogPointer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.active == nil || m.stateErr != nil {
		return CatalogPointer{}, false
	}
	return m.active.CatalogPointer, true
}

func (m *Manager) StateError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stateErr
}

func (m *Manager) Status() CatalogStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ref, _ := ParseConfiguredURL(m.config.CatalogURL)
	status := CatalogStatus{
		ConfiguredSource: ConfiguredSource{
			CatalogURL: m.config.CatalogURL,
			CatalogID:  ref.Owner + "/" + ref.Repo,
			Ref:        ref.Ref,
		},
		Active:    clonePublished(m.active),
		Candidate: cloneChecked(m.candidate),
		History:   append([]PublishedCatalog(nil), m.history...),
		Busy:      m.busy,
		LastError: cloneError(m.lastError),
	}
	if m.stateErr != nil {
		status.LastError = &ErrorInfo{Code: "catalog_state_invalid", Message: "漫画源发布状态损坏"}
	}
	return status
}

func (m *Manager) setBusy(value bool) {
	m.mu.Lock()
	m.busy = value
	m.mu.Unlock()
}

func (m *Manager) recordError(err error) {
	var opErr *OperationError
	if errors.As(err, &opErr) {
		m.mu.Lock()
		m.lastError = &ErrorInfo{Code: opErr.Code, Message: opErr.Message}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	m.lastError = &ErrorInfo{Code: "catalog_error", Message: err.Error()}
	m.mu.Unlock()
}

func clonePublished(value *PublishedCatalog) *PublishedCatalog {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneChecked(value *CheckedCatalog) *CheckedCatalog {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneError(value *ErrorInfo) *ErrorInfo {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// Keep time imported in this file's public behavior explicit for consumers
// that inspect the manager's UTC timestamps.
var _ = time.UTC
