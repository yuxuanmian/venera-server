package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SnapshotMetadata struct {
	Pointer     CatalogPointer `json:"catalog"`
	IndexSHA256 string         `json:"indexSha256"`
	SourceCount int            `json:"sourceCount"`
}

type Snapshot struct {
	Pointer  CatalogPointer
	Index    CatalogIndex
	IndexRaw []byte
	Metadata SnapshotMetadata
}

type Store struct {
	root string
}

func NewStore(dataDir string) *Store {
	return &Store{root: filepath.Join(dataDir, "catalog")}
}

func (s *Store) Root() string { return s.root }

func (s *Store) StatePath() string { return filepath.Join(s.root, "state.json") }

func (s *Store) SnapshotPath(pointer CatalogPointer) string {
	return filepath.Join(s.root, "snapshots", CatalogNamespace(pointer.CatalogID), pointer.Revision)
}

func (s *Store) SaveCheckedSnapshot(pointer CatalogPointer, indexRaw []byte, index CatalogIndex) error {
	if err := ValidatePointer(pointer); err != nil {
		return err
	}
	if len(indexRaw) > MaxIndexBytes {
		return errors.New("index exceeds maximum size")
	}
	if err := ValidateIndex(index); err != nil {
		return err
	}
	sum := sha256.Sum256(indexRaw)
	metadata := SnapshotMetadata{
		Pointer:     pointer,
		IndexSHA256: hex.EncodeToString(sum[:]),
		SourceCount: len(index),
	}
	dir := s.SnapshotPath(pointer)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	metadataRaw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot metadata: %w", err)
	}
	metadataRaw = append(metadataRaw, '\n')
	if err := atomicWriteFile(filepath.Join(dir, "index.json"), indexRaw); err != nil {
		return fmt.Errorf("write snapshot index: %w", err)
	}
	if err := atomicWriteFile(filepath.Join(dir, "metadata.json"), metadataRaw); err != nil {
		return fmt.Errorf("write snapshot metadata: %w", err)
	}
	return nil
}

func (s *Store) ReadSnapshot(pointer CatalogPointer) (*Snapshot, error) {
	if err := ValidatePointer(pointer); err != nil {
		return nil, err
	}
	dir := s.SnapshotPath(pointer)
	indexRaw, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return nil, fmt.Errorf("read snapshot index: %w", err)
	}
	metadataRaw, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return nil, fmt.Errorf("read snapshot metadata: %w", err)
	}
	var metadata SnapshotMetadata
	if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
		return nil, fmt.Errorf("decode snapshot metadata: %w", err)
	}
	if !SameIdentity(metadata.Pointer, pointer) || metadata.Pointer.IndexURL != pointer.IndexURL {
		return nil, errors.New("snapshot metadata identity mismatch")
	}
	if err := ValidatePointer(metadata.Pointer); err != nil {
		return nil, fmt.Errorf("snapshot metadata pointer: %w", err)
	}
	sum := sha256.Sum256(indexRaw)
	if !strings.EqualFold(metadata.IndexSHA256, hex.EncodeToString(sum[:])) {
		return nil, errors.New("snapshot index hash mismatch")
	}
	index, err := ParseIndex(indexRaw)
	if err != nil {
		return nil, fmt.Errorf("validate snapshot index: %w", err)
	}
	if metadata.SourceCount != len(index) {
		return nil, errors.New("snapshot source count mismatch")
	}
	return &Snapshot{Pointer: pointer, Index: index, IndexRaw: indexRaw, Metadata: metadata}, nil
}

func (s *Store) ReadState() (ServerCatalogState, error) {
	raw, err := os.ReadFile(s.StatePath())
	if err != nil {
		return ServerCatalogState{}, err
	}
	var state ServerCatalogState
	if err := json.Unmarshal(raw, &state); err != nil {
		return ServerCatalogState{}, fmt.Errorf("decode catalog state: %w", err)
	}
	if state.SchemaVersion != 1 {
		return ServerCatalogState{}, fmt.Errorf("unsupported catalog state schema %d", state.SchemaVersion)
	}
	if state.History == nil {
		state.History = []PublishedCatalog{}
	}
	if state.Active != nil {
		if err := validatePublished(*state.Active); err != nil {
			return ServerCatalogState{}, fmt.Errorf("validate active catalog: %w", err)
		}
	}
	for i, item := range state.History {
		if err := validatePublished(item); err != nil {
			return ServerCatalogState{}, fmt.Errorf("validate history[%d]: %w", i, err)
		}
	}
	return state, nil
}

func (s *Store) WriteState(state ServerCatalogState) error {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = 1
	}
	if state.SchemaVersion != 1 {
		return fmt.Errorf("unsupported catalog state schema %d", state.SchemaVersion)
	}
	if state.History == nil {
		state.History = []PublishedCatalog{}
	}
	if state.Active != nil {
		if err := validatePublished(*state.Active); err != nil {
			return err
		}
	}
	for _, item := range state.History {
		if err := validatePublished(item); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode catalog state: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return fmt.Errorf("create catalog directory: %w", err)
	}
	if err := atomicWriteFile(s.StatePath(), raw); err != nil {
		return fmt.Errorf("write catalog state: %w", err)
	}
	return nil
}

func validatePublished(item PublishedCatalog) error {
	if err := ValidatePointer(item.CatalogPointer); err != nil {
		return err
	}
	if item.ActivatedAt.IsZero() {
		return errors.New("activatedAt is required")
	}
	if item.SourceCount < 0 || item.SourceCount > MaxSources {
		return errors.New("invalid sourceCount")
	}
	return nil
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".catalog-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tmpName, path); err != nil {
		return err
	}
	return nil
}

// Used by tests to make timestamp construction explicit and deterministic.
func utcNow() time.Time { return time.Now().UTC() }
