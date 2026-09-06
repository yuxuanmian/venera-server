package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	RawHost       = "raw.githubusercontent.com"
	GitHubAPIHost = "api.github.com"
	MaxIndexBytes = 2 << 20
	MaxSources    = 256
)

var (
	shaPattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	keyPattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	fileNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*\.js$`)
	pathPartPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

type CatalogPointer struct {
	CatalogID string `json:"catalogId"`
	Revision  string `json:"revision"`
	IndexURL  string `json:"indexUrl"`
}

type CatalogRef struct {
	Owner string
	Repo  string
	Ref   string
}

type SourceEntry struct {
	Name        string `json:"name"`
	FileName    string `json:"fileName"`
	Key         string `json:"key"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type CatalogIndex []SourceEntry

type CheckedCatalog struct {
	CatalogPointer
	CheckedAt   time.Time `json:"checkedAt"`
	SourceCount int       `json:"sourceCount"`
}

type PublishedCatalog struct {
	CatalogPointer
	ActivatedAt time.Time `json:"activatedAt"`
	SourceCount int       `json:"sourceCount"`
}

type ServerCatalogState struct {
	SchemaVersion int                `json:"schemaVersion"`
	Active        *PublishedCatalog  `json:"active"`
	History       []PublishedCatalog `json:"history"`
}

type ConfiguredSource struct {
	CatalogURL string `json:"catalogUrl"`
	CatalogID  string `json:"catalogId"`
	Ref        string `json:"ref"`
}

type CatalogStatus struct {
	ConfiguredSource ConfiguredSource   `json:"configuredSource"`
	Active           *PublishedCatalog  `json:"active"`
	Candidate        *CheckedCatalog    `json:"candidate"`
	History          []PublishedCatalog `json:"history"`
	Busy             bool               `json:"busy"`
	LastError        *ErrorInfo         `json:"lastError"`
}

type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type OperationError struct {
	Code    string
	Message string
	Err     error
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Code + ": " + e.Message
	}
	return e.Code + ": " + e.Message + ": " + e.Err.Error()
}

func (e *OperationError) Unwrap() error { return e.Err }

func newOperationError(code, message string, err error) error {
	return &OperationError{Code: code, Message: message, Err: err}
}

func ParseConfiguredURL(raw string) (CatalogRef, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil {
		return CatalogRef{}, fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "https" || u.Host != RawHost || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.Fragment != "" {
		return CatalogRef{}, errors.New("catalog URL must be an https raw.githubusercontent.com URL without credentials, port, query, or fragment")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 4 || parts[3] != "index.json" || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return CatalogRef{}, errors.New("catalog URL must have the form owner/repo/ref/index.json")
	}
	for _, part := range parts[:3] {
		if !pathPartPattern.MatchString(part) || part == "." || part == ".." {
			return CatalogRef{}, errors.New("catalog URL contains an unsafe path segment")
		}
	}
	return CatalogRef{Owner: parts[0], Repo: parts[1], Ref: parts[2]}, nil
}

func BuildPinnedPointer(ref CatalogRef, revision string) (CatalogPointer, error) {
	if !shaPattern.MatchString(revision) {
		return CatalogPointer{}, errors.New("revision must be a 40-character lowercase SHA-1")
	}
	if !pathPartPattern.MatchString(ref.Owner) || !pathPartPattern.MatchString(ref.Repo) {
		return CatalogPointer{}, errors.New("catalog owner and repository are invalid")
	}
	catalogID := strings.ToLower(ref.Owner + "/" + ref.Repo)
	return CatalogPointer{
		CatalogID: catalogID,
		Revision:  revision,
		IndexURL:  "https://" + RawHost + "/" + ref.Owner + "/" + ref.Repo + "/" + revision + "/index.json",
	}, nil
}

func ValidatePointer(pointer CatalogPointer) error {
	if pointer.CatalogID == "" || pointer.Revision == "" || pointer.IndexURL == "" {
		return errors.New("catalog pointer requires catalogId, revision, and indexUrl")
	}
	if pointer.CatalogID != strings.ToLower(pointer.CatalogID) {
		return errors.New("catalogId must be lowercase")
	}
	parts := strings.Split(pointer.CatalogID, "/")
	if len(parts) != 2 {
		return errors.New("catalogId must be owner/repo")
	}
	p, err := ParseConfiguredURL(pointer.IndexURL)
	if err != nil {
		return err
	}
	if p.Owner != parts[0] || p.Repo != parts[1] || p.Ref != pointer.Revision {
		return errors.New("indexUrl does not match catalog identity")
	}
	if !shaPattern.MatchString(pointer.Revision) {
		return errors.New("revision must be a 40-character lowercase SHA-1")
	}
	return nil
}

func PinnedSourceURL(pointer CatalogPointer, fileName string) (string, error) {
	if err := ValidatePointer(pointer); err != nil {
		return "", err
	}
	if err := validateFileName(fileName); err != nil {
		return "", err
	}
	parts := strings.Split(pointer.CatalogID, "/")
	return "https://" + RawHost + "/" + parts[0] + "/" + parts[1] + "/" + pointer.Revision + "/" + fileName, nil
}

func ParseIndex(data []byte) (CatalogIndex, error) {
	if len(data) > MaxIndexBytes {
		return nil, errors.New("index exceeds maximum size")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var raw []json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("index must be a JSON array: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("index contains trailing JSON")
		}
		return nil, fmt.Errorf("index contains invalid trailing JSON: %w", err)
	}
	if len(raw) > MaxSources {
		return nil, fmt.Errorf("index contains more than %d sources", MaxSources)
	}
	index := make(CatalogIndex, 0, len(raw))
	for i, item := range raw {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(item, &fields); err != nil {
			return nil, fmt.Errorf("index entry %d must be an object: %w", i, err)
		}
		entry, err := parseEntry(fields)
		if err != nil {
			return nil, fmt.Errorf("index entry %d: %w", i, err)
		}
		index = append(index, entry)
	}
	if err := ValidateIndex(index); err != nil {
		return nil, err
	}
	return index, nil
}

func ValidateIndex(index CatalogIndex) error {
	if len(index) > MaxSources {
		return fmt.Errorf("index contains more than %d sources", MaxSources)
	}
	keys := make(map[string]struct{}, len(index))
	files := make(map[string]struct{}, len(index))
	for i, entry := range index {
		if entry.Name == "" || entry.Version == "" {
			return fmt.Errorf("index entry %d has empty name or version", i)
		}
		if !keyPattern.MatchString(entry.Key) {
			return fmt.Errorf("index entry %d has invalid key %q", i, entry.Key)
		}
		if err := validateFileName(entry.FileName); err != nil {
			return fmt.Errorf("index entry %d: %w", i, err)
		}
		if _, ok := keys[entry.Key]; ok {
			return fmt.Errorf("duplicate source key %q", entry.Key)
		}
		keys[entry.Key] = struct{}{}
		fileKey := strings.ToLower(entry.FileName)
		if _, ok := files[fileKey]; ok {
			return fmt.Errorf("duplicate source filename ignoring case %q", entry.FileName)
		}
		files[fileKey] = struct{}{}
	}
	return nil
}

func parseEntry(fields map[string]json.RawMessage) (SourceEntry, error) {
	readString := func(name string, required bool) (string, error) {
		raw, ok := fields[name]
		if !ok {
			if required {
				return "", fmt.Errorf("missing %s", name)
			}
			return "", nil
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || value == "" {
			return "", fmt.Errorf("%s must be a non-empty string", name)
		}
		return value, nil
	}
	name, err := readString("name", true)
	if err != nil {
		return SourceEntry{}, err
	}
	fileName, err := readString("fileName", true)
	if err != nil {
		return SourceEntry{}, err
	}
	key, err := readString("key", true)
	if err != nil {
		return SourceEntry{}, err
	}
	version, err := readString("version", true)
	if err != nil {
		return SourceEntry{}, err
	}
	description, err := readString("description", false)
	if err != nil {
		return SourceEntry{}, err
	}
	return SourceEntry{Name: name, FileName: fileName, Key: key, Version: version, Description: description}, nil
}

func validateFileName(fileName string) error {
	if fileName == "" || strings.ContainsAny(fileName, `/\\`) || strings.ContainsRune(fileName, 0) || !fileNamePattern.MatchString(fileName) {
		return fmt.Errorf("unsafe source filename %q", fileName)
	}
	for _, part := range strings.Split(fileName, "/") {
		if part == ".." {
			return fmt.Errorf("source filename contains path traversal")
		}
	}
	for _, r := range fileName {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("source filename contains control characters")
		}
	}
	return nil
}

func CatalogNamespace(catalogID string) string {
	sum := sha256.Sum256([]byte(catalogID))
	return hex.EncodeToString(sum[:])
}

func SameIdentity(a, b CatalogPointer) bool {
	return a.CatalogID == b.CatalogID && a.Revision == b.Revision
}

func (p CatalogPointer) Valid() error { return ValidatePointer(p) }

func (p CatalogPointer) Identity() string { return p.CatalogID + "@" + p.Revision }
