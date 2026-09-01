package v2scan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

var (
	ErrDetailCapabilityUnavailable = errors.New("detail executor is unavailable for this source")
	ErrDetailBatchInvalid          = errors.New("comic detail batch is invalid")
)

type DetailRequest struct {
	ArtifactID       string
	PackageReleaseID string
	VisibilityScope  string
	VariantKey       string
	ContractID       string
	ComicID          string
	Reason           string
	Deadline         time.Time
	Payload          json.RawMessage
}

type DetailResult struct {
	ComicID   string
	Payload   json.RawMessage
	Digest    string
	Error     string
	Retryable bool
}

type DetailBatchResult struct {
	ArtifactID string
	Results    []DetailResult
}

type DetailScanner interface {
	ScanComic(context.Context, DetailRequest) (json.RawMessage, error)
}

type DetailScanFunc func(context.Context, DetailRequest) (json.RawMessage, error)

func (f DetailScanFunc) ScanComic(ctx context.Context, request DetailRequest) (json.RawMessage, error) {
	return f(ctx, request)
}

// DecodeDetailOutput validates the stable scanComic envelope and returns only
// the content observation approved for persistence.  The summary remains a
// source result concern and is not silently promoted into another table.
func DecodeDetailOutput(raw []byte, expectedComicID string) (DetailResult, error) {
	if len(raw) == 0 || expectedComicID == "" {
		return DetailResult{}, ErrDetailBatchInvalid
	}
	var wire detailOutputWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return DetailResult{}, ErrDetailBatchInvalid
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF || wire.ComicID != expectedComicID || wire.ContentObservation == nil || strings.TrimSpace(wire.ContentObservation.VisibilityScope) == "" || len(wire.SessionPatch) > 0 && string(wire.SessionPatch) != "null" {
		return DetailResult{}, ErrDetailBatchInvalid
	}
	encoded, err := json.Marshal(wire.ContentObservation)
	if err != nil {
		return DetailResult{}, ErrDetailBatchInvalid
	}
	canonical, err := canonicalJSON(encoded)
	if err != nil {
		return DetailResult{}, ErrDetailBatchInvalid
	}
	return DetailResult{ComicID: expectedComicID, Payload: canonical, Digest: digestJSON(canonical)}, nil
}

type detailOutputWire struct {
	ComicID            string                        `json:"comicId"`
	Summary            *detailSummaryWire            `json:"summary"`
	ContentObservation *detailContentObservationWire `json:"contentObservation"`
	SessionPatch       json.RawMessage               `json:"sessionPatch"`
}

type detailSummaryWire struct {
	Title          string            `json:"title"`
	Cover          *string           `json:"cover"`
	Subtitle       *string           `json:"subtitle"`
	ChapterCount   *int              `json:"chapterCount"`
	RecentChapters []json.RawMessage `json:"recentChapters"`
}

type detailContentObservationWire struct {
	VisibilityScope string                `json:"visibilityScope"`
	UpdateTime      *string               `json:"updateTime"`
	ChapterCount    *int                  `json:"chapterCount"`
	RecentChapters  []json.RawMessage     `json:"recentChapters"`
	MarkerEvidence  *detailMarkerEvidence `json:"markerEvidence"`
}

type detailMarkerEvidence struct {
	Channel string `json:"channel"`
	Scheme  string `json:"scheme"`
	Value   string `json:"value"`
}

type DetailExecutor struct {
	Capabilities map[string]DetailCapability
	MaxBatch     int
}

func NewDetailExecutor(capabilities []DetailCapability, maxBatch int) *DetailExecutor {
	if maxBatch <= 0 {
		maxBatch = 20
	}
	lookup := make(map[string]DetailCapability, len(capabilities)*2)
	for _, capability := range capabilities {
		if capability.ArtifactID == "" {
			continue
		}
		lookup[capability.ArtifactID+"\x00"+capability.ObservationContractID] = capability
		if capability.ObservationContractID == "" {
			lookup[capability.ArtifactID] = capability
		}
	}
	return &DetailExecutor{Capabilities: lookup, MaxBatch: maxBatch}
}

func (e *DetailExecutor) Enabled(artifactID, contractID string) bool {
	if e == nil {
		return false
	}
	capability, ok := e.Capabilities[artifactID+"\x00"+contractID]
	if !ok {
		capability, ok = e.Capabilities[artifactID]
	}
	return ok && capability.Enabled
}

func (e *DetailExecutor) ExecuteBatch(ctx context.Context, requests []DetailRequest, scanner DetailScanner) (DetailBatchResult, error) {
	if e == nil || scanner == nil {
		return DetailBatchResult{}, ErrDetailCapabilityUnavailable
	}
	if len(requests) == 0 || len(requests) > e.MaxBatch {
		return DetailBatchResult{}, ErrDetailBatchInvalid
	}
	artifactID := requests[0].ArtifactID
	seen := make(map[string]struct{}, len(requests))
	result := DetailBatchResult{ArtifactID: artifactID, Results: make([]DetailResult, 0, len(requests))}
	for _, request := range requests {
		if request.ArtifactID == "" || request.ArtifactID != artifactID || request.ComicID == "" || request.VisibilityScope == "" || request.ContractID == "" || !e.Enabled(request.ArtifactID, request.ContractID) {
			return DetailBatchResult{}, ErrDetailBatchInvalid
		}
		if _, ok := seen[request.ComicID]; ok {
			return DetailBatchResult{}, ErrDetailBatchInvalid
		}
		seen[request.ComicID] = struct{}{}
		payload, err := scanner.ScanComic(ctx, request)
		if err != nil {
			result.Results = append(result.Results, DetailResult{ComicID: request.ComicID, Error: safeDetailError(err), Retryable: true})
			continue
		}
		canonical, err := canonicalJSON(payload)
		if err != nil || len(canonical) == 0 {
			result.Results = append(result.Results, DetailResult{ComicID: request.ComicID, Error: "invalid_result"})
			continue
		}
		result.Results = append(result.Results, DetailResult{ComicID: request.ComicID, Payload: canonical, Digest: digestJSON(canonical)})
	}
	return result, nil
}

func ValidateDetailResult(result DetailResult) error {
	if result.ComicID == "" {
		return ErrDetailBatchInvalid
	}
	if result.Error != "" {
		return nil
	}
	if len(result.Payload) == 0 || !json.Valid(result.Payload) {
		return ErrDetailBatchInvalid
	}
	canonical, err := canonicalJSON(result.Payload)
	if err != nil || (result.Digest != "" && result.Digest != digestJSON(canonical)) {
		return ErrDetailBatchInvalid
	}
	return nil
}

func safeDetailError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > 128 {
		message = message[:128]
	}
	if strings.ContainsAny(message, "\r\n") {
		return "detail_error"
	}
	return message
}
