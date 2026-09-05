package scan

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"venera-server/internal/tracking/domain"
	"venera-server/internal/tracking/worker"
)

const ManwaObservationFreshness = 24 * time.Hour

type SnapshotRequest struct {
	RequestID         string
	Artifact          domain.ArtifactIdentity
	Revision          string
	RuntimeGeneration int64
	Account           map[string]any
	Checkpoint        json.RawMessage
	Budget            worker.OperationBudget
	SessionCookies    []http.Cookie
}

type SnapshotExecutionResult struct {
	ExpectedTotal *int
	Items         []SnapshotItem
	Observations  []domain.Observation
	Complete      bool
	Checkpoint    json.RawMessage
	RequestCount  int
	ResponseBytes int64
}

// SnapshotExecutor runs one bounded scanner slice and turns it into
// revision-owned observations. It never compares content and never publishes
// an incomplete slice as a complete snapshot.
type SnapshotExecutor struct {
	Worker        *worker.Worker
	Clock         func() time.Time
	MaxItems      int
	MaxCheckpoint int
	Freshness     time.Duration
}

func NewSnapshotExecutor(runtimeWorker *worker.Worker) *SnapshotExecutor {
	return &SnapshotExecutor{
		Worker:        runtimeWorker,
		Clock:         time.Now,
		MaxItems:      DefaultSnapshotMaxItems,
		MaxCheckpoint: DefaultSnapshotMaxCheckpoint,
		Freshness:     ManwaObservationFreshness,
	}
}

func (executor *SnapshotExecutor) Run(ctx context.Context, request SnapshotRequest) (SnapshotExecutionResult, error) {
	if executor == nil || executor.Worker == nil || request.RequestID == "" {
		return SnapshotExecutionResult{}, ErrSnapshotInvalidSlice
	}
	if err := request.Artifact.Validate(); err != nil || !domain.FullRevision(request.Revision) || request.RuntimeGeneration < 1 {
		return SnapshotExecutionResult{}, ErrSnapshotInvalidSlice
	}
	if ctx == nil {
		ctx = context.Background()
	}
	maxItems := executor.MaxItems
	if maxItems <= 0 {
		maxItems = DefaultSnapshotMaxItems
	}
	maxCheckpoint := executor.MaxCheckpoint
	if maxCheckpoint <= 0 {
		maxCheckpoint = DefaultSnapshotMaxCheckpoint
	}
	if len(request.Checkpoint) > 0 && string(request.Checkpoint) != "null" {
		if err := ValidateCheckpoint(request.Checkpoint, maxCheckpoint); err != nil {
			return SnapshotExecutionResult{}, err
		}
	}
	maxRequests := request.Budget.MaxRequests
	if maxRequests <= 0 {
		maxRequests = 4
	}
	maxItemsBudget := request.Budget.MaxItems
	if maxItemsBudget <= 0 {
		maxItemsBudget = maxItems
	}
	input := map[string]any{
		"account":    request.Account,
		"checkpoint": nil,
		"budget": map[string]any{
			"maxRequests": maxRequests,
			"maxItems":    maxItemsBudget,
		},
	}
	if len(request.Checkpoint) > 0 && string(request.Checkpoint) != "null" {
		var checkpoint any
		if err := json.Unmarshal(request.Checkpoint, &checkpoint); err != nil {
			return SnapshotExecutionResult{}, ErrSnapshotInvalidSlice
		}
		input["checkpoint"] = checkpoint
	}
	if !request.Budget.DeadlineAt.IsZero() {
		input["budget"].(map[string]any)["deadlineAt"] = request.Budget.DeadlineAt.UTC().Format(time.RFC3339Nano)
	}
	workerResult, err := executor.Worker.Run(ctx, worker.RunRequest{
		RequestID:         request.RequestID,
		Operation:         worker.OperationScanFavoriteSnapshotSlice,
		Input:             input,
		ArtifactID:        request.Artifact.SourceKey,
		FileName:          request.Artifact.FileName,
		Revision:          request.Revision,
		RuntimeGeneration: request.RuntimeGeneration,
		Budget: worker.OperationBudget{
			MaxRequests: maxRequests,
			MaxItems:    maxItemsBudget,
			DeadlineAt:  request.Budget.DeadlineAt,
		},
		SessionCookies: append([]http.Cookie(nil), request.SessionCookies...),
	})
	if err != nil {
		return SnapshotExecutionResult{}, err
	}
	slice, err := DecodeSnapshotSliceWithLimits(workerResult.Output, maxItems, maxCheckpoint)
	if err != nil {
		return SnapshotExecutionResult{}, err
	}
	clock := executor.Clock
	if clock == nil {
		clock = time.Now
	}
	observedAt := clock().UTC()
	freshness := executor.Freshness
	if freshness <= 0 {
		freshness = ManwaObservationFreshness
	}
	observations := make([]domain.Observation, 0, len(slice.Items))
	for _, item := range slice.Items {
		observations = append(observations, domain.Observation{
			Artifact:          request.Artifact,
			Revision:          request.Revision,
			ComicID:           item.ComicID,
			ObservedAt:        observedAt,
			ValidUntil:        observedAt.Add(freshness),
			FavoriteUpdate:    item.FavoriteUpdate,
			RuntimeGeneration: request.RuntimeGeneration,
			PayloadDigest:     item.ItemDigest,
		})
	}
	return SnapshotExecutionResult{
		ExpectedTotal: slice.ExpectedTotal,
		Items:         slice.Items,
		Observations:  observations,
		Complete:      slice.Complete,
		Checkpoint:    slice.Checkpoint,
		RequestCount:  workerResult.RequestCount,
		ResponseBytes: workerResult.ResponseBytes,
	}, nil
}
