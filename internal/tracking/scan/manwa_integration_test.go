package scan

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
	trackingruntime "venera-server/internal/tracking/runtime"
	"venera-server/internal/tracking/worker"
)

const manwaTestRevision = "0123456789abcdef0123456789abcdef01234567"

type manwaRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn manwaRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestManwaProbeAndSnapshotProjection(t *testing.T) {
	core, extension := loadManwaScripts(t)
	fullVariant := false
	transport := manwaRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var body string
		switch request.URL.Path {
		case "/ucenter":
			body = `<html><body><div class="center-main-info-right"><p class="center-main-info-title">脱敏昵称</p><p class="center-main-info-title">用户名 : fixture-account</p></div></body></html>`
		case "/users/welfare":
			body = `<html><body><div class="center-main-info"><div class="detail-list-comment-lv"><span>Lv2</span></div></div></body></html>`
		case "/bookshelf":
			body = `<html><body><span class="favorite-count">1/800</span></body></html>`
		case "/getfavors":
			body = manwaFavoriteBody(fullVariant)
		default:
			return manwaResponse(request, http.StatusNotFound, "not found"), nil
		}
		return manwaResponse(request, http.StatusOK, body), nil
	})
	runtimeWorker, err := worker.NewWorker(worker.WorkerConfig{
		CoreScript:              core,
		ExtensionScript:         extension,
		AllowedOrigins:          []string{"https://manwa.me"},
		Client:                  &http.Client{Transport: transport},
		DefaultOperationTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	contract := AccountProbeContract{
		ID:                     "manwa-account-v1",
		Version:                1,
		IdentitySchemes:        []string{"manwa-username-v1"},
		AttributeFields:        []string{"accountLevel"},
		VisibilityScopePattern: `^manwa:level:\d+$`,
	}
	probe, err := NewAccountProbeRunner(runtimeWorker).Probe(context.Background(), ProbeRequest{
		RequestID:         "probe-manwa",
		Artifact:          artifact,
		Revision:          manwaTestRevision,
		RuntimeGeneration: 7,
		Contract:          contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	if probe.IdentityValue != "fixture-account" || probe.VisibilityScope != "manwa:level:2" ||
		probe.AttributesJSON != `{"accountLevel":2}` {
		t.Fatalf("probe = %+v", probe)
	}

	now := time.Date(2026, time.September, 3, 9, 0, 0, 0, time.UTC)
	executor := NewSnapshotExecutor(runtimeWorker)
	executor.Clock = func() time.Time { return now }
	normal, err := executor.Run(context.Background(), SnapshotRequest{
		RequestID:         "snapshot-normal",
		Artifact:          artifact,
		Revision:          manwaTestRevision,
		RuntimeGeneration: 7,
		Account:           map[string]any{"visibilityScope": probe.VisibilityScope},
		Budget:            worker.OperationBudget{MaxRequests: 4, MaxItems: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !normal.Complete || len(normal.Observations) != 1 {
		t.Fatalf("normal snapshot = %+v", normal)
	}
	if normal.Observations[0].FavoriteUpdate.State == nil ||
		normal.Observations[0].FavoriteUpdate.State.LatestChapterID == nil ||
		*normal.Observations[0].FavoriteUpdate.State.LatestChapterID != "chapter-42" ||
		normal.Observations[0].FavoriteUpdate.Marker != nil ||
		normal.Observations[0].FavoriteUpdate.SourceUnread == nil ||
		!*normal.Observations[0].FavoriteUpdate.SourceUnread {
		t.Fatalf("normal observation = %+v", normal.Observations[0])
	}
	if !normal.Observations[0].ValidUntil.Equal(now.Add(ManwaObservationFreshness)) {
		t.Fatalf("normal freshness = %s", normal.Observations[0].ValidUntil)
	}

	fullVariant = true
	full, err := executor.Run(context.Background(), SnapshotRequest{
		RequestID:         "snapshot-full",
		Artifact:          artifact,
		Revision:          manwaTestRevision,
		RuntimeGeneration: 7,
		Account:           map[string]any{"visibilityScope": probe.VisibilityScope},
		Budget:            worker.OperationBudget{MaxRequests: 4, MaxItems: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if full.Observations[0].FavoriteUpdate.State != nil || full.Observations[0].FavoriteUpdate.Marker == nil {
		t.Fatalf("full observation = %+v", full.Observations[0])
	}
	if full.Observations[0].FavoriteUpdate.SourceUnread == nil ||
		!*full.Observations[0].FavoriteUpdate.SourceUnread {
		t.Fatal("full sourceUnread was not folded from full_is_new")
	}
	if full.Observations[0].FavoriteUpdate.Metadata["fullLatestChapterId"] != "full-42" {
		t.Fatalf("full metadata = %#v", full.Observations[0].FavoriteUpdate.Metadata)
	}

	late, err := trackingruntime.BuildSnapshot(
		domain.Authority{
			CatalogID:      "owner/catalog",
			ActiveRevision: manwaTestRevision,
			Generation:     7,
			Artifacts:      []domain.ArtifactIdentity{artifact},
		},
		full.Observations,
		now.Add(ManwaObservationFreshness+time.Second),
		10000,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(late.Observations) != 0 {
		t.Fatal("expired Manwa observation remained current")
	}
}

func loadManwaScripts(t *testing.T) ([]byte, []byte) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "venera-configs")
	core, err := os.ReadFile(filepath.Join(root, "_venera_.js"))
	if err != nil {
		t.Fatal(err)
	}
	extension, err := os.ReadFile(filepath.Join(root, "extensions", "server", "manwa", "scanning.js"))
	if err != nil {
		t.Fatal(err)
	}
	return core, extension
}

func manwaFavoriteBody(full bool) string {
	book := `{"id":42,"book_name":"脱敏漫画","cover_url":"/static/fixture.webp","is_new":true,"full_is_new":false,"last_chapter":{"id":"chapter-42","chapter_name":"第42话"},"full_last_chapter":null,"read_last_chapter":{"id":"read-42","update_time":"2099-01-01 00:00:00"}}`
	if full {
		book = strings.Replace(book, `"full_is_new":false,"last_chapter"`, `"full_is_new":true,"last_chapter"`, 1)
		book = strings.Replace(book, `"full_last_chapter":null`, `"full_last_chapter":{"id":"full-42","chapter_name":"完整版第42话"}`, 1)
	}
	return `{"err":0,"books":[` + book + `]}`
}

func manwaResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}
