package service

import (
	"errors"
	"testing"
	"time"

	legacyStore "venera-server/internal/store"
	"venera-server/internal/tracking/catalog"
	"venera-server/internal/tracking/domain"
	trackingruntime "venera-server/internal/tracking/runtime"
	trackingstore "venera-server/internal/tracking/store"
	"venera-server/internal/tracking/worker"
)

const serviceTestRevision = "0123456789abcdef0123456789abcdef01234567"

func newTestService(t *testing.T) (*Service, *legacyStore.Store) {
	t.Helper()
	legacy, err := legacyStore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	repository, err := trackingstore.NewRepository(legacy.DB())
	if err != nil {
		t.Fatal(err)
	}
	runtimeState, err := trackingruntime.New(domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: serviceTestRevision,
		Generation:     1,
		Artifacts: []domain.ArtifactIdentity{{
			SourceKey: "manwa",
			FileName:  "manwa.js",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Catalog:     &catalog.Manager{},
		Runtime:     runtimeState,
		Repository:  repository,
		LegacyStore: legacy,
		CookieKey:   []byte("test-cookie-key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, legacy
}

func TestServiceDefaultsToTwelveHourBoundedRetryConfiguration(t *testing.T) {
	service, _ := newTestService(t)
	if service.interval != 12*time.Hour {
		t.Fatalf("tracking interval = %s, want 12h", service.interval)
	}
	if service.maxConcurrent != 2 {
		t.Fatalf("max concurrent = %d, want 2", service.maxConcurrent)
	}
	if service.maxSnapshotRequests != 64 {
		t.Fatalf("snapshot request budget = %d, want 64", service.maxSnapshotRequests)
	}
	if service.snapshotDeadline != 2*time.Minute {
		t.Fatalf("snapshot deadline = %s, want 2m", service.snapshotDeadline)
	}
	if service.maxAttempts != 3 {
		t.Fatalf("max attempts = %d, want 3", service.maxAttempts)
	}
}

func TestServiceRetryPolicyOnlyRetriesTransientWorkerFailures(t *testing.T) {
	if !retryableScanError(worker.StructuredError{Code: "transient"}) {
		t.Fatal("transient worker error was not retryable")
	}
	if !retryableScanError(worker.StructuredError{Code: "rate_limited"}) {
		t.Fatal("rate-limited worker error was not retryable")
	}
	if retryableScanError(worker.StructuredError{Code: "contract_drift"}) {
		t.Fatal("contract drift was retried")
	}
	if retryableScanError(errIncompleteSnapshot) {
		t.Fatal("incomplete snapshot was retried instead of being persisted")
	}
	if retryableScanError(trackingstore.ErrAuthorizationMismatch) {
		t.Fatal("authorization mismatch was retried")
	}
}

func TestBuildGroupsMergesExactArtifactDemandAcrossDevices(t *testing.T) {
	service, legacy := newTestService(t)
	if err := legacy.UpsertUser("user-1", "UTC", "en"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertDevice("device-a", "user-1", "token-a"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertDevice("device-b", "user-1", "token-b"); err != nil {
		t.Fatal(err)
	}
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	states := []domain.ClientState{
		{
			DeviceID:      "device-a",
			CloudEnabled:  true,
			StateRevision: 4,
			Interests: []domain.Interest{{
				DeviceID: "device-a",
				Artifact: artifact,
				ComicID:  "42",
			}},
		},
		{
			DeviceID:      "device-b",
			CloudEnabled:  true,
			StateRevision: 8,
			Interests: []domain.Interest{{
				DeviceID: "device-b",
				Artifact: artifact,
				ComicID:  "43",
			}},
		},
		{
			DeviceID:     "device-off",
			CloudEnabled: false,
			Interests:    []domain.Interest{{Artifact: artifact, ComicID: "ignored"}},
		},
	}
	demanded := map[serviceDemandKey]struct{}{
		{Artifact: artifact, ComicID: "42"}: {},
		{Artifact: artifact, ComicID: "43"}: {},
	}
	groups, err := service.buildGroups(states, demanded)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %+v, want one merged account group", groups)
	}
	group := groups[0]
	if len(group.InterestIDs) != 2 {
		t.Fatalf("merged interest IDs = %#v", group.InterestIDs)
	}
	if len(group.Demands) != 2 {
		t.Fatalf("demand fences = %#v, want two devices", group.Demands)
	}
	for _, demand := range group.Demands {
		switch demand.DeviceID {
		case "device-a":
			if demand.StateRevision != 4 || len(demand.InterestIDs) != 1 || demand.InterestIDs[0] != "42" {
				t.Fatalf("device-a demand = %+v", demand)
			}
		case "device-b":
			if demand.StateRevision != 8 || len(demand.InterestIDs) != 1 || demand.InterestIDs[0] != "43" {
				t.Fatalf("device-b demand = %+v", demand)
			}
		default:
			t.Fatalf("unexpected demand = %+v", demand)
		}
	}
}

func TestScanGroupSchedulingIdentityKeepsCompetingAccountsSeparate(t *testing.T) {
	service, legacy := newTestService(t)
	if err := legacy.UpsertUser("user-a", "UTC", "en"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertUser("user-b", "UTC", "en"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertDevice("device-a", "user-a", "token-a"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertDevice("device-b", "user-b", "token-b"); err != nil {
		t.Fatal(err)
	}
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	states := []domain.ClientState{
		{DeviceID: "device-a", CloudEnabled: true, StateRevision: 1, Interests: []domain.Interest{{DeviceID: "device-a", Artifact: artifact, ComicID: "42"}}},
		{DeviceID: "device-b", CloudEnabled: true, StateRevision: 1, Interests: []domain.Interest{{DeviceID: "device-b", Artifact: artifact, ComicID: "42"}}},
	}
	demanded := map[serviceDemandKey]struct{}{{Artifact: artifact, ComicID: "42"}: {}}
	groups, err := service.buildGroups(states, demanded)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || groups[0].UserID == groups[1].UserID {
		t.Fatalf("competing account groups = %+v", groups)
	}
	authority, ok := service.runtime.Authority()
	if !ok {
		t.Fatal("runtime authority is unavailable")
	}
	firstDigest, err := scanGroupDemandDigest(groups[0], authority)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := scanGroupDemandDigest(groups[1], authority)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest == secondDigest || scanGroupKey(groups[0].UserID, artifact) == scanGroupKey(groups[1].UserID, artifact) {
		t.Fatalf("competing account scheduling identities collapsed: %q %q", firstDigest, secondDigest)
	}
}

func TestServiceWakeIsCoalescedAndLeaseCoversBoundedRetries(t *testing.T) {
	service, _ := newTestService(t)
	service.Wake()
	service.Wake()
	select {
	case <-service.wake:
	default:
		t.Fatal("service wake was not queued")
	}
	select {
	case <-service.wake:
		t.Fatal("duplicate service wake was not coalesced")
	default:
	}
	if got, want := service.scanLeaseDuration(), 7*time.Minute; got != want {
		t.Fatalf("scan lease duration = %s, want %s", got, want)
	}
}

func TestPreLeaseLifecycleBackoffIsDeterministicAndWakeResetsIt(t *testing.T) {
	service, _ := newTestService(t)
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	service.clock = func() time.Time { return now }

	service.recordPreLeaseResult(preLeaseError(errors.New("authority unavailable")))
	if got, want := service.preLeaseRetryBoundary(), now.Add(250*time.Millisecond); !got.Equal(want) {
		t.Fatalf("first pre-lease retry = %s, want %s", got, want)
	}
	service.recordPreLeaseResult(preLeaseError(errors.New("authority unavailable")))
	if got, want := service.preLeaseRetryBoundary(), now.Add(500*time.Millisecond); !got.Equal(want) {
		t.Fatalf("second pre-lease retry = %s, want %s", got, want)
	}
	for index := 0; index < 16; index++ {
		service.recordPreLeaseResult(preLeaseError(errors.New("authority unavailable")))
	}
	if got, want := service.preLeaseRetryBoundary(), now.Add(preLeaseBackoffMax); !got.Equal(want) {
		t.Fatalf("capped pre-lease retry = %s, want %s", got, want)
	}

	service.Wake()
	if got := service.preLeaseRetryBoundary(); !got.IsZero() {
		t.Fatalf("explicit wake kept pre-lease backoff at %s", got)
	}
	service.recordPreLeaseResult(nil)
	if got := service.preLeaseRetryBoundary(); !got.IsZero() {
		t.Fatalf("successful lifecycle run kept pre-lease backoff at %s", got)
	}
}
