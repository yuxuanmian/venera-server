package v2api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
	"venera-server/internal/v2sync"
)

type httpCloudClaim struct {
	AccountID     string
	PreparationID string
	Token         string
	ClientID      string
}

func TestCloudPreparationSnapshotDoesNotAdvancePreparing(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientID, token := enrollCompatibleClient(t, fixture, "cp4-preparing", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "cp4-preparing-account"})
	activated := postCandidate(t, fixture.router, token, "cp4-preparing-candidate", candidateBody(pkg.PackageReleaseID, "preparing"))
	if activated.Code != http.StatusCreated {
		t.Fatalf("activate account = %d %s", activated.Code, activated.Body.String())
	}
	accountID := decodeAPI(t, activated)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, pkg.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	var accountRevision int64
	if err := fixture.db.SQL().QueryRow(`SELECT revision FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&accountRevision); err != nil {
		t.Fatal(err)
	}
	created := apiCall(t, fixture.router, http.MethodPost, "/v2/client/source-accounts/"+accountID+"/cloud-preparations", map[string]any{
		"expectedSourceAccountRevision": accountRevision,
		"expectedLinkRevision":          int64(selected.Revision),
		"expectedInventoryRevision":     int64(1),
	}, token, "cp4-preparing-create", "2")
	if created.Code != http.StatusCreated {
		t.Fatalf("create preparation = %d %s", created.Code, created.Body.String())
	}
	data := decodeAPI(t, created)["data"].(map[string]any)
	preparationID := data["preparationId"].(string)
	revision := int64(data["revision"].(float64))
	snapshot := apiCall(t, fixture.router, http.MethodGet, "/v2/client/cloud-preparations/"+preparationID+"/snapshot", nil, token, "", "2")
	if snapshot.Code != http.StatusConflict || !strings.Contains(snapshot.Body.String(), `"code":"preparation_state_invalid"`) {
		t.Fatalf("preparing snapshot = %d %s", snapshot.Code, snapshot.Body.String())
	}
	var state, stage string
	var currentRevision int64
	if err := fixture.db.SQL().QueryRow(`SELECT state, stage, revision FROM cloud_mode_preparations WHERE preparation_id = ?`, preparationID).Scan(&state, &stage, &currentRevision); err != nil {
		t.Fatal(err)
	}
	if state != "preparing" || stage != "waitSnapshot" || currentRevision != revision {
		t.Fatalf("preparing state changed after GET: state=%s stage=%s revision=%d", state, stage, currentRevision)
	}
	var receipts int
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM snapshot_receipts WHERE client_id = ? AND source_account_id = ?`, clientID, accountID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("GET preparing created %d snapshot receipts", receipts)
	}
}

func TestCloudPreparationHTTPFlowIsOwnerScopedAndClaimScoped(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientA, tokenA := enrollCompatibleClient(t, fixture, "cp5-cloud-a", pkg)
	clientB, tokenB := enrollCompatibleClient(t, fixture, "cp5-cloud-b", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "cp5-shared"})
	first := postCandidate(t, fixture.router, tokenA, "cp5-candidate-a", candidateBody(pkg.PackageReleaseID, "a"))
	if first.Code != http.StatusCreated {
		t.Fatalf("activate A = %d %s", first.Code, first.Body.String())
	}
	accountID := decodeAPI(t, first)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	second := postCandidate(t, fixture.router, tokenB, "cp5-candidate-b", candidateBody(pkg.PackageReleaseID, "b"))
	if second.Code != http.StatusCreated {
		t.Fatalf("activate B = %d %s", second.Code, second.Body.String())
	}

	claimA := createHTTPCloudClaim(t, fixture, tokenA, clientA, accountID, "cp5-a")
	if cross := apiCall(t, fixture.router, http.MethodGet, "/v2/client/cloud-preparations/"+claimA.PreparationID, nil, tokenB, "", "2"); cross.Code != http.StatusNotFound {
		t.Fatalf("cross-client preparation status = %d %s", cross.Code, cross.Body.String())
	}

	states := apiCall(t, fixture.router, http.MethodGet, "/v2/client/source-states", nil, tokenA, "", "2")
	if states.Code != http.StatusOK || !strings.Contains(states.Body.String(), `"claimState":"active"`) {
		t.Fatalf("A source states after commit = %d %s", states.Code, states.Body.String())
	}
	otherStates := apiCall(t, fixture.router, http.MethodGet, "/v2/client/source-states", nil, tokenB, "", "2")
	if otherStates.Code != http.StatusOK || !strings.Contains(otherStates.Body.String(), `"claimState":"absent"`) {
		t.Fatalf("B source states before commit = %d %s", otherStates.Code, otherStates.Body.String())
	}

	// Finish a second independent preparation, then prove that deleting A's
	// claim leaves B's claim and the shared source account intact.
	claimB := createHTTPCloudClaim(t, fixture, tokenB, clientB, accountID, "cp5-b")
	_ = claimB
	deleteBody := map[string]any{"expectedRevision": int64(1)}
	deleted := apiCall(t, fixture.router, http.MethodDelete, "/v2/client/source-accounts/"+accountID+"/cloud-claim", deleteBody, tokenA, "cp5-delete-a", "2")
	if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"regularLocalCheckSuggested":true`) {
		t.Fatalf("delete A claim = %d %s", deleted.Code, deleted.Body.String())
	}
	var activeClaims, accountCount int
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM client_cloud_claims WHERE source_account_id = ? AND state = 'active'`, accountID).Scan(&activeClaims); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if activeClaims != 1 || accountCount != 1 {
		t.Fatalf("shared account after A delete = claims:%d accounts:%d", activeClaims, accountCount)
	}
	last := apiCall(t, fixture.router, http.MethodDelete, "/v2/client/source-accounts/"+accountID+"/cloud-claim", deleteBody, tokenB, "cp5-delete-b", "2")
	if last.Code != http.StatusOK {
		t.Fatalf("delete B claim = %d %s", last.Code, last.Body.String())
	}
	if _, err := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{Clock: func() time.Time { return fixture.now }}).Plan(context.Background()); err != nil {
		t.Fatalf("plan account demand after final claim wake: %v", err)
	}
	demands, err := v2scan.NewDemandRepository(fixture.repo).ListDemands(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, demand := range demands {
		if demand.Kind == v2scan.DemandAccountSnapshot && demand.SourceAccountID == accountID && demand.State != v2scan.DemandInactive {
			t.Fatalf("last claim left account demand active: %+v", demand)
		}
	}

	// The route is intentionally absent; mode is an App-owned decision.
	mode := apiCall(t, fixture.router, http.MethodPut, "/v2/client/mode", map[string]any{"mode": "cloud"}, tokenA, "cp5-mode", "2")
	if mode.Code != http.StatusNotFound {
		t.Fatalf("mode PUT status = %d, want 404", mode.Code)
	}
}

func TestCloudMutationHandlersWakeOnlyAfterDurableMutation(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientID, token := enrollCompatibleClient(t, fixture, "sr1-wake", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "sr1-wake-account"})
	activated := postCandidate(t, fixture.router, token, "sr1-wake-candidate", candidateBody(pkg.PackageReleaseID, "sr1-wake"))
	if activated.Code != http.StatusCreated {
		t.Fatalf("activate wake account = %d %s", activated.Code, activated.Body.String())
	}
	accountID := decodeAPI(t, activated)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	wakeCount := 0
	fixture.router.SetRuntimeWake(func() { wakeCount++ })

	createHTTPCloudClaim(t, fixture, token, clientID, accountID, "sr1-wake-claim")
	if wakeCount != 2 {
		t.Fatalf("create/commit wake count = %d, want 2", wakeCount)
	}

	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, pkg.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	var accountRevision int64
	if err := fixture.db.SQL().QueryRow(`SELECT revision FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&accountRevision); err != nil {
		t.Fatal(err)
	}
	created := apiCall(t, fixture.router, http.MethodPost, "/v2/client/source-accounts/"+accountID+"/cloud-preparations", map[string]any{
		"expectedSourceAccountRevision": accountRevision,
		"expectedLinkRevision":          int64(selected.Revision),
		"expectedInventoryRevision":     int64(1),
	}, token, "sr1-wake-cancel-create", "2")
	if created.Code != http.StatusCreated {
		t.Fatalf("create cancel preparation = %d %s", created.Code, created.Body.String())
	}
	createdData := decodeAPI(t, created)["data"].(map[string]any)
	if value, ok := createdData["nextEvaluationAt"]; !ok || value != nil {
		t.Fatalf("initial nextEvaluationAt = %#v, want null", createdData["nextEvaluationAt"])
	}
	cancelled := apiCall(t, fixture.router, http.MethodDelete, "/v2/client/cloud-preparations/"+createdData["preparationId"].(string), map[string]any{"expectedRevision": int64(1)}, token, "sr1-wake-cancel", "2")
	if cancelled.Code != http.StatusOK || decodeAPI(t, cancelled)["data"].(map[string]any)["state"] != "cancelled" {
		t.Fatalf("cancel preparation = %d %s", cancelled.Code, cancelled.Body.String())
	}
	if wakeCount != 4 {
		t.Fatalf("cancel preparation wake count = %d, want 4", wakeCount)
	}

	fixture.now = fixture.now.Add(48 * time.Hour)
	accepted := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": pkg.ArtifactID, "reason": "manualCheck", "clientSignalId": "sr1-wake-accepted"}, token, "", "2")
	if accepted.Code != http.StatusOK || !strings.Contains(accepted.Body.String(), `"status":"accepted"`) {
		t.Fatalf("accepted refresh = %d %s", accepted.Code, accepted.Body.String())
	}
	coalesced := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": pkg.ArtifactID, "reason": "manualCheck", "clientSignalId": "sr1-wake-coalesced"}, token, "", "2")
	if coalesced.Code != http.StatusOK || !strings.Contains(coalesced.Body.String(), `"status":"coalesced"`) {
		t.Fatalf("coalesced refresh = %d %s", coalesced.Code, coalesced.Body.String())
	}
	if wakeCount != 6 {
		t.Fatalf("refresh wake count = %d, want 6", wakeCount)
	}

	deleted := apiCall(t, fixture.router, http.MethodDelete, "/v2/client/source-accounts/"+accountID+"/cloud-claim", map[string]any{"expectedRevision": int64(1)}, token, "sr1-wake-delete", "2")
	if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"claimState":"suspended"`) {
		t.Fatalf("delete claim = %d %s", deleted.Code, deleted.Body.String())
	}
	if wakeCount != 7 {
		t.Fatalf("delete claim wake count = %d, want 7", wakeCount)
	}
}

// createHTTPCloudClaim drives the public two-phase protocol. It returns the
// source account ID in AccountID; the preparation ID is kept local because
// the snapshot is already committed before the helper returns.
func createHTTPCloudClaim(t *testing.T, fixture *apiFixture, token, clientID, accountID, prefix string) httpCloudClaim {
	t.Helper()
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, "manwa")
	if err != nil {
		t.Fatal(err)
	}
	var accountRevision int64
	if err := fixture.db.SQL().QueryRow(`SELECT revision FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&accountRevision); err != nil {
		t.Fatal(err)
	}
	created := apiCall(t, fixture.router, http.MethodPost, "/v2/client/source-accounts/"+accountID+"/cloud-preparations", map[string]any{
		"expectedSourceAccountRevision": accountRevision, "expectedLinkRevision": int64(selected.Revision), "expectedInventoryRevision": int64(1),
	}, token, prefix+"-prepare", "2")
	if created.Code != http.StatusCreated {
		t.Fatalf("create preparation = %d %s", created.Code, created.Body.String())
	}
	if _, err := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{Clock: func() time.Time { return fixture.now }}).Plan(context.Background()); err != nil {
		t.Fatalf("plan account demand after preparation wake: %v", err)
	}
	createdData := decodeAPI(t, created)["data"].(map[string]any)
	preparationID := createdData["preparationId"].(string)
	revision := int64(createdData["revision"].(float64))
	// The resident runtime normally publishes the complete source snapshot and
	// advances this preparation before the client downloads it. Seed that
	// publication through the same domain path so this HTTP-flow fixture remains
	// deterministic without exercising the GET side effect that RF4 removed.
	artifactID := string(selected.ArtifactID)
	var packageReleaseID string
	if err := fixture.db.SQL().QueryRow(`SELECT package_release_id FROM client_source_inventory WHERE client_id = ? AND artifact_id = ?`, clientID, artifactID).Scan(&packageReleaseID); err != nil {
		t.Fatal(err)
	}
	demand, err := v2scan.NewDemandRepository(fixture.repo).GetDemandByKey(context.Background(), v2scan.AccountSnapshotDemandKey(artifactID, accountID))
	if err != nil {
		t.Fatalf("load account demand: %v", err)
	}
	var sessionEpoch int64
	if err := fixture.db.SQL().QueryRow(`SELECT session_epoch FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&sessionEpoch); err != nil {
		t.Fatal(err)
	}
	executor := v2scan.NewSnapshotExecutor(fixture.repo, 0, 0)
	run, err := executor.StartRun(context.Background(), v2scan.SnapshotRunRequest{SourceAccountID: accountID, PackageReleaseID: packageReleaseID, SessionEpoch: sessionEpoch, RunGeneration: demand.ExecutionGeneration})
	if err != nil {
		t.Fatalf("start preparation snapshot run: %v", err)
	}
	if run.State == "running" {
		empty := 0
		if err := executor.ApplySlice(context.Background(), run.ID, v2scan.SnapshotSliceResult{ExpectedTotal: &empty, Items: []v2scan.SnapshotItem{}, Complete: true, BoundaryDigest: "empty"}); err != nil {
			t.Fatalf("apply preparation snapshot slice (%+v): %v", run, err)
		}
		completed, err := executor.CompleteRun(context.Background(), run.ID, "empty")
		if err != nil {
			t.Fatalf("complete preparation snapshot run: %v", err)
		}
		if _, err := v2sync.NewPublisher(fixture.repo).PublishSnapshot(context.Background(), v2sync.PublishSnapshotRequest{RunID: completed.ID, SourceAccountID: accountID, PackageReleaseID: packageReleaseID, ExpectedGeneration: demand.ExecutionGeneration, ExpectedSessionEpoch: sessionEpoch, FreshUntil: fixture.now.Add(3 * time.Hour)}); err != nil {
			t.Fatalf("publish preparation snapshot: %v", err)
		}
	} else if run.State != "published" {
		t.Fatalf("preparation snapshot run state = %s", run.State)
	}
	if _, err := v2sync.NewPreparationCoordinator(fixture.repo).EvaluatePreparation(context.Background(), clientID, preparationID); err != nil {
		t.Fatalf("evaluate preparation readiness: %v", err)
	}
	snapshot := apiCall(t, fixture.router, http.MethodGet, "/v2/client/cloud-preparations/"+preparationID+"/snapshot", nil, token, "", "2")
	if snapshot.Code != http.StatusOK || snapshot.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("preparation snapshot = %d %s", snapshot.Code, snapshot.Body.String())
	}
	footer := readSnapshotFooter(t, snapshot.Body.String())
	receipt := footer["snapshotReceipt"].(string)
	digest := footer["snapshotDigest"].(string)
	ready := apiCall(t, fixture.router, http.MethodGet, "/v2/client/cloud-preparations/"+preparationID, nil, token, "", "2")
	if ready.Code != http.StatusOK {
		t.Fatalf("get ready preparation = %d %s", ready.Code, ready.Body.String())
	}
	revision = int64(decodeAPI(t, ready)["data"].(map[string]any)["revision"].(float64))
	if revision < 2 {
		t.Fatalf("preparation revision = %d, want snapshot-ready revision", revision)
	}
	commit := apiCall(t, fixture.router, http.MethodPost, "/v2/client/cloud-preparations/"+preparationID+"/commit", map[string]any{
		"expectedPreparationRevision": revision, "snapshotReceipt": receipt, "snapshotDigest": digest,
	}, token, prefix+"-commit", "2")
	if commit.Code != http.StatusOK || !strings.Contains(commit.Body.String(), `"claimState":"active"`) {
		t.Fatalf("commit preparation = %d %s", commit.Code, commit.Body.String())
	}
	return httpCloudClaim{AccountID: accountID, PreparationID: preparationID, Token: token, ClientID: clientID}
}

func readSnapshotFooter(t *testing.T, body string) map[string]any {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(body))
	var footer map[string]any
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		if line["type"] == "footer" {
			footer = line
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if footer == nil || !strings.HasPrefix(footer["snapshotDigest"].(string), "sha256:") {
		t.Fatalf("invalid snapshot footer: %#v", footer)
	}
	return footer
}

func TestRefreshSignalCoalescesAndReportsBlocked(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientID, token := enrollCompatibleClient(t, fixture, "cp5-refresh", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "cp5-refresh-account"})
	activated := postCandidate(t, fixture.router, token, "cp5-refresh-candidate", candidateBody(pkg.PackageReleaseID, "refresh"))
	if activated.Code != http.StatusCreated {
		t.Fatalf("activate refresh account = %d %s", activated.Code, activated.Body.String())
	}
	accountID := decodeAPI(t, activated)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	_ = createHTTPCloudClaim(t, fixture, token, clientID, accountID, "cp5-refresh-claim")
	fresh := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": "manwa", "reason": "manualCheck", "clientSignalId": "refresh-1"}, token, "", "2")
	if fresh.Code != http.StatusOK || !strings.Contains(fresh.Body.String(), `"status":"notNeeded"`) {
		t.Fatalf("fresh refresh signal = %d %s", fresh.Code, fresh.Body.String())
	}
	fixture.now = fixture.now.Add(48 * time.Hour)
	accepted := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": "manwa", "reason": "manualCheck", "clientSignalId": "refresh-2"}, token, "", "2")
	if accepted.Code != http.StatusOK || !strings.Contains(accepted.Body.String(), `"status":"accepted"`) {
		t.Fatalf("accepted refresh signal = %d %s", accepted.Code, accepted.Body.String())
	}
	coalesced := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": "manwa", "reason": "manualCheck", "clientSignalId": "refresh-3"}, token, "", "2")
	if coalesced.Code != http.StatusOK || !strings.Contains(coalesced.Body.String(), `"status":"coalesced"`) {
		t.Fatalf("coalesced refresh signal = %d %s", coalesced.Code, coalesced.Body.String())
	}
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `UPDATE source_scan_status SET status = 'blocked', blocked_reason = 'quarantined', revision = revision + 1 WHERE source_account_id = ?`, accountID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	blocked := apiCall(t, fixture.router, http.MethodPost, "/v2/refresh-signals", map[string]any{"sourceAccountId": accountID, "artifactId": "manwa", "reason": "manualCheck", "clientSignalId": "refresh-4"}, token, "", "2")
	if blocked.Code != http.StatusOK || !strings.Contains(blocked.Body.String(), `"status":"blocked"`) || strings.Contains(blocked.Body.String(), `"jobId"`) {
		t.Fatalf("blocked refresh signal = %d %s", blocked.Code, blocked.Body.String())
	}
}

func TestSyncPullLongPollAndSnapshotWireContract(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientID, token := enrollCompatibleClient(t, fixture, "cp5-sync", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "cp5-sync-account"})
	activated := postCandidate(t, fixture.router, token, "cp5-sync-candidate", candidateBody(pkg.PackageReleaseID, "sync"))
	if activated.Code != http.StatusCreated {
		t.Fatalf("activate sync account = %d %s", activated.Code, activated.Body.String())
	}
	accountID := decodeAPI(t, activated)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	createHTTPCloudClaim(t, fixture, token, clientID, accountID, "cp5-sync-claim")
	initialCursor, err := v2crypto.EncodeCursor(fixture.repo.Keys().CursorMAC, clientID, 0)
	if err != nil {
		t.Fatal(err)
	}
	page := apiCall(t, fixture.router, http.MethodPost, "/v2/sync/pull", map[string]any{"cursor": initialCursor, "limit": 500, "byteBudget": 1 << 20, "waitMs": 0}, token, "", "2")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), `"changes"`) {
		t.Fatalf("pull page = %d %s", page.Code, page.Body.String())
	}
	data := decodeAPI(t, page)["data"].(map[string]any)
	currentCursor := data["nextCursor"].(string)
	small := apiCall(t, fixture.router, http.MethodPost, "/v2/sync/pull", map[string]any{"cursor": initialCursor, "limit": 500, "byteBudget": 1, "waitMs": 0}, token, "", "2")
	if small.Code != http.StatusBadRequest || !strings.Contains(small.Body.String(), `"code":"change_byte_budget_exceeded"`) {
		t.Fatalf("small byte budget = %d %s", small.Code, small.Body.String())
	}
	longPoll := apiCall(t, fixture.router, http.MethodPost, "/v2/sync/pull", map[string]any{"cursor": currentCursor, "limit": 200, "byteBudget": 1 << 20, "waitMs": 20}, token, "", "2")
	if longPoll.Code != http.StatusOK || !strings.Contains(longPoll.Body.String(), `"hasMore":false`) {
		t.Fatalf("long poll = %d %s", longPoll.Code, longPoll.Body.String())
	}
	source := apiCall(t, fixture.router, http.MethodPost, "/v2/sync/snapshots", map[string]any{"scope": "source", "artifactId": "manwa", "sourceAccountId": accountID, "reason": "test"}, token, "", "2")
	if source.Code != http.StatusOK || source.Header().Get("Content-Type") != "application/x-ndjson" || !strings.Contains(source.Body.String(), `"protocol":2`) || !strings.Contains(source.Body.String(), `"snapshotReceipt"`) {
		t.Fatalf("source snapshot = %d headers=%v body=%s", source.Code, source.Header(), source.Body.String())
	}
	full := apiCall(t, fixture.router, http.MethodPost, "/v2/sync/snapshots", map[string]any{"scope": "full", "reason": "cursorExpired"}, token, "", "2")
	if full.Code != http.StatusOK || !strings.Contains(full.Body.String(), `"scope":"full"`) {
		t.Fatalf("full snapshot = %d %s", full.Code, full.Body.String())
	}
}

func TestLegacyAdminRoutesAreRemoved(t *testing.T) {
	fixture := newAPIFixture(t)
	requests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin/api/stats"},
		{http.MethodGet, "/admin/api/catalog"},
		{http.MethodGet, "/admin/api/sources"},
		{http.MethodGet, "/admin/api/jobs"},
		{http.MethodPost, "/admin/api/manifest/check-now"},
		{http.MethodPost, "/admin/api/source/unquarantine"},
		{http.MethodPost, "/admin/api/client/revoke"},
	}
	for _, request := range requests {
		response := apiCall(t, fixture.router, request.method, request.path, nil, "", "", "")
		if response.Code != http.StatusNotFound {
			t.Errorf("legacy admin route %s %s = %d %s, want 404", request.method, request.path, response.Code, response.Body.String())
		}
	}
}
