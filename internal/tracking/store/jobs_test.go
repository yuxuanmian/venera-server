package store

import (
	"context"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
)

func testScanJobSpec(userID, digest string, dueAt time.Time) ScanJobSpec {
	return ScanJobSpec{
		UserID:            userID,
		Artifact:          domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"},
		CatalogID:         "owner/catalog",
		CatalogRevision:   testRevisionA,
		RuntimeGeneration: 1,
		DemandDigest:      digest,
		DueAt:             dueAt,
		Priority:          ScanJobPriorityNormal,
		MaxAttempts:       3,
	}
}

func TestReconcileScanJobsKeepsCadenceAndWakesChangedDemand(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	specs := []ScanJobSpec{
		testScanJobSpec("user-a", "digest-a", now),
		testScanJobSpec("user-b", "digest-b", now),
	}
	if err := repository.ReconcileScanJobs(ctx, specs, now); err != nil {
		t.Fatal(err)
	}
	jobs, err := repository.ListDueScanJobs(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].UserID != "user-a" || jobs[1].UserID != "user-b" {
		t.Fatalf("due jobs = %+v", jobs)
	}

	if claimed, err := repository.ClaimScanJob(ctx, jobs[0].JobID, "lease-a", now, time.Hour); err != nil || !claimed {
		t.Fatalf("claim user-a: claimed=%v err=%v", claimed, err)
	}
	if due, err := repository.ListDueScanJobs(ctx, now, 10); err != nil {
		t.Fatal(err)
	} else if len(due) != 1 || due[0].UserID != "user-b" {
		t.Fatalf("running job remained due: %+v", due)
	}

	completedAt := now.Add(12 * time.Hour)
	if err := repository.CompleteScanJob(ctx, jobs[0].JobID, "lease-a", completedAt, now); err != nil {
		t.Fatal(err)
	}
	if next, found, err := repository.NextScanWake(ctx, now); err != nil {
		t.Fatal(err)
	} else if !found || !next.Equal(now) {
		t.Fatalf("next wake = %v found=%v, want user-b due now", next, found)
	}

	later := now.Add(time.Hour)
	if err := repository.ReconcileScanJobs(ctx, specs, later); err != nil {
		t.Fatal(err)
	}
	all, err := repository.ListScanJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || !all[0].DueAt.Equal(completedAt) {
		t.Fatalf("unchanged reconciliation changed cadence: %+v", all)
	}

	changed := testScanJobSpec("user-a", "digest-a-new", later)
	if err := repository.ReconcileScanJobs(ctx, []ScanJobSpec{changed, specs[1]}, later); err != nil {
		t.Fatal(err)
	}
	changedJobs, err := repository.ListDueScanJobs(ctx, later, 10)
	if err != nil {
		t.Fatal(err)
	}
	var changedJob *ScanJob
	for index := range changedJobs {
		if changedJobs[index].UserID == "user-a" {
			changedJob = &changedJobs[index]
		}
	}
	if len(changedJobs) != 2 || changedJob == nil || changedJob.DemandDigest != "digest-a-new" || changedJob.State != ScanJobPending {
		t.Fatalf("changed demand did not wake pending job: %+v", changedJobs)
	}
}

func TestScanJobRetryLeaseAndRestartRecoveryAreDurable(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 13, 0, 0, 0, time.UTC)
	if err := repository.ReconcileScanJobs(ctx, []ScanJobSpec{testScanJobSpec("user-a", "digest-a", now)}, now); err != nil {
		t.Fatal(err)
	}
	jobs, err := repository.ListDueScanJobs(ctx, now, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("initial jobs = %+v err=%v", jobs, err)
	}
	job := jobs[0]
	if claimed, err := repository.ClaimScanJob(ctx, job.JobID, "lease-a", now, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	if err := repository.RecoverRunningScanJobs(ctx, now.Add(30*time.Minute), false); err != nil {
		t.Fatal(err)
	}
	recovered, err := repository.ListDueScanJobs(ctx, now.Add(30*time.Minute), 10)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("unexpired lease was recovered: jobs=%+v err=%v", recovered, err)
	}
	if err := repository.RecoverRunningScanJobs(ctx, now.Add(30*time.Minute), true); err != nil {
		t.Fatal(err)
	}
	recovered, err = repository.ListDueScanJobs(ctx, now.Add(30*time.Minute), 10)
	if err != nil || len(recovered) != 1 || recovered[0].State != ScanJobPending {
		t.Fatalf("restart did not recover running job: jobs=%+v err=%v", recovered, err)
	}

	runAt := now.Add(time.Hour)
	if claimed, err := repository.ClaimScanJob(ctx, job.JobID, "lease-b", runAt, time.Hour); err != nil || !claimed {
		t.Fatalf("reclaim: claimed=%v err=%v", claimed, err)
	}
	if err := repository.FailScanJob(ctx, job.JobID, "lease-b", runAt, "temporary", true, 5*time.Minute, 12*time.Hour); err != nil {
		t.Fatal(err)
	}
	failed, err := repository.ListScanJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].State != ScanJobPending || !failed[0].DueAt.Equal(runAt.Add(5*time.Minute)) || failed[0].Attempts != 2 {
		t.Fatalf("retry state = %+v", failed)
	}

	retryAt := runAt.Add(5 * time.Minute)
	if claimed, err := repository.ClaimScanJob(ctx, job.JobID, "lease-c", retryAt, time.Hour); err != nil || !claimed {
		t.Fatalf("second claim: claimed=%v err=%v", claimed, err)
	}
	if err := repository.FailScanJob(ctx, job.JobID, "lease-c", retryAt, "permanent", true, time.Minute, 12*time.Hour); err != nil {
		t.Fatal(err)
	}
	failed, err = repository.ListScanJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failed[0].State != ScanJobFailed || failed[0].Attempts != 3 || !failed[0].DueAt.Equal(retryAt.Add(12*time.Hour)) {
		t.Fatalf("second retry state = %+v", failed[0])
	}
}

func TestScanJobReconciliationRemovesDisabledDemand(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 14, 0, 0, 0, time.UTC)
	if err := repository.ReconcileScanJobs(ctx, []ScanJobSpec{
		testScanJobSpec("user-a", "digest-a", now),
		testScanJobSpec("user-b", "digest-b", now),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReconcileScanJobs(ctx, []ScanJobSpec{testScanJobSpec("user-b", "digest-b", now)}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	jobs, err := repository.ListScanJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].UserID != "user-b" {
		t.Fatalf("disabled demand retained a job: %+v", jobs)
	}
}
