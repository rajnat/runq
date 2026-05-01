package store

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestClaimPendingRunsReturnsEarlyWhenNoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT r.id, r.job_id, j.tenant_id, j.queue, j.kind, COALESCE\(j.concurrency_key, ''\), r.scheduled_at`).
		WithArgs(4).
		WillReturnRows(sqlmock.NewRows([]string{"id", "job_id", "tenant_id", "queue", "kind", "concurrency_key", "scheduled_at"}))
	mock.ExpectCommit()

	store := &Store{db: db}
	assignments, summary, err := store.ClaimPendingRuns(context.Background(), 1, 30*time.Second, 0)
	if err != nil {
		t.Fatalf("claim pending runs: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %+v", assignments)
	}
	if summary.CandidateRuns != 0 || summary.EligibleWorkers != 0 || summary.AssignedRuns != 0 {
		t.Fatalf("expected empty summary, got %+v", summary)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
