package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestApplyFileSkipsEmptyMigration(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "000001_empty.sql")
	if err := os.WriteFile(path, []byte("  \n\t  "), 0o644); err != nil {
		t.Fatalf("write migration: %v", err)
	}

	r := &Runner{db: db, migrationsDir: dir}
	if err := r.applyFile("000001_empty.sql", path); err != nil {
		t.Fatalf("apply empty migration: %v", err)
	}
}

func TestReadPathIndexMigrationExists(t *testing.T) {
	path := filepath.Join("..", "..", "migrations", "000009_add_read_path_indexes.sql")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	body := string(contents)
	for _, want := range []string{
		"CREATE INDEX IF NOT EXISTS idx_jobs_created_at_id ON jobs(created_at DESC, id DESC);",
		"CREATE INDEX IF NOT EXISTS idx_runs_created_at_id ON runs(created_at DESC, id DESC);",
		"CREATE INDEX IF NOT EXISTS idx_run_events_run_id_id ON run_events(run_id, id);",
		"CREATE INDEX IF NOT EXISTS idx_workers_started_at_id ON workers(started_at DESC, id DESC);",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected migration to contain %q, got:\n%s", want, body)
		}
	}
}
