package migrate

import (
	"os"
	"path/filepath"
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
