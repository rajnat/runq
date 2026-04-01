package migrate

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/lib/pq"
)

type Runner struct {
	db            *sql.DB
	migrationsDir string
}

func NewRunner(databaseURL, migrationsDir string) (*Runner, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	return &Runner{
		db:            db,
		migrationsDir: migrationsDir,
	}, nil
}

func (r *Runner) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *Runner) Up() error {
	if err := r.ensureSchemaMigrationsTable(); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(r.migrationsDir, "*.sql"))
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	sort.Strings(files)

	for _, path := range files {
		version := filepath.Base(path)
		applied, err := r.isApplied(version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		if err := r.applyFile(version, path); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) ensureSchemaMigrationsTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	return nil
}

func (r *Runner) isApplied(version string) (bool, error) {
	var exists bool
	err := r.db.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)
	`, version).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check migration %s: %w", version, err)
	}
	return exists, nil
}

func (r *Runner) applyFile(version, path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}

	sqlText := strings.TrimSpace(string(contents))
	if sqlText == "" {
		return nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(sqlText); err != nil {
		return fmt.Errorf("apply migration %s: %w", version, err)
	}

	if _, err := tx.Exec(`
		INSERT INTO schema_migrations(version)
		VALUES ($1)
	`, version); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}

	return nil
}
