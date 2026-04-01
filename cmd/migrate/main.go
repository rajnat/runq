package main

import (
	"log"
	"os"

	"github.com/eswar/runq/internal/config"
	"github.com/eswar/runq/internal/migrate"
)

func main() {
	cfg := config.LoadMigration()
	logger := log.New(os.Stdout, "migrate ", log.LstdFlags|log.LUTC)

	runner, err := migrate.NewRunner(cfg.DatabaseURL, cfg.MigrationsDir)
	if err != nil {
		logger.Fatal(err)
	}
	defer runner.Close()

	if err := runner.Up(); err != nil {
		logger.Fatal(err)
	}

	logger.Printf("applied migrations from %s", cfg.MigrationsDir)
}
